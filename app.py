# app.py
import os
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from pytz import timezone, UTC

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from sheets_client import GoogleSheetsClient
from ringba_client import RingbaClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables (set these in Render)
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")          # e.g. 1abc123...
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Sheet1")
HOURLY_WORKSHEET_NAME = os.getenv("HOURLY_WORKSHEET_NAME", "Hourly")

if not SPREADSHEET_ID:
    raise RuntimeError("SPREADSHEET_ID env var is required")

# Daily reporting client (for Sheet1)
sheets_client = GoogleSheetsClient(
    spreadsheet_id=SPREADSHEET_ID,
    worksheet_name=WORKSHEET_NAME,
)

# Hourly reporting client (for Hourly tab)
hourly_sheets_client = GoogleSheetsClient(
    spreadsheet_id=SPREADSHEET_ID,
    worksheet_name=HOURLY_WORKSHEET_NAME,
)

ringba_client = RingbaClient()

# Initialize scheduler for end-of-day reports
scheduler = AsyncIOScheduler()


async def run_end_of_day_report():
    """Scheduled task to run end-of-day report."""
    logger.info("Running scheduled end-of-day report")
    try:
        # Get current date in EST timezone
        est = timezone('America/New_York')
        now_est = datetime.now(est)
        current_weekday = now_est.weekday()  # 0=Monday, 6=Sunday
        
        all_publishers = []
        
        if current_weekday == 0:  # Monday - pull Friday, Saturday, Sunday
            logger.info("Monday detected - pulling weekend data (Friday, Saturday, Sunday)")
            # Friday (3 days ago)
            friday = now_est - timedelta(days=3)
            friday_start = friday.replace(hour=0, minute=0, second=0, microsecond=0)
            friday_end = friday.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            # Saturday (2 days ago)
            saturday = now_est - timedelta(days=2)
            saturday_start = saturday.replace(hour=0, minute=0, second=0, microsecond=0)
            saturday_end = saturday.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            # Sunday (yesterday)
            sunday = now_est - timedelta(days=1)
            sunday_start = sunday.replace(hour=0, minute=0, second=0, microsecond=0)
            sunday_end = sunday.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            # Convert to UTC for API calls
            for day_name, day_start, day_end in [
                ("Friday", friday_start, friday_end),
                ("Saturday", saturday_start, saturday_end),
                ("Sunday", sunday_start, sunday_end)
            ]:
                day_start_utc = day_start.astimezone(UTC)
                day_end_utc = day_end.astimezone(UTC)
                
                logger.info(f"Pulling {day_name} data: {day_start_utc.date()}")
                publishers = ringba_client.get_publisher_payouts(
                    report_start=day_start_utc.isoformat().replace('+00:00', 'Z'),
                    report_end=day_end_utc.isoformat().replace('+00:00', 'Z')
                )
                all_publishers.extend(publishers)
        else:
            # Tuesday-Friday - pull previous day
            logger.info(f"Weekday detected ({now_est.strftime('%A')}) - pulling previous day data")
            yesterday = now_est - timedelta(days=1)
            report_start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
            report_end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            # Convert to UTC for API calls
            report_start_utc = report_start.astimezone(UTC)
            report_end_utc = report_end.astimezone(UTC)
            
            all_publishers = ringba_client.get_publisher_payouts(
                report_start=report_start_utc.isoformat().replace('+00:00', 'Z'),
                report_end=report_end_utc.isoformat().replace('+00:00', 'Z')
            )
        
        if all_publishers:
            sheets_client.write_publisher_payouts(all_publishers, clear_existing=False)
            logger.info(f"End-of-day report completed: {len(all_publishers)} publishers synced")
        else:
            logger.warning("End-of-day report: No publisher data found")
            
    except Exception as e:
        logger.exception(f"Failed to run end-of-day report: {e}")


async def run_hourly_report():
    """Scheduled task to run hourly report. Only runs between 9am and 9pm EST."""
    logger.info("Running scheduled hourly report")
    try:
        # Get current time in EST timezone
        est = timezone('America/New_York')
        now_est = datetime.now(est)
        current_hour = now_est.hour
        
        # Safety check: Only run between 9am (9) and 9pm (21) EST
        # After 9pm, data is finalized and should not be overwritten
        if current_hour < 9 or current_hour > 21:
            logger.info(f"Hourly report skipped: Current hour {current_hour} is outside operating window (9am-9pm EST)")
            return
        
        # Calculate previous hour's time range
        previous_hour = now_est - timedelta(hours=1)
        previous_hour_num = previous_hour.hour
        
        # Additional safety check: Only process hours between 9am (inclusive) and 9pm (exclusive)
        # This ensures we don't process data from before 9am or at/after 9pm
        # At 9:05 AM, we'd process hour 8 (8am-8:59am) - skip this
        # At 10:05 AM, we'd process hour 9 (9am-9:59am) - process this
        # At 9:05 PM, we'd process hour 20 (8pm-8:59pm) - process this
        # At 10:05 PM, we'd process hour 21 (9pm-9:59pm) - skip this (data finalized)
        if previous_hour_num < 9 or previous_hour_num >= 21:
            logger.info(f"Hourly report skipped: Previous hour {previous_hour_num} is outside operating window (9am-9pm EST). Data is finalized after 9pm.")
            return
        
        hour_start = previous_hour.replace(minute=0, second=0, microsecond=0)
        hour_end = previous_hour.replace(minute=59, second=59, microsecond=999999)
        
        # Create hour identifier for tracking (format: "YYYY-MM-DD HH:00")
        hour_identifier = hour_start.strftime("%Y-%m-%d %H:00")
        
        # Convert to UTC for API calls
        hour_start_utc = hour_start.astimezone(UTC)
        hour_end_utc = hour_end.astimezone(UTC)
        
        logger.info(f"Pulling hourly data for {hour_identifier}: {hour_start_utc} to {hour_end_utc}")
        
        # Fetch data from Ringba for the previous hour
        publishers = ringba_client.get_publisher_payouts(
            report_start=hour_start_utc.isoformat().replace('+00:00', 'Z'),
            report_end=hour_end_utc.isoformat().replace('+00:00', 'Z')
        )
        
        if publishers:
            # Update Date field to include hour information for hourly tab
            # Extract date from hour_start
            date_str = hour_start.strftime("%Y-%m-%d")
            for pub in publishers:
                pub["Date"] = date_str
            
            # Write to hourly sheet (clears previous hour's data and writes fresh)
            hourly_sheets_client.write_hourly_publisher_payouts(publishers, hour_identifier)
            logger.info(f"Hourly report completed: {len(publishers)} publishers synced for hour {hour_identifier}")
        else:
            logger.warning(f"Hourly report: No publisher data found for hour {hour_identifier}")
            
    except Exception as e:
        logger.exception(f"Failed to run hourly report: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events for the app."""
    # Check if scheduler should be enabled (default: True)
    # Set ENABLE_SCHEDULER=false to disable if using external cron
    enable_scheduler = os.getenv("ENABLE_SCHEDULER", "true").lower() == "true"
    
    if enable_scheduler:
        # Startup: Schedule end-of-day report
        # Run at 9:00 AM EST on weekdays (Monday-Friday)
        scheduler.add_job(
            run_end_of_day_report,
            trigger=CronTrigger(hour=9, minute=0, day_of_week='mon-fri', timezone="America/New_York"),
            id="end_of_day_report",
            replace_existing=True
        )
        
        # Startup: Schedule hourly report
        # Run at minute 5 of every hour between 9am and 9pm EST (e.g., 9:05, 10:05, ..., 21:05)
        # After 9pm, data is finalized and will not be overwritten
        scheduler.add_job(
            run_hourly_report,
            trigger=CronTrigger(minute=5, hour='9-21', timezone="America/New_York"),
            id="hourly_report",
            replace_existing=True
        )
        
        scheduler.start()
        logger.info("Scheduler started - End-of-day report scheduled for 9:00 AM EST on weekdays (Monday-Friday)")
        logger.info("Scheduler started - Hourly report scheduled for minute 5 of every hour between 9am-9pm EST")
    else:
        logger.info("Scheduler disabled - Use external cron or manual triggers")
    
    yield
    
    # Shutdown
    if enable_scheduler:
        scheduler.shutdown()
        logger.info("Scheduler stopped")


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def healthcheck():
    return {"status": "ok", "message": "Ringba → Google Sheets sync is running"}


@app.get("/debug-ringba")
async def debug_ringba(
    report_start: Optional[str] = Query("2025-11-18T00:00:00Z", description="Start date in ISO format"),
    report_end: Optional[str] = Query("2025-11-18T23:59:59Z", description="End date in ISO format")
):
    """
    Debug endpoint to see the raw Ringba API response.
    This helps us understand the response structure.
    """
    try:
        from httpx import Client
        
        request_body = {
            "reportStart": report_start,
            "reportEnd": report_end,
            "groupByColumns": [
                {
                    "column": "publisherName",
                    "displayName": "Publisher"
                },
                {
                    "column": "campaignName",
                    "displayName": "Campaign"
                }
            ],
            "valueColumns": [
                {
                    "column": "payoutAmount",
                    "aggregateFunction": None
                }
            ],
            "orderByColumns": [
                {
                    "column": "payoutAmount",
                    "direction": "desc"
                }
            ],
            "formatTimespans": True,
            "formatPercentages": True,
            "generateRollups": True,
            "maxResultsPerGroup": 1000,
            "filters": [],
            "formatTimeZone": "America/Los_Angeles"
        }
        
        url = f"https://api.ringba.com/v2/{ringba_client.account_id}/insights"
        headers = ringba_client.headers
        
        with Client(timeout=30.0) as client:
            response = client.post(url, json=request_body, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "response_keys": list(data.keys()) if isinstance(data, dict) else "Not a dict",
                    "response_type": type(data).__name__,
                    "response_preview": str(data)[:2000],  # First 2000 chars
                    "full_response": data
                }
            )
    except Exception as e:
        logger.exception("Debug endpoint error")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)}
        )


@app.get("/sync-publisher-payouts")
@app.post("/sync-publisher-payouts")
async def sync_publisher_payouts(
    report_start: Optional[str] = Query(None, description="Start date in ISO format (e.g., 2025-11-18T04:00:00Z)"),
    report_end: Optional[str] = Query(None, description="End date in ISO format (e.g., 2025-11-19T03:59:59Z)"),
    clear_existing: bool = Query(True, description="Clear existing data before writing new data")
):
    """
    Pull publisher payout data from Ringba and write to Google Sheets.
    
    This endpoint can be called:
    - Manually anytime (GET or POST) for on-demand reports
    - Automatically via scheduled end-of-day job (runs at 4:05 AM UTC daily)
    
    Query Parameters:
        report_start: Optional start date (defaults to yesterday)
        report_end: Optional end date (defaults to today)
        clear_existing: Whether to clear existing data before writing (default: True)
    """
    try:
        # Fetch data from Ringba
        publishers = ringba_client.get_publisher_payouts(
            report_start=report_start,
            report_end=report_end
        )
        
        if not publishers:
            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "message": "No publisher data found",
                    "publishers_count": 0
                }
            )
        
        # Write to Google Sheets
        sheets_client.write_publisher_payouts(publishers, clear_existing=clear_existing)
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": f"Synced {len(publishers)} publishers to Google Sheets",
                "publishers_count": len(publishers),
                "publishers": publishers
            }
        )
        
    except Exception as e:
        logger.exception("Failed to sync publisher payouts")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to sync publisher payouts: {str(e)}"
        ) from e


@app.post("/ringba-webhook")
async def ringba_webhook(request: Request):
    """
    Endpoint Ringba will POST to.

    Expected:
      - Content-Type: application/json
      - Body: JSON payload from Ringba webhook

    Behavior:
      - On first call: create header row from keys of payload
      - On each call: append row in header order
    """
    try:
        payload: Dict[str, Any] = await request.json()
    except Exception as e:
        logger.exception("Failed to parse JSON payload")
        raise HTTPException(status_code=400, detail="Invalid JSON") from e

    logger.info("Received Ringba webhook payload: %s", payload)

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload must be a JSON object")

    try:
        sheets_client.append_payload(payload)
    except Exception as e:
        logger.exception("Failed to write to Google Sheets")
        raise HTTPException(status_code=500, detail="Failed to write to Google Sheets") from e

    return JSONResponse(
        status_code=200,
        content={"status": "success", "message": "Webhook received and logged"},
    )
