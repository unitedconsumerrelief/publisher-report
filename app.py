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

if not SPREADSHEET_ID:
    raise RuntimeError("SPREADSHEET_ID env var is required")

sheets_client = GoogleSheetsClient(
    spreadsheet_id=SPREADSHEET_ID,
    worksheet_name=WORKSHEET_NAME,
)

ringba_client = RingbaClient()

# Initialize scheduler for end-of-day reports
scheduler = AsyncIOScheduler()


async def run_hourly_refresh():
    """
    Scheduled task to run hourly refresh for today's data.
    Will skip if today's data has already been finalized (after 9 PM).
    """
    logger.info("Running hourly refresh for today's data")
    try:
        # Get current date/time in EST timezone
        est = timezone('America/New_York')
        now_est = datetime.now(est)
        today_date = now_est.strftime('%Y-%m-%d')
        
        # Check if today's data has already been finalized
        if sheets_client._has_finalized_data_for_date(today_date):
            logger.info(f"Today's data ({today_date}) has already been finalized. Skipping hourly refresh.")
            return
        
        # Get today's data from start of day to now
        today_start = now_est.replace(hour=0, minute=0, second=0, microsecond=0)
        today_end = now_est
        
        # Convert to UTC for API calls
        today_start_utc = today_start.astimezone(UTC)
        today_end_utc = today_end.astimezone(UTC)
        
        logger.info(f"Pulling today's data ({today_date}): {today_start_utc.isoformat()} to {today_end_utc.isoformat()}")
        
        publishers = ringba_client.get_publisher_payouts(
            report_start=today_start_utc.isoformat().replace('+00:00', 'Z'),
            report_end=today_end_utc.isoformat().replace('+00:00', 'Z')
        )
        
        if publishers:
            # Write with Status = "LIVE", replacing any existing LIVE data for today
            # This method will also check for finalized data internally as a safety measure
            sheets_client.write_today_hourly_payouts(publishers, today_date)
            logger.info(f"Hourly refresh completed: {len(publishers)} publishers synced for {today_date}")
        else:
            logger.warning(f"Hourly refresh: No publisher data found for {today_date}")
            
    except Exception as e:
        logger.exception(f"Failed to run hourly refresh: {e}")


async def run_end_of_day_report():
    """
    Scheduled task to finalize today's LIVE data.
    Changes Status from "LIVE" to "FINAL" for all today's rows.
    Historical data is already in the system from when it was "today", so we don't pull it again.
    """
    logger.info("Running scheduled end-of-day report")
    try:
        # Get current date in EST timezone
        est = timezone('America/New_York')
        now_est = datetime.now(est)
        today_date = now_est.strftime('%Y-%m-%d')
        
        # Finalize today's LIVE data (change Status from LIVE to FINAL)
        # This is the final update for today - the data is already accurate from hourly pulls
        finalized_count = sheets_client.finalize_today_data(today_date)
        
        if finalized_count > 0:
            logger.info(f"End-of-day report completed: Finalized {finalized_count} LIVE rows for {today_date} (Status changed to FINAL)")
        else:
            logger.warning(f"End-of-day report: No LIVE rows found to finalize for {today_date}")
            
    except Exception as e:
        logger.exception(f"Failed to run end-of-day report: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events for the app."""
    # Check if scheduler should be enabled (default: True)
    # Set ENABLE_SCHEDULER=false to disable if using external cron
    enable_scheduler = os.getenv("ENABLE_SCHEDULER", "true").lower() == "true"
    
    if enable_scheduler:
        # Startup: Schedule hourly refresh for today's data
        # Run every hour at minute 0 (e.g., 1:00, 2:00, 3:00, etc.)
        scheduler.add_job(
            run_hourly_refresh,
            trigger=CronTrigger(minute=0, timezone="America/New_York"),
            id="hourly_refresh",
            replace_existing=True
        )
        logger.info("Scheduled hourly refresh - runs every hour at :00 minutes EST")
        
        # Schedule end-of-day report
        # Run at 9:00 PM EST every day to finalize today's data
        scheduler.add_job(
            run_end_of_day_report,
            trigger=CronTrigger(hour=21, minute=0, timezone="America/New_York"),
            id="end_of_day_report",
            replace_existing=True
        )
        logger.info("Scheduled end-of-day report - runs at 9:00 PM EST every day")
        
        scheduler.start()
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
                },
                {
                    "column": "completedCalls",
                    "aggregateFunction": "count"
                },
                {
                    "column": "payoutCount",
                    "aggregateFunction": "count"
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
    - Automatically via scheduled end-of-day job (runs at 9:00 PM EST on weekdays)
    
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


@app.get("/sync-today-hourly")
@app.post("/sync-today-hourly")
async def sync_today_hourly():
    """
    Manually trigger hourly refresh for today's data.
    This replaces any existing LIVE data for today with fresh data from Ringba.
    
    This endpoint:
    - Fetches data from start of today to now
    - Deletes existing LIVE rows for today
    - Writes new data with Status = "LIVE"
    """
    try:
        # Get current date/time in EST timezone
        est = timezone('America/New_York')
        now_est = datetime.now(est)
        today_date = now_est.strftime('%Y-%m-%d')
        
        # Get today's data from start of day to now
        today_start = now_est.replace(hour=0, minute=0, second=0, microsecond=0)
        today_end = now_est
        
        # Convert to UTC for API calls
        today_start_utc = today_start.astimezone(UTC)
        today_end_utc = today_end.astimezone(UTC)
        
        logger.info(f"Manual hourly refresh for {today_date}: {today_start_utc.isoformat()} to {today_end_utc.isoformat()}")
        
        publishers = ringba_client.get_publisher_payouts(
            report_start=today_start_utc.isoformat().replace('+00:00', 'Z'),
            report_end=today_end_utc.isoformat().replace('+00:00', 'Z')
        )
        
        if not publishers:
            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "message": f"No publisher data found for {today_date}",
                    "publishers_count": 0,
                    "date": today_date
                }
            )
        
        # Check if today's data has already been finalized
        if sheets_client._has_finalized_data_for_date(today_date):
            return JSONResponse(
                status_code=200,
                content={
                    "status": "skipped",
                    "message": f"Today's data ({today_date}) has already been finalized. No new LIVE data written to prevent duplicates.",
                    "publishers_count": 0,
                    "date": today_date
                }
            )
        
        # Write with Status = "LIVE", replacing any existing LIVE data for today
        # This method will also check for finalized data internally as a safety measure
        sheets_client.write_today_hourly_payouts(publishers, today_date)
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": f"Hourly refresh completed: {len(publishers)} publishers synced for {today_date}",
                "publishers_count": len(publishers),
                "date": today_date
            }
        )
        
    except Exception as e:
        logger.exception("Failed to sync today's hourly data")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to sync today's hourly data: {str(e)}"
        ) from e


@app.get("/cleanup-duplicates")
@app.post("/cleanup-duplicates")
async def cleanup_duplicates(
    date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format (defaults to today)")
):
    """
    Clean up duplicate LIVE rows for a date that already has FINAL rows.
    This removes LIVE rows when FINAL rows already exist to prevent duplicates.
    
    Query Parameters:
        date: Optional date in YYYY-MM-DD format (defaults to today)
    """
    try:
        if date:
            target_date = date
        else:
            # Default to today
            est = timezone('America/New_York')
            now_est = datetime.now(est)
            target_date = now_est.strftime('%Y-%m-%d')
        
        # Validate date format
        try:
            datetime.strptime(target_date, '%Y-%m-%d')
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid date format. Use YYYY-MM-DD format. Got: {target_date}"
            )
        
        # Clean up duplicate LIVE rows
        deleted_count = sheets_client.cleanup_duplicate_live_rows(target_date)
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": f"Cleaned up {deleted_count} duplicate LIVE rows for {target_date}",
                "date": target_date,
                "rows_deleted": deleted_count
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to cleanup duplicates")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to cleanup duplicates: {str(e)}"
        ) from e


@app.get("/finalize-date")
@app.post("/finalize-date")
async def finalize_date(
    date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format (defaults to today)")
):
    """
    Manually finalize LIVE data for a specific date.
    Changes Status from "LIVE" to "FINAL" for all rows with the specified date.
    
    Query Parameters:
        date: Optional date in YYYY-MM-DD format (defaults to today)
    """
    try:
        if date:
            target_date = date
        else:
            # Default to today
            est = timezone('America/New_York')
            now_est = datetime.now(est)
            target_date = now_est.strftime('%Y-%m-%d')
        
        # Validate date format
        try:
            datetime.strptime(target_date, '%Y-%m-%d')
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid date format. Use YYYY-MM-DD format. Got: {target_date}"
            )
        
        # Finalize LIVE data for the specified date
        finalized_count = sheets_client.finalize_today_data(target_date)
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": f"Finalized {finalized_count} LIVE rows for {target_date}",
                "date": target_date,
                "rows_finalized": finalized_count
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to finalize date")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to finalize date: {str(e)}"
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
