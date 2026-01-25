# app.py
import os
import logging
from typing import Dict, Any, Optional, List
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
            # IMPORTANT: Always set status to LIVE - the finalization job at 5:05 AM next day will convert to FINAL
            # This ensures LIVE data remains available until 5am the next day
            status = "LIVE"
            
            for pub in publishers:
                pub["Date"] = date_str
                pub["Status"] = status  # Set status for each publisher
            
            # Calculate cumulative data: fetch raw data for all hours from 9am to current hour and sum
            cumulative_publishers = await get_cumulative_hourly_data(
                date_str, previous_hour_num, status
            )
            
            # Write to hourly sheet (clears previous hour's data and writes cumulative totals)
            hourly_sheets_client.write_hourly_publisher_payouts(cumulative_publishers, hour_identifier)
            logger.info(f"Hourly report completed: {len(cumulative_publishers)} publishers synced for hour {hour_identifier} with status {status} (cumulative from 9am)")
        else:
            logger.warning(f"Hourly report: No publisher data found for hour {hour_identifier}")
            
    except Exception as e:
        logger.exception(f"Failed to run hourly report: {e}")


async def finalize_previous_day_data():
    """
    Scheduled task to finalize previous day's LIVE data at 5:05am EST.
    
    IMPORTANT: This only finalizes the PREVIOUS day's data, not today's data.
    This ensures LIVE data remains available until 5am the next day.
    """
    logger.info("Running scheduled finalization of previous day's data")
    try:
        # Get current time in EST timezone
        est = timezone('America/New_York')
        now_est = datetime.now(est)
        current_date_str = now_est.strftime("%Y-%m-%d")
        
        # Calculate previous day (the day we're finalizing)
        previous_day = now_est - timedelta(days=1)
        previous_day_str = previous_day.strftime("%Y-%m-%d")
        
        logger.info(f"Finalizing LIVE data from {previous_day_str} (current date: {current_date_str})")
        
        # Get all data from hourly sheet
        try:
            all_values = hourly_sheets_client.sheet.get_all_values()
            if len(all_values) <= 1:  # Only header or empty
                logger.info("No data to finalize")
                return
            
            # Column indices (0-based):
            # Column A (0): Date
            # Column H (7): Status
            status_col_index = 7  # Status column (0-based, column H, updated for Target column)
            date_col_index = 0    # Date column (0-based, column A)
            
            rows_to_update = []
            rows_skipped_today = 0
            rows_skipped_final = 0
            
            for i in range(1, len(all_values)):  # Skip header row
                row = all_values[i]
                if len(row) <= max(status_col_index, date_col_index):
                    continue
                
                row_date = str(row[date_col_index]).strip()
                row_status = str(row[status_col_index]).strip().upper() if len(row) > status_col_index else ""
                
                # Safety check: NEVER finalize today's data - only previous day
                if row_date == current_date_str:
                    rows_skipped_today += 1
                    logger.debug(f"Skipping row {i+1}: Date is today ({current_date_str}), not finalizing")
                    continue
                
                # Only update LIVE rows from previous day (not FINAL rows)
                if row_status == "LIVE" and row_date == previous_day_str:
                    rows_to_update.append(i + 1)  # 1-indexed row number
                elif row_status == "FINAL":
                    rows_skipped_final += 1
            
            if rows_skipped_today > 0:
                logger.info(f"Skipped {rows_skipped_today} rows from today ({current_date_str}) - keeping them LIVE")
            
            if rows_to_update:
                # Update status to FINAL for all matching rows
                # Update each row individually (gspread's reliable method)
                successful_updates = 0
                failed_updates = 0
                
                for row_num in rows_to_update:
                    try:
                        status_range = f"H{row_num}"  # Column H (Status), specific row
                        hourly_sheets_client.sheet.update(status_range, [["FINAL"]], value_input_option="RAW")
                        successful_updates += 1
                    except Exception as update_error:
                        failed_updates += 1
                        logger.warning(f"Could not update row {row_num}: {update_error}")
                
                if successful_updates > 0:
                    logger.info(f"Finalized {successful_updates} rows from {previous_day_str} (changed LIVE to FINAL)")
                if failed_updates > 0:
                    logger.warning(f"Failed to finalize {failed_updates} rows from {previous_day_str}")
            else:
                logger.info(f"No LIVE rows found to finalize for {previous_day_str} (skipped {rows_skipped_final} FINAL rows, {rows_skipped_today} today rows)")
                
        except Exception as e:
            logger.exception(f"Failed to finalize previous day's data: {e}")
            
    except Exception as e:
        logger.exception(f"Failed to run finalization job: {e}")


async def get_cumulative_hourly_data(
    date_str: str,
    current_hour_num: int,
    status: str
) -> List[Dict[str, Any]]:
    """
    Calculate cumulative totals by fetching raw data for all hours from 9am to current hour.
    
    Args:
        date_str: Date string (e.g., "2026-01-06")
        current_hour_num: Current hour number (9-20)
        current_hour_publishers: Publishers data for current hour
        status: Status to set (LIVE or FINAL)
    
    Returns:
        List of publishers with cumulative totals from 9am to current hour
    """
    from pytz import timezone as tz
    est = tz('America/New_York')
    
    # Dictionary to store cumulative totals by (Publisher, Campaign, Target)
    cumulative_dict = {}
    
    # Fetch raw data for each hour from 9am to current hour
    # Note: current_hour_num is the hour we're processing (previous hour from now)
    for hour_num in range(9, current_hour_num + 1):
        try:
            # Create hour time range
            hour_start = est.localize(datetime.strptime(f"{date_str} {hour_num:02d}:00:00", "%Y-%m-%d %H:%M:%S"))
            hour_end = hour_start.replace(minute=59, second=59, microsecond=999999)
            
            # Convert to UTC for API calls
            hour_start_utc = hour_start.astimezone(UTC)
            hour_end_utc = hour_end.astimezone(UTC)
            
            # Fetch data for this hour
            hour_publishers = ringba_client.get_publisher_payouts(
                report_start=hour_start_utc.isoformat().replace('+00:00', 'Z'),
                report_end=hour_end_utc.isoformat().replace('+00:00', 'Z')
            )
            
            # Add to cumulative totals
            for pub in hour_publishers:
                publisher = pub.get("Publisher", "")
                campaign = pub.get("Campaign", "")
                target = pub.get("Target", "")
                key = (publisher, campaign, target)
                
                payout = float(pub.get("Payout", 0))
                completed_calls = int(pub.get("Completed Calls", 0))
                paid_calls = int(pub.get("Paid Calls", 0))
                
                if key in cumulative_dict:
                    cumulative_dict[key]["Payout"] += payout
                    cumulative_dict[key]["Completed Calls"] += completed_calls
                    cumulative_dict[key]["Paid Calls"] += paid_calls
                else:
                    cumulative_dict[key] = {
                        "Publisher": publisher,
                        "Campaign": campaign,
                        "Target": target,
                        "Payout": payout,
                        "Completed Calls": completed_calls,
                        "Paid Calls": paid_calls
                    }
            
            logger.info(f"Fetched data for hour {hour_num}: {len(hour_publishers)} publishers")
            # Log sample data for debugging
            if hour_publishers:
                sample = hour_publishers[0]
                logger.info(f"Sample data for hour {hour_num}: Publisher={sample.get('Publisher')}, Payout={sample.get('Payout')}, Completed={sample.get('Completed Calls')}, Paid={sample.get('Paid Calls')}")
        except Exception as e:
            logger.warning(f"Failed to fetch data for hour {hour_num}: {e}")
            continue
    
    # Convert to list and add Date and Status
    cumulative_list = []
    for key, pub_data in cumulative_dict.items():
        pub_data["Date"] = date_str
        pub_data["Status"] = status
        cumulative_list.append(pub_data)
    
    logger.info(f"Calculated cumulative totals: {len(cumulative_list)} unique publishers from 9am to hour {current_hour_num}")
    # Log sample cumulative data for debugging
    if cumulative_list:
        for pub in cumulative_list[:3]:  # Log first 3 publishers
            logger.info(f"Cumulative: Publisher={pub.get('Publisher')}, Campaign={pub.get('Campaign')}, Payout={pub.get('Payout')}, Completed={pub.get('Completed Calls')}, Paid={pub.get('Paid Calls')}")
    return cumulative_list


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
        
        # Startup: Schedule finalization of previous day's data
        # Run at 5:05 AM EST daily to finalize yesterday's LIVE data
        # This allows users to pull "Today" data until 5am the next day
        scheduler.add_job(
            finalize_previous_day_data,
            trigger=CronTrigger(hour=5, minute=5, timezone="America/New_York"),
            id="finalize_previous_day",
            replace_existing=True
        )
        
        scheduler.start()
        logger.info("Scheduler started - End-of-day report scheduled for 9:00 AM EST on weekdays (Monday-Friday)")
        logger.info("Scheduler started - Hourly report scheduled for minute 5 of every hour between 9am-9pm EST")
        logger.info("Scheduler started - Previous day finalization scheduled for 5:05 AM EST daily")
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
                },
                {
                    "column": "targetName",
                    "displayName": "Target"
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


@app.get("/test-hourly-report")
@app.post("/test-hourly-report")
async def test_hourly_report():
    """
    Test endpoint to manually trigger hourly report.
    Useful for testing the hourly reporting functionality.
    """
    try:
        logger.info("Manual hourly report test triggered")
        await run_hourly_report()
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": "Hourly report test completed. Check logs and Hourly tab for results."
            }
        )
    except Exception as e:
        logger.exception("Failed to run test hourly report")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Failed to run hourly report: {str(e)}"
            }
        )


@app.get("/test-daily-report")
@app.post("/test-daily-report")
async def test_daily_report():
    """
    Test endpoint to manually trigger daily report.
    Useful for testing the daily reporting functionality or pulling missing dates.
    """
    try:
        logger.info("Manual daily report test triggered")
        await run_end_of_day_report()
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": "Daily report test completed. Check logs and Sheet1 tab for results."
            }
        )
    except Exception as e:
        logger.exception("Failed to run test daily report")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Failed to run daily report: {str(e)}"
            }
        )


@app.get("/sync-date")
@app.post("/sync-date")
async def sync_date(
    date: str = Query(..., description="Date in YYYY-MM-DD format (e.g., 2026-01-23)"),
    clear_existing: bool = Query(False, description="Clear existing data for this date before writing")
):
    """
    Pull publisher payout data for a specific date and write to Google Sheets.
    Handles timezone conversion correctly (EST timezone).
    
    Args:
        date: Date string in YYYY-MM-DD format
        clear_existing: If True, clears existing data for this date before writing
    """
    try:
        # Parse date and create time range in EST
        est = timezone('America/New_York')
        try:
            date_obj = datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": f"Invalid date format. Use YYYY-MM-DD (e.g., 2026-01-23)"}
            )
        
        # Create date range for the specified date in EST
        report_start = est.localize(date_obj.replace(hour=0, minute=0, second=0, microsecond=0))
        report_end = est.localize(date_obj.replace(hour=23, minute=59, second=59, microsecond=999999))
        
        # Convert to UTC for API calls
        report_start_utc = report_start.astimezone(UTC)
        report_end_utc = report_end.astimezone(UTC)
        
        logger.info(f"Pulling data for date {date} (EST): {report_start_utc} to {report_end_utc}")
        
        # Fetch data from Ringba
        publishers = ringba_client.get_publisher_payouts(
            report_start=report_start_utc.isoformat().replace('+00:00', 'Z'),
            report_end=report_end_utc.isoformat().replace('+00:00', 'Z')
        )
        
        if not publishers:
            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "message": f"No publisher data found for date {date}",
                    "publishers_count": 0
                }
            )
        
        # If clear_existing is True, remove existing rows for this date first
        if clear_existing:
            try:
                all_values = sheets_client.sheet.get_all_values()
                date_col_index = 0  # Column A: Date
                rows_to_delete = []
                
                for i in range(1, len(all_values)):  # Skip header row
                    row = all_values[i]
                    if len(row) > date_col_index:
                        row_date = str(row[date_col_index]).strip()
                        if row_date == date:
                            rows_to_delete.append(i + 1)  # 1-indexed row number
                
                if rows_to_delete:
                    # Delete rows from bottom to top to preserve indices
                    rows_to_delete.sort(reverse=True)
                    for row_num in rows_to_delete:
                        try:
                            sheets_client.sheet.delete_rows(row_num)
                        except Exception as e:
                            logger.warning(f"Could not delete row {row_num}: {e}")
                    logger.info(f"Cleared {len(rows_to_delete)} existing rows for date {date}")
            except Exception as e:
                logger.warning(f"Could not clear existing data for date {date}: {e}")
        
        # Write to Google Sheets (with duplicate prevention)
        sheets_client.write_publisher_payouts(publishers, clear_existing=False)
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": f"Synced {len(publishers)} publishers for date {date} to Google Sheets",
                "publishers_count": len(publishers),
                "date": date
            }
        )
        
    except Exception as e:
        logger.exception(f"Failed to sync date {date}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Failed to sync date {date}: {str(e)}"
            }
        )


@app.get("/cleanup-hourly-duplicates")
@app.post("/cleanup-hourly-duplicates")
async def cleanup_hourly_duplicates():
    """
    Cleanup endpoint to remove duplicate entries for the same hour.
    Keeps only the most recent entry for each hour.
    """
    try:
        logger.info("Hourly duplicates cleanup triggered")
        
        # Get all data from the hourly sheet
        all_values = hourly_sheets_client.sheet.get_all_values()
        if len(all_values) <= 1:
            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "message": "No data to clean up",
                    "rows_removed": 0
                }
            )
        
        hour_col_index = 7  # Hour column index
        status_col_index = 6  # Status column index
        rows_by_hour = {}  # Track rows by hour identifier
        
        # Group rows by hour identifier (excluding FINAL status rows)
        for i in range(1, len(all_values)):  # Skip header
            row = all_values[i]
            if len(row) > hour_col_index:
                # Check status - NEVER touch rows with "FINAL" status
                row_status = str(row[status_col_index]).strip() if len(row) > status_col_index else ""
                if row_status.upper() == "FINAL":
                    continue  # Skip FINAL rows - they are permanent
                
                hour_identifier = str(row[hour_col_index]).strip()
                if hour_identifier:
                    if hour_identifier not in rows_by_hour:
                        rows_by_hour[hour_identifier] = []
                    rows_by_hour[hour_identifier].append(i + 1)  # 1-indexed row number
        
        # Find hours with duplicates
        rows_to_delete = []
        for hour_identifier, row_numbers in rows_by_hour.items():
            if len(row_numbers) > 1:
                # Keep the last row (most recent), delete the rest
                row_numbers.sort()  # Sort ascending
                rows_to_delete.extend(row_numbers[:-1])  # All except the last one
                logger.info(f"Found {len(row_numbers)} rows for hour {hour_identifier}, keeping most recent, deleting {len(row_numbers) - 1}")
        
        # Delete duplicate rows (from bottom to top)
        if rows_to_delete:
            rows_to_delete.sort(reverse=True)
            for row_num in rows_to_delete:
                try:
                    hourly_sheets_client.sheet.delete_rows(row_num)
                except Exception as e:
                    logger.warning(f"Could not delete row {row_num}: {e}")
            
            logger.info(f"Cleaned up {len(rows_to_delete)} duplicate rows")
            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "message": f"Cleaned up {len(rows_to_delete)} duplicate rows",
                    "rows_removed": len(rows_to_delete)
                }
            )
        else:
            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "message": "No duplicates found",
                    "rows_removed": 0
                }
            )
            
    except Exception as e:
        logger.exception("Failed to cleanup hourly duplicates")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Failed to cleanup duplicates: {str(e)}"
            }
        )
