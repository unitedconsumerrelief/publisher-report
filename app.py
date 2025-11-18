# app.py
import os
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

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


async def run_end_of_day_report():
    """Scheduled task to run end-of-day report."""
    logger.info("Running scheduled end-of-day report")
    try:
        # Calculate yesterday's date range (4 AM to 3:59:59 AM next day)
        yesterday = datetime.utcnow() - timedelta(days=1)
        report_start = yesterday.replace(hour=4, minute=0, second=0, microsecond=0).isoformat() + "Z"
        report_end = datetime.utcnow().replace(hour=3, minute=59, second=59, microsecond=999999).isoformat() + "Z"
        
        publishers = ringba_client.get_publisher_payouts(
            report_start=report_start,
            report_end=report_end
        )
        
        if publishers:
            sheets_client.write_publisher_payouts(publishers, clear_existing=True)
            logger.info(f"End-of-day report completed: {len(publishers)} publishers synced")
        else:
            logger.warning("End-of-day report: No publisher data found")
            
    except Exception as e:
        logger.exception(f"Failed to run end-of-day report: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events for the app."""
    # Check if scheduler should be enabled (default: True)
    # Set ENABLE_SCHEDULER=false to disable if using external cron
    enable_scheduler = os.getenv("ENABLE_SCHEDULER", "true").lower() == "true"
    
    if enable_scheduler:
        # Startup: Schedule end-of-day report
        # Run at 4:05 AM UTC (adjust timezone as needed)
        # This ensures all data from the previous day is finalized
        scheduler.add_job(
            run_end_of_day_report,
            trigger=CronTrigger(hour=4, minute=5, timezone="UTC"),
            id="end_of_day_report",
            replace_existing=True
        )
        scheduler.start()
        logger.info("Scheduler started - End-of-day report scheduled for 4:05 AM UTC daily")
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
