# ringba_client.py
import os
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import httpx

logger = logging.getLogger(__name__)


class RingbaClient:
    """
    Client for fetching data from Ringba API.
    """

    def __init__(self, api_token: Optional[str] = None, account_id: Optional[str] = None):
        """
        Initialize Ringba client.
        
        Args:
            api_token: Ringba API token (from RINGBA_API_TOKEN env var if not provided)
            account_id: Ringba account ID (from RINGBA_ACCOUNT_ID env var if not provided)
        """
        self.api_token = api_token or os.getenv("RINGBA_API_TOKEN")
        self.account_id = account_id or os.getenv("RINGBA_ACCOUNT_ID")
        
        if not self.api_token:
            raise RuntimeError("RINGBA_API_TOKEN env var is required")
        if not self.account_id:
            raise RuntimeError("RINGBA_ACCOUNT_ID env var is required")
        
        self.base_url = "https://api.ringba.com"
        # Ringba uses: Authorization: Token {token}
        self.headers = {
            "Authorization": f"Token {self.api_token}",
            "Content-Type": "application/json",
        }

    def get_publisher_payouts(
        self,
        report_start: Optional[str] = None,
        report_end: Optional[str] = None,
        timezone: str = "America/Los_Angeles"
    ) -> List[Dict[str, Any]]:
        """
        Fetch publisher payout data from Ringba.
        
        Args:
            report_start: Start date in ISO format (e.g., "2025-11-18T04:00:00Z")
                          If not provided, defaults to yesterday
            report_end: End date in ISO format (e.g., "2025-11-19T03:59:59Z")
                        If not provided, defaults to today
            timezone: Timezone for the report (default: "America/Los_Angeles")
        
        Returns:
            List of dictionaries with Publisher, Campaign, Payout, and Date data
        """
        # Default to yesterday if not provided
        if not report_start:
            yesterday = datetime.utcnow() - timedelta(days=1)
            report_start = yesterday.replace(hour=4, minute=0, second=0, microsecond=0).isoformat() + "Z"
        
        if not report_end:
            today = datetime.utcnow()
            report_end = today.replace(hour=3, minute=59, second=59, microsecond=999999).isoformat() + "Z"
        
        # Extract date from report_start (format: YYYY-MM-DD)
        # report_start format: "2025-11-18T00:00:00Z" -> extract "2025-11-18"
        report_date = report_start.split("T")[0] if "T" in report_start else report_start[:10]

        # Request body - Publisher, Campaign, and Payout
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
            "formatTimeZone": timezone
        }

        url = f"{self.base_url}/v2/{self.account_id}/insights"
        
        logger.info(f"Fetching publisher payouts from Ringba: {report_start} to {report_end}")
        
        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.post(url, json=request_body, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                
                # Log the full response structure for debugging
                logger.info(f"Ringba API response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                
                # Extract publisher and payout data from response
                publishers = []
                
                # Ringba API returns: { "isSuccessful": true, "transactionId": "...", "report": { "records": [...] } }
                if isinstance(data, dict) and "report" in data:
                    report = data["report"]
                    if isinstance(report, dict) and "records" in report:
                        records = report["records"]
                        if isinstance(records, list):
                            for record in records:
                                if isinstance(record, dict):
                                    # Skip records without publisherName (like totals/rollups)
                                    publisher_name = record.get("publisherName")
                                    if not publisher_name:
                                        continue
                                    
                                    # Get campaign name
                                    campaign_name = record.get("campaignName", "")
                                    
                                    # payoutAmount comes as a string, convert to float
                                    payout_amount_str = record.get("payoutAmount", "0")
                                    try:
                                        payout_amount = float(payout_amount_str)
                                    except (ValueError, TypeError):
                                        payout_amount = 0.0
                                    
                                    publishers.append({
                                        "Publisher": publisher_name,
                                        "Campaign": campaign_name,
                                        "Payout": payout_amount,
                                        "Date": report_date
                                    })
                
                logger.info(f"Retrieved {len(publishers)} publishers from Ringba")
                if len(publishers) == 0:
                    logger.warning(f"No publishers found. Response structure: {str(data)[:500]}")
                return publishers
                
        except httpx.HTTPStatusError as e:
            logger.error(f"Ringba API error: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.exception("Failed to fetch data from Ringba API")
            raise

