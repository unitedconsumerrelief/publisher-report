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
            List of dictionaries with Publisher and Payout data
        """
        # Default to yesterday if not provided
        if not report_start:
            yesterday = datetime.utcnow() - timedelta(days=1)
            report_start = yesterday.replace(hour=4, minute=0, second=0, microsecond=0).isoformat() + "Z"
        
        if not report_end:
            today = datetime.utcnow()
            report_end = today.replace(hour=3, minute=59, second=59, microsecond=999999).isoformat() + "Z"

        # Simplified request body - only Publisher and Payout
        request_body = {
            "reportStart": report_start,
            "reportEnd": report_end,
            "groupByColumns": [
                {
                    "column": "publisherName",
                    "displayName": "Publisher"
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
                
                # Extract publisher and payout data from response
                publishers = []
                if "groups" in data:
                    for group in data["groups"]:
                        publisher_name = group.get("publisherName", "Unknown")
                        payout_amount = group.get("payoutAmount", 0)
                        publishers.append({
                            "Publisher": publisher_name,
                            "Payout": payout_amount
                        })
                
                logger.info(f"Retrieved {len(publishers)} publishers from Ringba")
                return publishers
                
        except httpx.HTTPStatusError as e:
            logger.error(f"Ringba API error: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.exception("Failed to fetch data from Ringba API")
            raise

