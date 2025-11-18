# sheets_client.py
import os
import json
import logging
from typing import Dict, Any, List

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)


class GoogleSheetsClient:
    """
    Simple wrapper around gspread to:
      - connect using a service account JSON (in env)
      - ensure header row exists (first payload defines columns)
      - append new rows in header order
    """

    def __init__(self, spreadsheet_id: str, worksheet_name: str = "Sheet1"):
        self.spreadsheet_id = spreadsheet_id
        self.worksheet_name = worksheet_name
        self.client = self._authorize()
        self.sheet = self._open_worksheet()

    def _authorize(self) -> gspread.Client:
        """
        Authorize using a service account JSON stored in
        GOOGLE_SERVICE_ACCOUNT_JSON env var.
        """
        sa_json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        if not sa_json_str:
            raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON env var is required")

        try:
            sa_info = json.loads(sa_json_str)
        except json.JSONDecodeError as e:
            raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON") from e

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

        credentials = Credentials.from_service_account_info(sa_info, scopes=scopes)
        client = gspread.authorize(credentials)
        return client

    def _open_worksheet(self):
        sh = self.client.open_by_key(self.spreadsheet_id)
        try:
            ws = sh.worksheet(self.worksheet_name)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=self.worksheet_name, rows=1000, cols=50)
        return ws

    def _get_header_row(self) -> List[str]:
        """
        Returns the list of header columns (or empty if no header).
        """
        try:
            values = self.sheet.row_values(1)
            return values
        except Exception as e:
            logger.warning("Could not read header row: %s", e)
            return []

    def _set_header_row(self, keys: List[str]) -> None:
        """
        Sets the header row from a list of keys.
        """
        logger.info("Setting header row: %s", keys)
        self.sheet.update("1:1", [keys])  # row 1, entire row

    def append_payload(self, payload: Dict[str, Any]) -> None:
        """
        Ensure headers exist; append row in header order.
        - First payload defines columns (sorted by key for stability).
        - Later payloads use existing header to map values.
        """
        header = self._get_header_row()

        # If no header yet, create it from this payload's keys
        if not header:
            header = sorted(payload.keys())
            self._set_header_row(header)

        # Build row values in the same order as header
        row = []
        for key in header:
            value = payload.get(key, "")
            # Convert nested dicts/lists to JSON string
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False)
            row.append(str(value))

        logger.info("Appending row: %s", row)
        self.sheet.append_row(row, value_input_option="RAW")

    def write_publisher_payouts(self, publishers: List[Dict[str, Any]], clear_existing: bool = True) -> None:
        """
        Write publisher payout data to the sheet.
        
        Args:
            publishers: List of dicts with "Publisher", "Campaign", "Payout", and "Date" keys
            clear_existing: If True, clear existing data before writing (default: True)
        """
        if not publishers:
            logger.warning("No publisher data to write")
            return

        # Define header order: Date, Publisher, Campaign, Payout
        header = ["Date", "Publisher", "Campaign", "Payout"]
        
        # Set header row
        self._set_header_row(header)
        
        # Clear existing data if requested (keep header row)
        if clear_existing:
            try:
                # Get all existing data rows
                existing_data = self.sheet.get_all_values()
                if len(existing_data) > 1:  # More than just header
                    # Clear from row 2 onwards
                    range_to_clear = f"2:{len(existing_data)}"
                    self.sheet.batch_clear([range_to_clear])
                    logger.info("Cleared existing data")
            except Exception as e:
                logger.warning(f"Could not clear existing data: {e}")

        # Build rows
        rows = []
        for pub in publishers:
            row = [
                str(pub.get("Date", "")),
                str(pub.get("Publisher", "")),
                str(pub.get("Campaign", "")),
                str(pub.get("Payout", ""))
            ]
            rows.append(row)

        # Write rows - append if not clearing, overwrite if clearing
        if rows:
            if clear_existing:
                # Overwrite starting from row 2
                range_name = f"2:{len(rows) + 1}"
                self.sheet.update(range_name, rows, value_input_option="RAW")
                logger.info(f"Wrote {len(rows)} publisher rows to sheet (overwritten)")
            else:
                # Append to the end of existing data
                try:
                    # Get all existing values to find the last row
                    all_values = self.sheet.get_all_values()
                    next_row = len(all_values) + 1
                    
                    # Append rows starting from next_row
                    if next_row == 2:
                        # No data yet, start from row 2
                        range_name = f"2:{len(rows) + 1}"
                        self.sheet.update(range_name, rows, value_input_option="RAW")
                    else:
                        # Append after existing data
                        range_name = f"{next_row}:{next_row + len(rows) - 1}"
                        self.sheet.update(range_name, rows, value_input_option="RAW")
                    logger.info(f"Appended {len(rows)} publisher rows to sheet (starting at row {next_row})")
                except Exception as e:
                    logger.warning(f"Could not append data, trying direct append: {e}")
                    # Fallback: use append_row for each row
                    for row in rows:
                        self.sheet.append_row(row, value_input_option="RAW")
                    logger.info(f"Appended {len(rows)} publisher rows to sheet (using append_row)")