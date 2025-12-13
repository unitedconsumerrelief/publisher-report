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

    def _ensure_status_column(self) -> None:
        """
        Ensure the Status column (Column F) exists in the header.
        If header doesn't have Status column, add it.
        """
        header = self._get_header_row()
        
        # Check if Status column exists (should be column index 5, which is column F)
        # We'll check if any column contains "Status" or if we need to add it
        has_status = any(col.lower() == "status" for col in header)
        
        if not has_status:
            # Add Status column if it doesn't exist
            # If header is empty or has fewer than 6 columns, extend it
            while len(header) < 6:
                header.append("")
            # Set column F (index 5) to "Status"
            header[5] = "Status"
            self._set_header_row(header)
            logger.info("Added Status column to header")

    def _delete_today_live_rows(self, today_date: str) -> int:
        """
        Delete all rows where Date = today_date AND Status = "LIVE".
        
        Args:
            today_date: Date string in YYYY-MM-DD format
            
        Returns:
            Number of rows deleted
        """
        try:
            all_values = self.sheet.get_all_values()
            if len(all_values) <= 1:  # Only header or empty
                return 0
            
            header = all_values[0]
            
            # Find column indices
            date_col_idx = None
            status_col_idx = None
            
            for idx, col_name in enumerate(header):
                if col_name.lower() == "date":
                    date_col_idx = idx
                elif col_name.lower() == "status":
                    status_col_idx = idx
            
            if date_col_idx is None:
                logger.warning("Date column not found in header")
                return 0
            
            # Collect row numbers to delete (1-indexed, but we skip header)
            rows_to_delete = []
            for row_idx, row in enumerate(all_values[1:], start=2):  # Start from row 2
                # Ensure row has enough columns
                if len(row) <= date_col_idx:
                    continue
                
                row_date = str(row[date_col_idx]).strip()
                
                # Check if this is today's date
                if row_date == today_date:
                    # If Status column exists, check if it's LIVE
                    if status_col_idx is not None and len(row) > status_col_idx:
                        status = str(row[status_col_idx]).strip().upper()
                        if status == "LIVE":
                            rows_to_delete.append(row_idx)
                    elif status_col_idx is None:
                        # If Status column doesn't exist yet, assume all today's rows are LIVE
                        # This handles the case where Status column was just added
                        rows_to_delete.append(row_idx)
            
            # Delete rows in reverse order to maintain correct indices
            if rows_to_delete:
                rows_to_delete.sort(reverse=True)
                for row_num in rows_to_delete:
                    self.sheet.delete_rows(row_num)
                logger.info(f"Deleted {len(rows_to_delete)} LIVE rows for date {today_date}")
                return len(rows_to_delete)
            
            return 0
            
        except Exception as e:
            logger.exception(f"Error deleting today's LIVE rows: {e}")
            return 0

    def _has_finalized_data_for_date(self, target_date: str) -> bool:
        """
        Check if there are any FINAL rows for the specified date.
        
        Args:
            target_date: Date string in YYYY-MM-DD format
            
        Returns:
            True if there are FINAL rows for this date, False otherwise
        """
        try:
            all_values = self.sheet.get_all_values()
            if len(all_values) <= 1:
                return False
            
            header = all_values[0]
            
            # Find column indices
            date_col_idx = None
            status_col_idx = None
            
            for idx, col_name in enumerate(header):
                if col_name.lower() == "date":
                    date_col_idx = idx
                elif col_name.lower() == "status":
                    status_col_idx = idx
            
            if date_col_idx is None or status_col_idx is None:
                return False
            
            # Check if there are any FINAL rows for this date
            for row in all_values[1:]:
                if len(row) > date_col_idx:
                    row_date = str(row[date_col_idx]).strip()
                    if row_date == target_date:
                        if len(row) > status_col_idx:
                            status = str(row[status_col_idx]).strip().upper()
                            if status == "FINAL":
                                return True
            
            return False
            
        except Exception as e:
            logger.exception(f"Error checking for finalized data: {e}")
            return False

    def write_today_hourly_payouts(self, publishers: List[Dict[str, Any]], today_date: str) -> None:
        """
        Write today's hourly publisher payout data, replacing any existing LIVE data for today.
        
        IMPORTANT: If today's data has already been finalized (Status = FINAL), 
        this method will NOT write new LIVE data to prevent duplicates.
        
        Args:
            publishers: List of dicts with "Publisher", "Campaign", "Payout", and "Date" keys
            today_date: Date string in YYYY-MM-DD format for today
        """
        if not publishers:
            logger.warning("No publisher data to write for hourly refresh")
            return

        # Ensure Status column exists
        self._ensure_status_column()
        
        # CRITICAL: Check if today's data has already been finalized
        # If it has, don't write new LIVE data to prevent duplicates
        if self._has_finalized_data_for_date(today_date):
            logger.warning(f"Today's data ({today_date}) has already been finalized. Skipping hourly refresh to prevent duplicates.")
            return
        
        # Delete existing LIVE rows for today
        deleted_count = self._delete_today_live_rows(today_date)
        logger.info(f"Deleted {deleted_count} existing LIVE rows for {today_date}")

        # Get current header to maintain column order
        header = self._get_header_row()
        
        # Ensure header has the required columns
        required_cols = ["Date", "Publisher", "Campaign", "Payout", "Status"]
        header_dict = {col.lower(): idx for idx, col in enumerate(header)}
        
        # Build rows with Status = "LIVE"
        rows = []
        for pub in publishers:
            row = [""] * max(len(header), 6)  # Ensure at least 6 columns
            
            # Set Date
            if "date" in header_dict:
                row[header_dict["date"]] = str(pub.get("Date", today_date))
            elif len(header) > 0:
                row[0] = str(pub.get("Date", today_date))
            
            # Set Publisher
            if "publisher" in header_dict:
                row[header_dict["publisher"]] = str(pub.get("Publisher", ""))
            elif len(header) > 1:
                row[1] = str(pub.get("Publisher", ""))
            
            # Set Campaign
            if "campaign" in header_dict:
                row[header_dict["campaign"]] = str(pub.get("Campaign", ""))
            elif len(header) > 2:
                row[2] = str(pub.get("Campaign", ""))
            
            # Set Payout
            if "payout" in header_dict:
                row[header_dict["payout"]] = str(pub.get("Payout", ""))
            elif len(header) > 3:
                row[3] = str(pub.get("Payout", ""))
            
            # Set Status = "LIVE" (Column F, index 5)
            if "status" in header_dict:
                row[header_dict["status"]] = "LIVE"
            else:
                row[5] = "LIVE"
            
            rows.append(row)

        # Append new rows
        if rows:
            try:
                all_values = self.sheet.get_all_values()
                next_row = len(all_values) + 1
                
                # Update header if needed to match our row structure
                if len(header) < len(rows[0]):
                    # Extend header
                    while len(header) < len(rows[0]):
                        header.append("")
                    self._set_header_row(header)
                
                # Write rows
                range_name = f"{next_row}:{next_row + len(rows) - 1}"
                self.sheet.update(range_name, rows, value_input_option="RAW")
                logger.info(f"Wrote {len(rows)} LIVE publisher rows for {today_date} (starting at row {next_row})")
            except Exception as e:
                logger.exception(f"Error writing hourly data: {e}")
                raise

    def finalize_live_data_for_dates(self, dates: List[str]) -> int:
        """
        Change Status from "LIVE" to "FINAL" for all rows with the specified dates.
        
        Args:
            dates: List of date strings in YYYY-MM-DD format
            
        Returns:
            Number of rows finalized
        """
        if not dates:
            return 0
            
        try:
            all_values = self.sheet.get_all_values()
            if len(all_values) <= 1:
                return 0
            
            header = all_values[0]
            
            # Find column indices
            date_col_idx = None
            status_col_idx = None
            
            for idx, col_name in enumerate(header):
                if col_name.lower() == "date":
                    date_col_idx = idx
                elif col_name.lower() == "status":
                    status_col_idx = idx
            
            if date_col_idx is None:
                logger.warning("Date column not found")
                return 0
            
            if status_col_idx is None:
                logger.warning("Status column not found - cannot finalize")
                return 0
            
            dates_set = set(dates)  # For faster lookup
            
            # Find rows to update
            rows_to_update = []
            for row_idx, row in enumerate(all_values[1:], start=2):
                if len(row) <= date_col_idx:
                    continue
                
                row_date = str(row[date_col_idx]).strip()
                if row_date in dates_set:
                    if len(row) > status_col_idx:
                        current_status = str(row[status_col_idx]).strip().upper()
                        if current_status == "LIVE":
                            rows_to_update.append((row_idx, row))
            
            # Update rows
            if rows_to_update:
                for row_idx, row in rows_to_update:
                    # Update just the Status column
                    cell_address = f"{chr(65 + status_col_idx)}{row_idx}"  # Convert to A1 notation
                    self.sheet.update(cell_address, [["FINAL"]])
                
                logger.info(f"Finalized {len(rows_to_update)} LIVE rows for dates: {dates}")
                return len(rows_to_update)
            
            return 0
            
        except Exception as e:
            logger.exception(f"Error finalizing LIVE data: {e}")
            return 0

    def finalize_today_data(self, today_date: str) -> int:
        """
        Change Status from "LIVE" to "FINAL" for all rows with today's date.
        
        Args:
            today_date: Date string in YYYY-MM-DD format
            
        Returns:
            Number of rows finalized
        """
        return self.finalize_live_data_for_dates([today_date])
        try:
            all_values = self.sheet.get_all_values()
            if len(all_values) <= 1:
                return 0
            
            header = all_values[0]
            
            # Find column indices
            date_col_idx = None
            status_col_idx = None
            
            for idx, col_name in enumerate(header):
                if col_name.lower() == "date":
                    date_col_idx = idx
                elif col_name.lower() == "status":
                    status_col_idx = idx
            
            if date_col_idx is None:
                logger.warning("Date column not found")
                return 0
            
            if status_col_idx is None:
                logger.warning("Status column not found - cannot finalize")
                return 0
            
            # Find rows to update
            rows_to_update = []
            for row_idx, row in enumerate(all_values[1:], start=2):
                if len(row) <= date_col_idx:
                    continue
                
                row_date = str(row[date_col_idx]).strip()
                if row_date == today_date:
                    if len(row) > status_col_idx:
                        current_status = str(row[status_col_idx]).strip().upper()
                        if current_status == "LIVE":
                            rows_to_update.append((row_idx, row))
            
            # Update rows
            if rows_to_update:
                updates = []
                for row_idx, row in rows_to_update:
                    # Ensure row has enough columns
                    while len(row) <= status_col_idx:
                        row.append("")
                    row[status_col_idx] = "FINAL"
                    updates.append((row_idx, row))
                
                # Batch update
                for row_idx, row in updates:
                    # Update just the Status column
                    cell_address = f"{chr(65 + status_col_idx)}{row_idx}"  # Convert to A1 notation
                    self.sheet.update(cell_address, [["FINAL"]])
                
                logger.info(f"Finalized {len(updates)} rows for date {today_date}")
                return len(updates)
            
            return 0
            
        except Exception as e:
            logger.exception(f"Error finalizing today's data: {e}")
            return 0

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

        # Ensure Status column exists
        self._ensure_status_column()

        # Define header order: Date, Publisher, Campaign, Payout, Status
        header = ["Date", "Publisher", "Campaign", "Payout", "", "Status"]
        
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

        # Build rows with Status = "FINAL" for historical data
        rows = []
        for pub in publishers:
            row = [
                str(pub.get("Date", "")),
                str(pub.get("Publisher", "")),
                str(pub.get("Campaign", "")),
                str(pub.get("Payout", "")),
                "",  # Empty column (normalized date column)
                "FINAL"  # Status column
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
                # When appending historical data, we need to:
                # 1. Finalize any LIVE data for the dates we're about to write
                # 2. Delete ALL existing rows for those dates (to prevent duplicates)
                # 3. Then write fresh data
                try:
                    all_values = self.sheet.get_all_values()
                    if len(all_values) <= 1:
                        # No existing data, just write
                        range_name = f"2:{len(rows) + 1}"
                        self.sheet.update(range_name, rows, value_input_option="RAW")
                        logger.info(f"Wrote {len(rows)} publisher rows to sheet (no existing data)")
                    else:
                        # Get the dates we're about to write
                        dates_to_process = set()
                        for pub in publishers:
                            pub_date = str(pub.get("Date", "")).strip()
                            if pub_date:
                                dates_to_process.add(pub_date)
                        
                        # Step 1: Finalize any LIVE data for these dates
                        if dates_to_process:
                            finalized_count = self.finalize_live_data_for_dates(list(dates_to_process))
                            if finalized_count > 0:
                                logger.info(f"Finalized {finalized_count} LIVE rows before writing historical data")
                            
                            # Step 2: Delete ALL existing rows for these dates (both LIVE and FINAL)
                            # This ensures we don't have duplicates
                            header = all_values[0]
                            date_col_idx = None
                            
                            for idx, col_name in enumerate(header):
                                if col_name.lower() == "date":
                                    date_col_idx = idx
                                    break
                            
                            if date_col_idx is not None:
                                rows_to_delete = []
                                for row_idx, row in enumerate(all_values[1:], start=2):
                                    if len(row) > date_col_idx:
                                        row_date = str(row[date_col_idx]).strip()
                                        if row_date in dates_to_process:
                                            rows_to_delete.append(row_idx)
                                
                                # Delete rows in reverse order
                                if rows_to_delete:
                                    rows_to_delete.sort(reverse=True)
                                    for row_num in rows_to_delete:
                                        self.sheet.delete_rows(row_num)
                                    logger.info(f"Deleted {len(rows_to_delete)} existing rows for dates {dates_to_process} before writing fresh data")
                        
                        # Step 3: Append the new rows
                        all_values = self.sheet.get_all_values()  # Refresh after deletions
                        next_row = len(all_values) + 1
                        range_name = f"{next_row}:{next_row + len(rows) - 1}"
                        self.sheet.update(range_name, rows, value_input_option="RAW")
                        logger.info(f"Wrote {len(rows)} fresh publisher rows to sheet (starting at row {next_row}, old data removed)")
                        
                except Exception as e:
                    logger.warning(f"Could not append data with deduplication, trying direct append: {e}")
                    # Fallback: use append_row for each row
                    for row in rows:
                        self.sheet.append_row(row, value_input_option="RAW")
                    logger.info(f"Appended {len(rows)} publisher rows to sheet (using append_row fallback)")