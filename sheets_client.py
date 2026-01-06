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
            publishers: List of dicts with "Publisher", "Campaign", "Payout", "Completed Calls", "Paid Calls", and "Date" keys
            clear_existing: If True, clear existing data before writing (default: True)
        """
        if not publishers:
            logger.warning("No publisher data to write")
            return

        # Define header order: Date, Publisher, Campaign, Payout, Completed Calls, Paid Calls, Status
        header = ["Date", "Publisher", "Campaign", "Payout", "Completed Calls", "Paid Calls", "Status"]
        
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
                str(pub.get("Payout", "")),
                str(pub.get("Completed Calls", "0")),
                str(pub.get("Paid Calls", "0")),
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

    def write_hourly_publisher_payouts(self, publishers: List[Dict[str, Any]], hour_identifier: str) -> None:
        """
        Write publisher payout data to the sheet for a specific hour.
        Clears existing data for that hour and writes fresh data.
        
        Args:
            publishers: List of dicts with "Publisher", "Campaign", "Payout", "Completed Calls", "Paid Calls", "Date", and "Status" keys
            hour_identifier: String identifier for the hour (e.g., "2026-01-02 14:00") used to identify which rows to clear
        """
        if not publishers:
            logger.warning("No publisher data to write for hourly report")
            return

        # Define header order: Date, Publisher, Campaign, Payout, Completed Calls, Paid Calls, Status, Hour
        header = ["Date", "Publisher", "Campaign", "Payout", "Completed Calls", "Paid Calls", "Status", "Hour"]
        
        # Set header row
        self._set_header_row(header)
        
        # Get all existing data to find rows matching this hour
        # Clear ALL rows for this specific hour identifier to prevent duplicates
        try:
            all_values = self.sheet.get_all_values()
            if len(all_values) > 1:  # More than just header
                # Find the Hour column index (should be last column, index 7)
                hour_col_index = 7  # 0-based index for column H (8th column)
                
                # Find ALL rows that match this hour identifier (including partial matches)
                rows_to_delete = []
                for i in range(1, len(all_values)):  # Skip header row
                    row = all_values[i]
                    # Check if row has hour column and matches the hour identifier
                    if len(row) > hour_col_index:
                        row_hour = str(row[hour_col_index]).strip()
                        if row_hour == hour_identifier or row_hour.startswith(hour_identifier.split()[0] + " " + hour_identifier.split()[1]):
                            rows_to_delete.append(i + 1)  # +1 because sheet rows are 1-indexed
                
                # Delete matching rows (delete from bottom to top to preserve indices)
                if rows_to_delete:
                    rows_to_delete.sort(reverse=True)
                    for row_num in rows_to_delete:
                        try:
                            self.sheet.delete_rows(row_num)
                        except Exception as e:
                            logger.warning(f"Could not delete row {row_num}: {e}")
                    logger.info(f"Cleared {len(rows_to_delete)} existing rows for hour {hour_identifier}")
                else:
                    logger.info(f"No existing rows found for hour {hour_identifier}")
        except Exception as e:
            logger.warning(f"Could not clear existing hourly data: {e}")

        # Build rows with hour identifier
        rows = []
        for pub in publishers:
            row = [
                str(pub.get("Date", "")),
                str(pub.get("Publisher", "")),
                str(pub.get("Campaign", "")),
                str(pub.get("Payout", "")),
                str(pub.get("Completed Calls", "0")),
                str(pub.get("Paid Calls", "0")),
                str(pub.get("Status", "LIVE")),  # Status column (LIVE or FINAL)
                hour_identifier  # Hour identifier for tracking
            ]
            rows.append(row)

        # Append new rows
        if rows:
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
                logger.info(f"Wrote {len(rows)} hourly publisher rows to sheet for hour {hour_identifier}")
            except Exception as e:
                logger.warning(f"Could not append hourly data, trying direct append: {e}")
                # Fallback: use append_row for each row
                for row in rows:
                    self.sheet.append_row(row, value_input_option="RAW")
                    logger.info(f"Appended {len(rows)} hourly publisher rows to sheet (using append_row)")

    def get_cumulative_publishers(
        self, 
        new_hour_publishers: List[Dict[str, Any]], 
        current_hour_identifier: str,
        date_str: str,
        current_hour_num: int
    ) -> List[Dict[str, Any]]:
        """
        Calculate cumulative totals by summing data from 9am to current hour.
        
        Args:
            new_hour_publishers: List of publishers for the current hour
            current_hour_identifier: Hour identifier for current hour (e.g., "2026-01-06 12:00")
            date_str: Date string (e.g., "2026-01-06")
            current_hour_num: Current hour number (0-23)
        
        Returns:
            List of publishers with cumulative totals from 9am to current hour
        """
        # Dictionary to store cumulative totals by (Publisher, Campaign)
        cumulative_dict = {}
        
        # Get all existing data from previous hours (9am to previous hour)
        try:
            all_values = self.sheet.get_all_values()
            if len(all_values) > 1:  # More than just header
                hour_col_index = 7  # Hour column index
                
                # Process existing rows from 9am to previous hour
                for i in range(1, len(all_values)):  # Skip header row
                    row = all_values[i]
                    if len(row) > hour_col_index:
                        row_hour_identifier = row[hour_col_index]
                        # Extract hour number from identifier (format: "YYYY-MM-DD HH:00")
                        try:
                            row_hour_str = row_hour_identifier.split(" ")[1] if " " in row_hour_identifier else ""
                            row_hour_num = int(row_hour_str.split(":")[0]) if ":" in row_hour_str else -1
                            
                            # Only include hours from 9am (9) to previous hour (current_hour_num - 1)
                            if 9 <= row_hour_num < current_hour_num:
                                publisher = row[1] if len(row) > 1 else ""
                                campaign = row[2] if len(row) > 2 else ""
                                
                                if publisher:  # Skip empty rows
                                    key = (publisher, campaign)
                                    
                                    # Parse values
                                    try:
                                        payout = float(row[3]) if len(row) > 3 and row[3] else 0.0
                                        completed_calls = int(float(row[4])) if len(row) > 4 and row[4] else 0
                                        paid_calls = int(float(row[5])) if len(row) > 5 and row[5] else 0
                                    except (ValueError, TypeError):
                                        payout = 0.0
                                        completed_calls = 0
                                        paid_calls = 0
                                    
                                    # Add to cumulative totals
                                    if key in cumulative_dict:
                                        cumulative_dict[key]["Payout"] += payout
                                        cumulative_dict[key]["Completed Calls"] += completed_calls
                                        cumulative_dict[key]["Paid Calls"] += paid_calls
                                    else:
                                        cumulative_dict[key] = {
                                            "Publisher": publisher,
                                            "Campaign": campaign,
                                            "Payout": payout,
                                            "Completed Calls": completed_calls,
                                            "Paid Calls": paid_calls
                                        }
                        except (ValueError, IndexError):
                            continue  # Skip rows with invalid hour format
        except Exception as e:
            logger.warning(f"Could not read existing data for cumulative calculation: {e}")
        
        # Add current hour's data to cumulative totals
        for pub in new_hour_publishers:
            publisher = pub.get("Publisher", "")
            campaign = pub.get("Campaign", "")
            key = (publisher, campaign)
            
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
                    "Payout": payout,
                    "Completed Calls": completed_calls,
                    "Paid Calls": paid_calls
                }
        
        # Convert to list and add Date and Status from first publisher (they should all be the same)
        cumulative_list = []
        status = new_hour_publishers[0].get("Status", "LIVE") if new_hour_publishers else "LIVE"
        
        for key, pub_data in cumulative_dict.items():
            pub_data["Date"] = date_str
            pub_data["Status"] = status
            cumulative_list.append(pub_data)
        
        logger.info(f"Calculated cumulative totals: {len(cumulative_list)} unique publishers from 9am to hour {current_hour_num}")
        return cumulative_list