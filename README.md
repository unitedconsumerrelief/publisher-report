# Ringba Payout Reporting

Automated system to pull publisher payout data from Ringba API and sync it to Google Sheets.

## Features

- **Automatic End-of-Day Reports**: Scheduled job runs daily at 4:05 AM UTC
- **Manual Trigger**: Pull reports anytime via API endpoint
- **Google Sheets Integration**: Automatically writes Publisher and Payout data

## Setup

### 1. Environment Variables

You need to set these environment variables:

#### Ringba API
- `RINGBA_API_TOKEN`: Your Ringba API token
- `RINGBA_ACCOUNT_ID`: Your Ringba account ID (e.g., `RA092c10a91f7c461098e354a1bbeda598`)

#### Google Sheets
- `SPREADSHEET_ID`: Your Google Spreadsheet ID (from the URL)
- `WORKSHEET_NAME`: Worksheet name (default: "Sheet1")
- `GOOGLE_SERVICE_ACCOUNT_JSON`: Full JSON string of your Google Service Account credentials

### 2. Google Service Account Setup

**📖 Detailed Step-by-Step Guide: See [GOOGLE_SHEETS_SETUP.md](GOOGLE_SHEETS_SETUP.md)**

Quick summary:
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing
3. Enable Google Sheets API and Google Drive API
4. Create a Service Account
5. Download the JSON key file
6. **Share your Google Spreadsheet with the service account email** (found in the JSON) - **This is required!**
7. Copy the entire JSON content and paste it as the `GOOGLE_SERVICE_ACCOUNT_JSON` environment variable

### 3. Deployment to Render

#### Option A: Using render.yaml (Recommended)

1. Push your code to GitHub
2. In Render dashboard, go to "New" → "Blueprint"
3. Connect your GitHub repository
4. Render will automatically detect `render.yaml` and configure the service
5. Set the environment variables in Render dashboard

#### Option B: Manual Setup

1. Push your code to GitHub
2. In Render dashboard, go to "New" → "Web Service"
3. Connect your GitHub repository
4. Configure:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn app:app --host 0.0.0.0 --port $PORT`
5. Set all environment variables
6. Deploy

### 4. Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables (create a .env file or export them)
export RINGBA_API_TOKEN="your-api-token"
export RINGBA_ACCOUNT_ID="RA092c10a91f7c461098e354a1bbeda598"
export SPREADSHEET_ID="your-spreadsheet-id"
export GOOGLE_SERVICE_ACCOUNT_JSON='{"type":"service_account",...}'

# Run the app
uvicorn app:app --reload
```

## API Endpoints

### Health Check
```
GET /
```
Returns service status.

### Sync Publisher Payouts (Manual Trigger)
```
GET /sync-publisher-payouts
POST /sync-publisher-payouts
```

**Query Parameters:**
- `report_start` (optional): Start date in ISO format (e.g., `2025-11-18T04:00:00Z`)
- `report_end` (optional): End date in ISO format (e.g., `2025-11-19T03:59:59Z`)
- `clear_existing` (optional): Clear existing data before writing (default: `true`)

**Example:**
```bash
curl https://your-app.onrender.com/sync-publisher-payouts
```

### Webhook Endpoint (Legacy)
```
POST /ringba-webhook
```
Original webhook endpoint (still available if needed).

## Scheduled Reports

The app automatically runs an end-of-day report at **4:05 AM UTC daily**.

To change the schedule, edit `app.py` line 70:
```python
trigger=CronTrigger(hour=4, minute=5, timezone="UTC")
```

## Important Notes

### Render Considerations

1. **Free Tier**: Render free tier services sleep after 15 minutes of inactivity. 
   - ⚠️ **Scheduled jobs may not run reliably** on free tier because the service might be asleep
   - The service will wake up when the job tries to run, but there may be delays
   - **Solution**: Use external cron service (see below) or upgrade to paid tier

2. **Paid Tier**: Recommended for production to ensure:
   - Service stays awake 24/7
   - Scheduled jobs run on time
   - No cold start delays

3. **Time Zone**: The scheduler uses UTC. Adjust the cron trigger in `app.py` line 75 if you need a different timezone.

4. **Alternative: External Cron Service** (Recommended for Free Tier):
   
   If using Render's free tier, disable the built-in scheduler and use an external cron service:
   
   a. Set environment variable: `ENABLE_SCHEDULER=false`
   
   b. Use a free cron service like [cron-job.org](https://cron-job.org):
      - Create account
      - Add new cron job
      - URL: `https://your-app.onrender.com/sync-publisher-payouts`
      - Schedule: Daily at your desired time (e.g., 4:05 AM UTC)
      - Method: GET
   
   This ensures the job runs even if Render service is sleeping (it will wake up when pinged).

## Troubleshooting

### Scheduled job not running
- Check Render logs for errors
- Verify the service is awake (free tier may sleep)
- Check timezone settings

### Google Sheets not updating
- Verify service account has access to the spreadsheet
- Check `GOOGLE_SERVICE_ACCOUNT_JSON` is valid JSON
- Verify `SPREADSHEET_ID` is correct

### Ringba API errors
- Verify `RINGBA_API_TOKEN` and `RINGBA_ACCOUNT_ID` are correct
- Check Ringba API endpoint URL in `ringba_client.py` (line 96)
- Review API response structure - may need to adjust parsing logic

