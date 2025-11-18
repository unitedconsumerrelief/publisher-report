# Google Sheets Setup Guide

This guide walks you through setting up Google Sheets integration for the Ringba Payout Reporter.

## Step-by-Step Setup

### Step 1: Create a Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Click the project dropdown at the top
3. Click **"New Project"**
4. Enter a project name (e.g., "Ringba Payout Reporter")
5. Click **"Create"**
6. Wait for the project to be created, then select it

### Step 2: Enable Required APIs

1. In the Google Cloud Console, go to **"APIs & Services"** → **"Library"**
2. Search for **"Google Sheets API"** and click it
3. Click **"Enable"**
4. Go back to the library
5. Search for **"Google Drive API"** and click it
6. Click **"Enable"**

### Step 3: Create a Service Account

1. In Google Cloud Console, go to **"APIs & Services"** → **"Credentials"**
2. Click **"+ CREATE CREDENTIALS"** at the top
3. Select **"Service account"**
4. Fill in the details:
   - **Service account name**: `ringba-payout-reporter` (or any name you prefer)
   - **Service account ID**: Will auto-fill (you can change it)
   - **Description**: `Service account for Ringba payout reporting`
5. Click **"Create and Continue"**
6. Skip the optional steps (click **"Continue"** and then **"Done"**)

### Step 4: Create and Download JSON Key

⚠️ **If you see "Service account key creation is disabled" error**, see [Alternative Authentication Methods](#alternative-authentication-methods) below.

1. In the **"Credentials"** page, find your service account in the list
2. Click on the service account email (it will look like: `ringba-payout-reporter@your-project.iam.gserviceaccount.com`)
3. Go to the **"Keys"** tab
4. Click **"Add Key"** → **"Create new key"**
5. Select **"JSON"** format
6. Click **"Create"**
7. A JSON file will download automatically - **SAVE THIS FILE SECURELY**
   - ⚠️ **Important**: This file contains sensitive credentials. Don't commit it to Git!

**If you get an error about organization policy blocking key creation**, you have these options:
- **Option A**: Use OAuth 2.0 instead (see Alternative Methods below) - **Recommended for external services**
- **Option B**: Request your organization admin to allow key creation
- **Option C**: Use a personal Google account (not under organization policy)

### Step 5: Get the Service Account Email

1. The service account email is visible in:
   - The JSON file you downloaded (field: `"client_email"`)
   - The service account details page
2. It looks like: `ringba-payout-reporter@your-project-id.iam.gserviceaccount.com`
3. **Copy this email** - you'll need it in the next step

### Step 6: Share Your Google Spreadsheet

1. Open your Google Spreadsheet (or create a new one)
2. Click the **"Share"** button (top right)
3. In the "Add people and groups" field, paste the **service account email** from Step 5
4. Set permission to **"Editor"** (or at minimum "Viewer" if you only read)
   - For this app, **"Editor"** is required since we write data
5. **Uncheck** "Notify people" (the service account doesn't have an email inbox)
6. Click **"Share"**

### Step 7: Get Your Spreadsheet ID

1. Open your Google Spreadsheet
2. Look at the URL in your browser:
   ```
   https://docs.google.com/spreadsheets/d/SPREADSHEET_ID_HERE/edit#gid=0
   ```
3. Copy the part between `/d/` and `/edit` - that's your **Spreadsheet ID**
   - Example: If URL is `https://docs.google.com/spreadsheets/d/1abc123xyz456/edit`
   - Then Spreadsheet ID is: `1abc123xyz456`

### Step 8: Prepare the JSON for Environment Variable

1. Open the JSON file you downloaded in Step 4
2. Copy the **entire contents** of the file
3. It should look something like:
   ```json
   {
     "type": "service_account",
     "project_id": "your-project-id",
     "private_key_id": "...",
     "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
     "client_email": "ringba-payout-reporter@your-project.iam.gserviceaccount.com",
     "client_id": "...",
     "auth_uri": "https://accounts.google.com/o/oauth2/auth",
     "token_uri": "https://oauth2.googleapis.com/token",
     "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
     "client_x509_cert_url": "..."
   }
   ```
4. You'll paste this entire JSON as a **single-line string** in the environment variable

### Step 9: Set Environment Variables

Set these in your deployment platform (Render, local, etc.):

#### Required Variables:

1. **`SPREADSHEET_ID`**
   - Value: The Spreadsheet ID from Step 7
   - Example: `1abc123xyz456`

2. **`WORKSHEET_NAME`** (optional, defaults to "Sheet1")
   - Value: The name of the worksheet/tab in your spreadsheet
   - Example: `Sheet1` or `Payouts` or `Publisher Data`

3. **`GOOGLE_SERVICE_ACCOUNT_JSON`**
   - Value: The **entire JSON content** from Step 8 as a single-line string
   - In Render: Paste the entire JSON (it will handle it correctly)
   - For command line: You may need to escape quotes or use single quotes

#### Example for Render:

In Render dashboard, when setting `GOOGLE_SERVICE_ACCOUNT_JSON`:
- Just paste the entire JSON content directly
- Render will handle the formatting

#### Example for Local Development (.env file):

```env
SPREADSHEET_ID=1abc123xyz456
WORKSHEET_NAME=Sheet1
GOOGLE_SERVICE_ACCOUNT_JSON={"type":"service_account","project_id":"your-project",...}
```

⚠️ **Note**: In a `.env` file, you might need to put the JSON in single quotes:
```env
GOOGLE_SERVICE_ACCOUNT_JSON='{"type":"service_account",...}'
```

## Verification

### Test the Setup Locally:

1. Set your environment variables
2. Run the app:
   ```bash
   uvicorn app:app --reload
   ```
3. Test the endpoint:
   ```bash
   curl http://localhost:8000/sync-publisher-payouts
   ```
4. Check your Google Spreadsheet - you should see:
   - Row 1: Headers (`Publisher`, `Payout`)
   - Row 2+: Publisher data

### Common Issues:

1. **"Permission denied" error**:
   - Make sure you shared the spreadsheet with the service account email
   - Check that the permission is set to "Editor" (not just "Viewer")

2. **"Spreadsheet not found" error**:
   - Verify the `SPREADSHEET_ID` is correct
   - Make sure the spreadsheet is shared with the service account

3. **"Invalid JSON" error**:
   - Make sure the entire JSON is copied (including all brackets and quotes)
   - Check for any extra characters or line breaks

4. **"API not enabled" error**:
   - Go back to Step 2 and make sure both APIs are enabled
   - Wait a few minutes for the APIs to fully activate

## Security Best Practices

1. ✅ **Never commit the JSON key file to Git** (it's in `.gitignore`)
2. ✅ **Only share the spreadsheet with the specific service account email**
3. ✅ **Use environment variables** - never hardcode credentials
4. ✅ **Rotate keys periodically** if you suspect any compromise
5. ✅ **Limit service account permissions** - only give Editor access to the specific spreadsheet

## Quick Checklist

- [ ] Google Cloud Project created
- [ ] Google Sheets API enabled
- [ ] Google Drive API enabled
- [ ] Service Account created
- [ ] JSON key downloaded
- [ ] Service account email copied
- [ ] Spreadsheet shared with service account email (Editor permission)
- [ ] Spreadsheet ID copied
- [ ] Environment variables set:
  - [ ] `SPREADSHEET_ID`
  - [ ] `WORKSHEET_NAME` (optional)
  - [ ] `GOOGLE_SERVICE_ACCOUNT_JSON`
- [ ] Tested the connection

## Alternative Authentication Methods

If your organization blocks service account key creation, use one of these alternatives:

### Option A: OAuth 2.0 (Recommended for External Services)

This method uses OAuth 2.0 with refresh tokens, which works well for services like Render.

#### Setup Steps:

1. **Create OAuth 2.0 Credentials**:
   - In Google Cloud Console, go to **"APIs & Services"** → **"Credentials"**
   - Click **"+ CREATE CREDENTIALS"** → **"OAuth client ID"**
   - If prompted, configure the OAuth consent screen first:
     - User Type: **External** (unless you have Google Workspace)
     - App name: `Ringba Payout Reporter`
     - User support email: Your email
     - Developer contact: Your email
     - Click **"Save and Continue"**
     - Add scopes: `https://www.googleapis.com/auth/spreadsheets` and `https://www.googleapis.com/auth/drive`
     - Click **"Save and Continue"**
     - Add test users (your email) if needed
     - Click **"Back to Dashboard"**

2. **Create OAuth Client**:
   - Application type: **"Web application"**
   - Name: `Ringba Payout Reporter`
   - Authorized redirect URIs: `http://localhost:8080` (for testing) or your Render URL
   - Click **"Create"**
   - **Copy the Client ID and Client Secret** - you'll need these

3. **Get Refresh Token** (One-time setup):
   
   You'll need to run a one-time script to get a refresh token. Create a file `get_oauth_token.py`:

   ```python
   from google_auth_oauthlib.flow import InstalledAppFlow
   import json
   
   SCOPES = [
       'https://www.googleapis.com/auth/spreadsheets',
       'https://www.googleapis.com/auth/drive'
   ]
   
   flow = InstalledAppFlow.from_client_secrets_file(
       'client_secrets.json', SCOPES)
   creds = flow.run_local_server(port=8080)
   
   # Save credentials
   with open('token.json', 'w') as token:
       token.write(creds.to_json())
   
   print("Refresh token saved to token.json")
   print(f"Refresh Token: {creds.refresh_token}")
   ```

   - Download your OAuth client credentials as `client_secrets.json`
   - Run: `python get_oauth_token.py`
   - Authorize in the browser
   - Copy the refresh token from the output

4. **Update Environment Variables**:
   - `GOOGLE_CLIENT_ID`: Your OAuth Client ID
   - `GOOGLE_CLIENT_SECRET`: Your OAuth Client Secret  
   - `GOOGLE_REFRESH_TOKEN`: The refresh token from step 3
   - Remove `GOOGLE_SERVICE_ACCOUNT_JSON` (not needed)

5. **Update Code** (I'll need to modify `sheets_client.py` to support OAuth):
   - The code needs to be updated to use OAuth instead of service account
   - Let me know if you want to use this method and I'll update the code

### Option B: Request Policy Exception

If you have access to an organization administrator:

1. Contact your **Organization Policy Administrator**
2. Request they disable the `iam.disableServiceAccountKeyCreation` policy
3. Or request an exception for your specific project

### Option C: Use Personal Google Account

1. Create a new Google Cloud project using a **personal Google account** (not organization account)
2. Follow the original service account setup steps
3. This account will be separate from your organization

### Option D: Workload Identity Federation (Advanced)

For GCP-native deployments, you can use Workload Identity Federation, but this is more complex and typically used within Google Cloud Platform.

---

## Need Help?

If you encounter issues:
1. Check the logs in your application
2. Verify all environment variables are set correctly
3. Double-check the spreadsheet is shared with the correct email
4. Ensure both APIs are enabled in Google Cloud Console
5. **If blocked by organization policy**: Use one of the alternative methods above

