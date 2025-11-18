# Ringba API Setup - What We Need to Verify

⚠️ **Important**: The code currently uses assumptions about Ringba's API. We need to verify these details with Ringba's actual API documentation or support.

## Assumptions Made (Need Verification)

### 1. API Endpoint URL
**✅ CONFIRMED by Ringba:**
```
POST https://api.ringba.com/v2/{accountId}/insights
```

**Status:**
- ✅ Base URL: `https://api.ringba.com`
- ✅ Endpoint: `/v2/{accountId}/insights`
- ✅ HTTP Method: `POST`

### 2. Authentication
**✅ CONFIRMED by Ringba:**
- Using API token authentication
- Format: `Authorization: Token {your-token}`
- Header: `Authorization: Token {token}`

### 3. Request Body Format
**Current request body** (based on what Ringba support provided):
```json
{
  "reportStart": "2025-11-18T04:00:00Z",
  "reportEnd": "2025-11-19T03:59:59Z",
  "groupByColumns": [
    {
      "column": "publisherName",
      "displayName": "Publisher"
    }
  ],
  "valueColumns": [
    {
      "column": "payoutAmount",
      "aggregateFunction": null
    }
  ],
  "orderByColumns": [
    {
      "column": "payoutAmount",
      "direction": "desc"
    }
  ],
  "formatTimespans": true,
  "formatPercentages": true,
  "generateRollups": true,
  "maxResultsPerGroup": 1000,
  "filters": [],
  "formatTimeZone": "America/Los_Angeles"
}
```

**What we need to verify:**
- ✅ Is this the correct format?
- ✅ Are all these fields required?
- ✅ Are there any additional required fields?

### 4. Response Format
**Current assumption:**
```json
{
  "groups": [
    {
      "publisherName": "Publisher Name",
      "payoutAmount": 123.45
    }
  ]
}
```

**What we need to know:**
- ✅ What is the actual response structure?
- ✅ How is the data nested?
- ✅ What are the exact field names in the response?

### 5. Account ID
**Current assumption:**
- Using `RINGBA_ACCOUNT_ID` environment variable
- Included in URL path: `/v2/{account_id}/reports`

**What we need to know:**
- ✅ Where do we find the account ID?
- ✅ Is it in the Ringba dashboard?
- ✅ Is it the same as the account name or a separate ID?

## How to Get This Information

### Option 1: Ringba API Documentation
1. Check if Ringba has API documentation:
   - Look for "API Documentation" or "Developer Docs" in Ringba dashboard
   - Check Ringba's help/support section
   - Look for API endpoints in Ringba's settings

### Option 2: Contact Ringba Support
Since Ringba support already provided the request body, ask them:
1. **API Endpoint URL**: What is the exact endpoint URL?
2. **Authentication**: How do we authenticate? Where do we get the API token?
3. **Account ID**: Where do we find the account ID?
4. **Response Format**: What does the response look like?
5. **API Documentation**: Do they have API docs we can reference?

### Option 3: Test with Ringba Support
Ask Ringba support to provide:
- A sample API call (with curl command or Postman collection)
- Example request and response
- Authentication instructions

## Current Code Location

The Ringba API client is in `ringba_client.py`:
- **Line 32**: Base URL (`https://api.ringba.com`)
- **Line 33-36**: Authentication headers
- **Line 96**: Endpoint URL construction
- **Line 66-94**: Request body
- **Line 108-115**: Response parsing

## Testing Steps

Once you have the correct information:

1. **Test the endpoint manually** (using curl or Postman):
   ```bash
   curl -X POST https://api.ringba.com/v2/{account_id}/reports \
     -H "Authorization: Bearer {token}" \
     -H "Content-Type: application/json" \
     -d @ringba_request_body_simplified.json
   ```

2. **Check the response structure** and update the parsing code if needed

3. **Update `ringba_client.py`** with the correct:
   - Endpoint URL
   - Authentication method
   - Response parsing logic

## Questions to Ask Ringba Support

1. ✅ **What is the exact API endpoint URL for generating reports?** → `POST https://api.ringba.com/v2/{accountId}/insights`
2. ✅ **How do we authenticate API requests?** → `Authorization: Token {your-token}`
3. ✅ **Where do we find the account ID?** → `RA092c10a91f7c461098e354a1bbeda598` (provided)
4. ⚠️ **Response format** - What does the API response look like? (Need to verify parsing logic)
5. Can you provide a sample API request/response?
6. Do you have API documentation we can reference?

## Next Steps

1. ✅ **Google Sheets setup is complete** - Spreadsheet ID: `1dCnDcyURNwPbXD1vdPPRGqETApS5DUhDFJOcCpwWouA`
2. ⏳ **Ringba API setup** - Need to verify the assumptions above
3. ⏳ **Test the integration** - Once Ringba API details are confirmed

Let me know what information Ringba provides, and I'll update the code accordingly!

