# Hybrid VALD Performance System Setup Guide

## Quick Fix for Current Error

The "Invalid URL" error occurs because your VALD API credentials are not configured. Here's how to fix it:

### 1. Create Environment File

Copy the example file and add your credentials:

```bash
cp .env.example .env
```

### 2. Edit .env File

Open `.env` and replace the placeholder values with your actual VALD API credentials:

```env
# VALD ForceDecks API Configuration
FORCEDECKS_URL=https://your-actual-vald-instance.forcedecks.com/api
DYNAMO_URL=https://your-actual-vald-instance.forcedecks.com/dynamo
PROFILE_URL=https://your-actual-vald-instance.forcedecks.com/profiles
AUTH_URL=https://your-actual-vald-instance.forcedecks.com/oauth/token

# VALD API Authentication
CLIENT_ID=your_actual_client_id
CLIENT_SECRET=your_actual_client_secret
TENANT_ID=your_actual_tenant_id

# Server Configuration
PORT=4000
NODE_ENV=development
```

### 3. Restart Backend

After creating the `.env` file:

```bash
cd backend-api
npm run dev
```

## System Behavior

### With VALD API Configured ✅
- 🔴 **Live Data**: Real-time athlete search and test results
- ✅ **Analytics**: Database percentiles and benchmarks
- 📊 **Hybrid Reports**: Live data + analytics

### Without VALD API Configured ⚠️
- 💾 **Cached Data Only**: Falls back to BigQuery data
- ⚠️ **Warning Messages**: Clear indicators of configuration status
- 🔧 **Still Functional**: System works with existing data

## Complete Setup Instructions

### Backend Setup
```bash
cd backend-api
npm install
# Create .env file (see above)
npm run dev
```

### Frontend Setup
```bash
cd performance_data_app
npm install
npm start
```

### Verify Setup
1. Backend should show: `✅ VALD API token refreshed successfully`
2. Frontend should show live data indicators: 🔴 Live Data
3. If no VALD config: `⚠️ VALD API not configured - system will use cached data only`

## Troubleshooting

### Common Issues

1. **"Invalid URL" Error**
   - Missing or incorrect `.env` file
   - Solution: Create `.env` with valid VALD URLs

2. **"Authentication failed" Error**
   - Incorrect CLIENT_ID/CLIENT_SECRET
   - Solution: Verify credentials with VALD team

3. **"VALD API not configured" Warning**
   - Missing environment variables
   - Solution: System works in cached-only mode

### Environment Variables Required

| Variable | Description | Example |
|----------|-------------|---------|
| `FORCEDECKS_URL` | Main VALD API URL | `https://instance.forcedecks.com/api` |
| `AUTH_URL` | OAuth token endpoint | `https://instance.forcedecks.com/oauth/token` |
| `CLIENT_ID` | VALD API client ID | `your_client_id` |
| `CLIENT_SECRET` | VALD API client secret | `your_client_secret` |
| `TENANT_ID` | VALD tenant identifier | `your_tenant_id` |

## System Architecture

```
VALD API (Live) ←→ Node.js Hybrid API ←→ React Frontend
     ↓                    ↓
BigQuery Analytics    Visual Indicators
```

## Next Steps

1. **Get VALD Credentials**: Contact your VALD administrator for API access
2. **Test Hybrid System**: Try athlete search with live data
3. **Generate Reports**: Test PDF generation with live + analytics data

The system is designed to work gracefully whether VALD API is available or not!