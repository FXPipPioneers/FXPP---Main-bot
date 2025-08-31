# Live Price Tracking System Setup Guide

## Overview
The Discord Trading Bot now includes a comprehensive live price tracking system that automatically monitors trading signals and provides real-time TP/SL notifications. This system uses multiple API providers for reliable price data and includes advanced features like breakeven logic after TP2 hits.

## Required API Keys

### 1. FXApi (Primary - Recommended) ⭐
- **Website**: https://fxapi.com/
- **Free Tier**: 1,000 requests/month
- **Environment Variable**: `FXAPI_KEY`
- **Coverage**: 170+ currencies, real-time forex data

**Registration Steps:**
1. Go to https://fxapi.com/
2. Click "Get Free API Key" or "Sign Up"
3. Create account with email and password
4. Verify your email address
5. Go to Dashboard → API Keys
6. Copy your API key (starts with "fxapi_live_...")
7. Add to Render environment variables as `FXAPI_KEY`

### 2. Twelve Data (Secondary) 🥈
- **Website**: https://twelvedata.com/
- **Free Tier**: 800 requests/day
- **Environment Variable**: `TWELVE_DATA_KEY`
- **Coverage**: Forex, stocks, crypto, commodities

**Registration Steps:**
1. Go to https://twelvedata.com/
2. Click "Get Free API Key" 
3. Fill out registration form (name, email, company)
4. Verify email and login
5. Navigate to Dashboard → API Keys
6. Copy your API key (32-character string)
7. Add to Render environment variables as `TWELVE_DATA_KEY`

### 3. Alpha Vantage (Backup) 🥉
- **Website**: https://www.alphavantage.co/
- **Free Tier**: 500 requests/day
- **Environment Variable**: `ALPHA_VANTAGE_KEY`
- **Coverage**: Forex, stocks, crypto

**Registration Steps:**
1. Go to https://www.alphavantage.co/support/#api-key
2. Fill out form: Name, Email, Organization
3. Check email for API key (usually instant)
4. API key format: 16-character alphanumeric
5. Add to Render environment variables as `ALPHA_VANTAGE_KEY`

### 4. Financial Modeling Prep (Final Fallback) 🔄
- **Website**: https://financialmodelingprep.com/
- **Free Tier**: 250 requests/day
- **Environment Variable**: `FMP_KEY`
- **Coverage**: Stocks, forex, commodities

**Registration Steps:**
1. Go to https://financialmodelingprep.com/developer/docs
2. Click "Get Free API Key"
3. Create account with email/password
4. Verify email address
5. Go to Dashboard → API Key section
6. Copy your API key (32-character string)
7. Add to Render environment variables as `FMP_KEY`

## Setup Instructions for Render.com

1. **Go to your Render dashboard**
2. **Navigate to your bot service**
3. **Click on "Environment" tab**
4. **Add the following environment variables:**

```
FXAPI_KEY=your_fxapi_key_here
TWELVE_DATA_KEY=your_twelve_data_key_here
ALPHA_VANTAGE_KEY=your_alpha_vantage_key_here
FMP_KEY=your_fmp_key_here
```

5. **Save changes and redeploy**

## How It Works

### Signal Detection
- Monitors all messages from Discord ID `462707111365836801` (owner) and bot messages
- Looks for messages containing "Trade Signal For:" 
- Excludes channel ID `1394958907943817326` (testing channel)
- Automatically parses: pair, action (BUY/SELL), entry, TP1, TP2, TP3, SL

### Price Monitoring
- Background task runs every 30 seconds
- Checks current price against all TP/SL levels
- Uses multiple API fallbacks for reliability
- Handles API failures gracefully

### TP/SL Logic
1. **TP1 Hit**: Sends "@everyone **TP1 HAS BEEN HIT!** 🎯"
2. **TP2 Hit**: Sends notification + "**SL moved to breakeven (entry: X.XXXX)**"
3. **TP3 Hit**: Sends notification + removes trade from tracking
4. **SL Hit**: Sends "@everyone **SL HAS BEEN HIT!** ❌" + removes trade
5. **Breakeven Hit** (after TP2): Sends custom breakeven message + removes trade

### Commands Available

- `/pricetracking true/false` - Enable/disable the system
- `/activetrades` - View all currently tracked signals
- `/pricetest EURUSD` - Test price retrieval for any pair

## Supported Trading Pairs

The system supports all major forex pairs, commodities, and indices that are available through the configured APIs:

### Forex Pairs
- Major: EURUSD, GBPUSD, USDJPY, USDCHF, AUDUSD, USDCAD, NZDUSD
- Minor: EURGBP, EURJPY, GBPJPY, AUDCAD, NZDCAD, etc.
- Exotic: USDTRY, USDZAR, USDMXN, etc.

### Commodities
- XAUUSD (Gold), XAGUSD (Silver)
- USOIL (Crude Oil), UKOIL (Brent Oil)

### Indices
- US100 (Nasdaq), GER40 (DAX), UK100 (FTSE)
- SPX500 (S&P 500), etc.

## Error Handling

The system includes comprehensive error handling:
- API timeout protection (10 seconds per API)
- Automatic fallback between APIs
- Failed trade cleanup
- Detailed logging for troubleshooting
- Graceful degradation if all APIs fail

## Performance Considerations

- **Memory Efficient**: Only stores active trades in memory
- **Rate Limit Aware**: Respects API rate limits with smart fallbacks
- **Background Processing**: Non-blocking price checks
- **Automatic Cleanup**: Removes completed/failed trades

## Security Features

- **Owner Restricted**: All control commands limited to Discord ID 462707111365836801
- **API Key Protection**: Keys stored as environment variables
- **Channel Exclusion**: Prevents tracking test signals
- **Input Validation**: Validates all parsed signal data

## Troubleshooting

### No Price Data
1. Check if at least one API key is configured
2. Verify API keys are valid and have remaining requests
3. Use `/pricetest EURUSD` to test API connectivity

### Signals Not Being Tracked
1. Verify price tracking is enabled with `/pricetracking true`
2. Check if message contains "Trade Signal For:" exactly
3. Ensure message is from owner or bot account
4. Confirm not sent in excluded channel

### Missing Notifications
1. Check bot has permissions to reply to messages
2. Verify @everyone permissions in target channels
3. Ensure active trade appears in `/activetrades`

## API Rate Limits

With all 4 APIs configured, you get:
- **Daily requests**: ~2,800 total
- **Active trades**: Can monitor 50+ simultaneously
- **Check frequency**: Every 30 seconds = 2,880 checks/day
- **Redundancy**: Multiple APIs ensure reliability

## Cost Considerations

All recommended APIs offer generous free tiers sufficient for most trading groups:
- **FXApi**: 1,000 requests/month (free)
- **Twelve Data**: 800 requests/day (free)  
- **Alpha Vantage**: 500 requests/day (free)
- **Financial Modeling Prep**: 250 requests/day (free)

For high-volume usage, paid plans start at $10-50/month per API.