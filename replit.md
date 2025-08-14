# Discord Trading Bot

## Overview

This is a professional Discord bot designed for trading signal distribution with automatic TP/SL calculations and comprehensive statistics tracking. The bot provides a complete solution for forex and commodity trading communities to share and track trading signals across multiple Discord channels.

## System Architecture

### Core Technologies
- **Runtime**: Python 3.11
- **Framework**: Discord.py 2.5.2 (async/await architecture)
- **Environment Management**: python-dotenv for configuration
- **Web Framework**: aiohttp for web server functionality
- **Deployment**: Multi-platform support (Render.com, Heroku, Replit)

### Security Architecture
- **Split Token System**: Discord bot tokens are split into two environment variables for enhanced security
- **Environment-based Configuration**: All sensitive data stored in environment variables
- **Secure Deployment**: No hardcoded credentials in source code

## Key Components

### Bot Commands
1. **`/entry` Command**: Creates and distributes trading signals
   - Entry types: Buy limit, Sell limit, Buy execution, Sell execution
   - Supported pairs: XAUUSD, GBPJPY, USDJPY, GBPUSD, EURUSD, AUDUSD, NZDUSD, US100, US500, custom pairs
   - Automatic TP/SL calculation based on pip values
   - Multi-channel distribution
   - Role tagging functionality
   - Broker warning for BTCUSD, US100, GER40 pairs

2. **`/stats` Command**: Displays trading performance statistics
   - Customizable date ranges
   - TP hit tracking (TP1, TP2, TP3)
   - SL hit tracking
   - Win rate calculations
   - Multi-channel distribution

3. **`/giveaway` Command**: Comprehensive giveaway management system
   - Step-by-step setup: message, role, duration, winners
   - Flexible duration (weeks, days, hours, minutes)
   - Role-based entry requirements
   - **`/giveaway list`**: View all active giveaways with details
   - **`/giveaway choose_winners`**: Smart giveaway selection for guaranteed winners
   - **`/giveaway end`**: Smart giveaway selection for manual ending
   - Auto-guidance when giveaway ID not provided
   - React-to-enter with automatic validation
   - Professional embeds with all giveaway details
   - Always posts to giveaway channel (1405490561963786271)

4. **`/telegram` Command**: Check Telegram integration status
   - Configuration validation
   - Connection status
   - Setup guidance

5. **`/dbstatus` Command**: Database health monitoring
   - Connection status and pool information
   - Table verification
   - Performance metrics

### Trading Logic
- **Pip Calculation Engine**: Instrument-specific pip value calculations
- **Price Formatting**: Decimal precision based on trading pair requirements
- **Automatic TP/SL Generation**: 
  - TP1: 20 pips from entry
  - TP2: 50 pips from entry  
  - TP3: 100 pips from entry
  - SL: 70 pips from entry (opposite direction)

### Message Distribution
- **Multi-channel Broadcasting**: Send signals to multiple channels simultaneously
- **Role Tagging System**: Configurable role mentions at message bottom
- **Immediate Delivery**: Real-time signal distribution without delays

### Telegram Integration
- **Automatic Signal Forwarding**: Monitors Telegram groups for trading signals
- **Intelligent Signal Parsing**: Recognizes trading pairs, entry types, and prices
- **Instant Relay**: Forwards signals to Discord without delays
- **Broker Warnings**: Adds specific warnings for volatile pairs (BTCUSD, US100, GER40)
- **Configurable Monitoring**: Can monitor specific chat IDs or all accessible chats

### Timed Auto-Role System
- **Automatic Role Assignment**: Assigns specified roles to new members upon joining
- **Configurable Duration**: Set custom expiration time (default 24 hours)
- **Automatic Role Removal**: Removes expired roles via background monitoring task
- **Custom DM Notifications**: Sends personalized messages when roles expire
- **Persistent Storage**: Maintains member tracking across bot restarts
- **Admin Controls**: `/timedautorole` command for enable/disable/status management

## Data Flow

1. **Command Input**: User executes `/entry` or `/stats` slash command
2. **Parameter Validation**: Bot validates trading pair, price format, and channels
3. **Calculation Engine**: Automatic TP/SL calculation based on pair configuration
4. **Message Generation**: Professional formatting with proper decimal places
5. **Multi-channel Distribution**: Simultaneous broadcasting to selected channels
6. **Role Notification**: Tag specified roles at message bottom

### Trading Pair Configuration
```python
PAIR_CONFIG = {
    'XAUUSD': {'decimals': 2, 'pip_value': 0.1},
    'GBPJPY': {'decimals': 3, 'pip_value': 0.01},
    'USDJPY': {'decimals': 3, 'pip_value': 0.01},
    'GBPUSD': {'decimals': 4, 'pip_value': 0.0001},
    'EURUSD': {'decimals': 4, 'pip_value': 0.0001},
    'AUDUSD': {'decimals': 5, 'pip_value': 0.0001},
    'NZDUSD': {'decimals': 5, 'pip_value': 0.0001},
    'US100': {'decimals': 0, 'pip_value': 1.0},
    'US500': {'decimals': 2, 'pip_value': 0.1}
}
```

## External Dependencies

### Required Packages
- `discord.py==2.5.2`: Discord API interaction
- `python-dotenv==1.1.0`: Environment variable management
- `aiohttp==3.12.13`: Web server functionality
- `asyncpg==0.30.0`: PostgreSQL database connectivity
- `pyrogram==2.0.106`: Telegram integration
- `tgcrypto==1.2.5`: Telegram encryption support
- `pytz>=2025.2`: Timezone handling with DST support
- `requests>=2.32.4`: HTTP requests functionality

### Discord API Requirements
- Bot token with appropriate permissions:
  - Send Messages
  - Use Slash Commands
  - Mention Everyone (for role tagging)
  - Read Message History

### Environment Variables
- `DISCORD_TOKEN_PART1`: First half of Discord bot token
- `DISCORD_TOKEN_PART2`: Second half of Discord bot token  
- `DISCORD_CLIENT_ID_PART1`: First half of Discord client ID
- `DISCORD_CLIENT_ID_PART2`: Second half of Discord client ID

### Discord Bot Permissions
For the auto-role system to work, the bot needs:
- **Manage Roles**: To assign and remove roles from members
- **Send Messages**: To send notifications and confirmations
- **Use Slash Commands**: For the `/timedautorole` command
- **View Members**: To detect new member joins

### Telegram Integration Variables
- `TELEGRAM_API_ID`: Telegram API ID from my.telegram.org
- `TELEGRAM_API_HASH`: Telegram API hash from my.telegram.org
- `TELEGRAM_PHONE_NUMBER`: Phone number associated with Telegram account
- `TELEGRAM_SOURCE_CHAT_ID`: Chat ID of the source trading group (optional)
- `TELEGRAM_DEFAULT_CHANNELS`: Default Discord channels for forwarding (comma-separated)
- `TELEGRAM_DEFAULT_ROLES`: Default roles to mention (comma-separated)

## Deployment Strategy

### Multi-platform Support
1. **Render.com**: Web service deployment with automatic scaling
2. **Heroku**: Worker dyno configuration via Procfile
3. **Replit**: Development and testing environment

### Deployment Configuration
- **Build Command**: `pip install --upgrade pip && pip install discord.py==2.5.2 python-dotenv==1.1.0 aiohttp==3.12.13 asyncpg==0.30.0 requests pyrogram==2.0.106 tgcrypto==1.2.5`
- **Start Command**: `python main.py`
- **Runtime**: Python 3.11.0
- **Service Type**: Web service (keeps bot alive 24/7)

### Security Considerations
- Split token system prevents complete token exposure
- Environment-based configuration
- No sensitive data in version control
- Secure deployment practices across platforms

## User Preferences

Preferred communication style: Simple, everyday language.

## Recent Changes (August 14, 2025)

✓ Fixed critical bot offline issues - added proper aiohttp client session cleanup to prevent unclosed session errors
✓ Added comprehensive offline member recovery system - automatically catches up on members who joined while bot was offline
✓ Implemented complete giveaway system with all requested features:
  - Step-by-step giveaway creation (`/giveaway message`, `role`, `duration`, `winners`)
  - Role-based entry requirements with automatic validation
  - Flexible duration settings (weeks, days, hours, minutes)
  - Guaranteed winner selection with `choose_winners` action
  - Automatic giveaway ending with winner selection
  - React-to-enter system with 🎉 emoji
  - Automatic reaction removal for users without required roles
  - Professional giveaway embeds with all details
  - Always posts to designated giveaway channel (ID: 1405490561963786271)
  - @everyone notification when giveaways are posted
✓ Enhanced anti-abuse display filtering - role listing commands now only show users with active roles
✓ Added comprehensive database monitoring with `/dbstatus` command
✓ Fixed all indentation and syntax errors in giveaway system
✓ Added automatic role validation and DM notifications for giveaway entries
✓ Implemented proper giveaway scheduling and automatic cleanup
✓ Enhanced giveaway management with smart active giveaway selection
✓ Added `/giveaway list` command to view all active giveaways
✓ Auto-guidance for choose_winners and end actions when giveaway ID not provided
✓ Prevention of exceeding winner limits with guaranteed winners

## Previous Changes (August 1, 2025)

✓ Fixed command sync reliability issues for 24/7 responsiveness
✓ Updated weekend message with new welcome text
✓ Fixed weekend timer to expire at Monday 23:59 instead of Tuesday 01:00
✓ Implemented proper Amsterdam timezone handling with DST support using pytz
✓ Simplified weekend activation system with direct expiry times
✓ Added retry mechanisms for command syncing and bot startup
✓ Enhanced error handling and logging throughout the bot
✓ Added web server component for deployment stability
✓ Installed pytz dependency for accurate timezone calculations
✓ Fixed "ERROR" display issue in /timedautorole list command - now shows proper countdown times
✓ Added "adduser" feature to /timedautorole command for manual member addition
✓ Added "removeuser" feature to /timedautorole command for manual member removal
✓ Added timing options: "24hours", "weekend", or "custom" with hours/minutes fields
✓ Custom timing allows precise duration control (e.g., 2h 30m, 72h, 45m)
✓ Enhanced timezone compatibility with fallback system for Render deployment
✓ Improved weekend member time calculation with proper expiry_time handling
✓ Added safety validation for custom timing (max 168 hours, proper range checks)
✓ Enhanced background monitoring for instant role management (30-second checks)
✓ Fixed manual adduser role assignment and automatic expiration system
✓ Improved expired member filtering - no longer shows expired users in lists
✓ Implemented comprehensive memory system with persistent data storage across bot restarts
✓ Added anti-abuse system - members can only receive auto-role once per Discord account (permanent)
✓ Added follow-up DM campaign system (3, 7, 14 days after role expiration)
✓ Enhanced Discord logging system - all bot activities logged to designated channel
✓ Bot invite detection and exclusion - ignores members joining via bot-created invites
✓ Gold Pioneer role detection - skips follow-up DMs for members who already purchased

### Previous Changes (January 31, 2025)
✓ Replaced main.py with updated version including all requested features
✓ Added special broker warning note for US100 & GER40 trading signals
✓ Implemented precise timer display (hours, minutes, seconds) for /timedautorole command
✓ Fixed auto-role duration to 24 hours (no longer adjustable)
✓ Added comprehensive weekend handling logic with Amsterdam timezone
✓ Weekend period: Friday 12:00 to Sunday 23:59 (Amsterdam time)
✓ Weekend joiners get delayed activation until Monday 00:01
✓ Implemented three different DM messages:
  - Weekend notification for weekend joiners
  - Activation notification when 24h timer starts on Monday
  - Expiration message when role is removed
✓ Added background tasks for weekend activation monitoring

## Recent Changes

- August 8, 2025: Major project cleanup and dependency fixes:
  - Removed all duplicate directories (discord-bot-updated, discord-trading-bot-download, attached_assets)
  - Removed duplicate documentation files and build scripts
  - Fixed missing asyncpg dependency causing Render deployment failures
  - Streamlined project to only essential files (main.py, pyproject.toml, DEPLOYMENT_INSTRUCTIONS.md, README.md, replit.md)
  - Updated all deployment configurations to include asyncpg==0.30.0
  - Clean, minimal project structure for better maintainability

- July 29, 2025: Enhanced with comprehensive Telegram monitoring and tracking system:
  - Added /tracking command for detailed signal forwarding monitoring
  - Implemented comprehensive logging system for all Telegram activities
  - Added real-time activity tracking with statistics and success rates
  - Enhanced error tracking and debugging capabilities
  - Improved signal parsing with detailed logging and validation
  - Added recent signals history and activity logs
  - Fixed Render deployment issues with explicit package installation
  - Combined Telegram forwarding, auto-role system, and monitoring in unified bot
- January 10, 2025: Added Telegram integration for automatic signal forwarding
- January 10, 2025: Added broker warning messages for BTCUSD, US100, and GER40 pairs
- January 10, 2025: Implemented intelligent signal parsing from Telegram messages
- January 10, 2025: Added /telegram command for integration status checking
- July 10, 2025: Cleaned up codebase by removing all failed trading integration attempts (MetaAPI, OANDA, MT5)
- July 10, 2025: Simplified bot to focus on core signal distribution functionality
- July 10, 2025: Updated deployment files to include Telegram dependencies

## Changelog

Changelog:
- June 24, 2025. Initial setup