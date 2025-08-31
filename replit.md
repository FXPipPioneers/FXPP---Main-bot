# Discord Trading Bot

## Overview
This project is a professional Discord bot designed for trading signal distribution, automatic Take Profit (TP) and Stop Loss (SL) calculations, and comprehensive statistics tracking. Its main purpose is to provide a complete solution for forex and commodity trading communities to efficiently share and track trading signals across multiple Discord channels, enhancing communication and data management for trading insights. The project aims to streamline signal dissemination and performance analysis for trading groups.

## User Preferences
- Preferred communication style: Simple, everyday language.
- Bot owner restricted to Discord ID: 462707111365836801
- TP/SL calculations updated: TP1=20 pips, TP2=40 pips, TP3=70 pips, SL=50 pips (changed from TP2=50, TP3=100, SL=70)

## Recent Changes (August 30, 2025)
- **CRITICAL TP/SL TRACKING FIX**: Implemented live API price-based TP/SL tracking with calculate_live_tracking_levels method for accurate monitoring while showing Discord entry prices on frontend
- **REMOVED TELEGRAM INTEGRATION**: Completely removed all Telegram features and `/stats` command as requested, cleaned up unused imports and dependencies
- **ENHANCED DM STATUS SYSTEM**: dmstatus command now automatically removes users who completed 14-day DM sequence for improved data management
- **INVITE BACKTRACKING**: Added comprehensive system to track pre-existing server invites that existed before invite tracking was enabled
- **MISSED SIGNAL RECOVERY**: Implemented automatic signal recovery system for when bot goes down, with live price alignment for accurate tracking restart
- **ENHANCED ANTI-ABUSE SYSTEM**: Added sophisticated abuse prevention using account age verification (7+ days) and rapid join pattern detection (max 5 joins/hour)
- **LEADERBOARD FUNCTIONALITY**: Added comprehensive leaderboard to `/level` command showing top community members ranked by level and message count
- **NEW MANAGEMENT COMMANDS**: Added `/antiabuse` command for complete anti-abuse system management (view, block, unblock, stats)
- **ENHANCED API DOCUMENTATION**: Provided detailed step-by-step registration instructions for all 4 price tracking APIs with comprehensive setup guides

## 🚀 PRODUCTION DEPLOYMENT - RENDER.COM

**CRITICAL: This bot runs 24/7 on Render.com hosting platform**

### Render Configuration
- **Platform**: Render.com Web Service (Python runtime)
- **Database**: Render managed PostgreSQL database instance
- **Region**: Oregon (configured in render.yaml)
- **Runtime**: Python 3.11.0
- **Plan**: Free tier with automatic scaling
- **Health Monitoring**: /health endpoint for continuous uptime monitoring
- **Auto-deploy**: Disabled (manual deployments only)

### Database Setup
- **Provider**: Render PostgreSQL (managed database)
- **Connection**: Via DATABASE_URL environment variable
- **Backup**: Automatic backups handled by Render
- **Connection Pool**: asyncpg with connection pooling for optimal performance
- **Tables**: All bot data stored in Render's PostgreSQL instance

### Environment Variables (Set in Render Dashboard)
- DISCORD_TOKEN_PART1 & DISCORD_TOKEN_PART2 (split for security)
- DISCORD_CLIENT_ID_PART1 & DISCORD_CLIENT_ID_PART2
- DATABASE_URL (automatically provided by Render PostgreSQL)
- **Price Tracking APIs**: FXAPI_KEY, ALPHA_VANTAGE_KEY, TWELVE_DATA_KEY, FMP_KEY (for live price monitoring)

### Deployment Files
- `render.yaml`: Service configuration for Render deployment
- `main.py`: Entry point with health check endpoint on /health
- All dependencies specified in render.yaml buildCommand

**IMPORTANT**: Any code changes must be compatible with Render's PostgreSQL database and Python 3.11 runtime.

## System Architecture

### Core Technologies
- **Runtime**: Python 3.11
- **Framework**: Discord.py 2.5.2 (async/await architecture)
- **Environment Management**: python-dotenv for configuration
- **Web Framework**: aiohttp for web server functionality
- **Deployment**: **RENDER.COM 24/7 HOSTING** (Production deployment with Render PostgreSQL database)

### Security Architecture
- **Split Token System**: Discord bot tokens are split into two environment variables for enhanced security.
- **Environment-based Configuration**: All sensitive data is stored in environment variables.
- **Secure Deployment**: No hardcoded credentials in source code.

### Key Features

#### Bot Commands
- **`/entry`**: Creates and distributes trading signals with automatic TP/SL calculation, multi-channel distribution, and role tagging. Supports various forex and commodity pairs.
- **`/giveaway`**: Comprehensive giveaway management system supporting step-by-step setup, flexible durations, role-based entry, winner selection, and professional embeds.
- **`/dbstatus`**: Monitors database health, connection status, table verification, and performance metrics.
- **`/timedautorole`**: Manages automatic role assignment system for new members with configurable duration and weekend handling.
- **`/dmstatus`**: Tracks and displays which users have received 3, 7, or 14-day follow-up messages after their timed auto-role expires.
- **`/invitetracking`**: Comprehensive invite tracking system for managing server invites, setting nicknames, viewing statistics, and tracking member retention through specific invite links.
- **`/pricetracking`**: Toggle the live price tracking system on/off with real-time monitoring configuration.
- **`/activetrades`**: View all currently tracked trading signals with status, TP hits, and breakeven information.
- **`/pricetest`**: Test live price retrieval for any trading pair using multiple API fallbacks.
- **`/level`**: Check individual user level/message count or display server leaderboard with top community members ranked by activity.
- **`/antiabuse`**: Comprehensive anti-abuse system management for viewing blocked users, manual blocking/unblocking, and viewing abuse prevention statistics.

#### Trading Logic
- **Pip Calculation Engine**: Instrument-specific pip value calculations.
- **Price Formatting**: Decimal precision based on trading pair requirements.
- **Automatic TP/SL Generation**: Standardized TP1, TP2, TP3, and SL levels based on pip values from entry.

#### Message Distribution
- **Multi-channel Broadcasting**: Simultaneous signal distribution to selected channels.
- **Role Tagging System**: Configurable role mentions at the bottom of messages.

#### Community Management
- **Level System**: Tracks user messages and calculates experience levels with comprehensive leaderboard functionality showing top community members.
- **Anti-Abuse Protection**: Sophisticated abuse prevention using account age verification (7+ days) and rapid join pattern detection (max 5 joins/hour).
- **Comprehensive Tracking**: All systems use Render PostgreSQL for persistent 24/7 data storage with automatic recovery capabilities.

#### Timed Auto-Role System
- **Automatic Role Assignment**: Assigns specified roles to new members upon joining with a configurable duration.
- **Automatic Role Removal**: Removes expired roles via a background monitoring task.
- **Custom DM Notifications**: Sends personalized messages when roles expire.
- **Persistent Storage**: Maintains member tracking across bot restarts.
- **Admin Controls**: `/timedautorole` command for management.

#### Live Price Tracking System
- **Real-time Monitoring**: Automatically detects and tracks trading signals containing "Trade Signal For:" from owner and bot messages.
- **Multiple API Fallbacks**: Uses FXApi, Twelve Data, Alpha Vantage, and Financial Modeling Prep APIs for reliable price data.
- **Smart TP/SL Detection**: Monitors all TP1, TP2, TP3, and SL levels with precise price comparison logic.
- **Breakeven Logic**: After TP2 is hit, automatically moves SL to entry price and monitors for breakeven scenarios.
- **Professional Notifications**: Sends @everyone alerts when TP/SL levels are hit with detailed status updates.
- **Background Task**: 30-second interval price checking for all active trades with error handling and cleanup.
- **Owner-Only Control**: Complete system management restricted to Discord ID 462707111365836801.
- **Automatic Signal Parsing**: Intelligent regex-based extraction of pair, action, entry, TP1-3, and SL from signal messages.
- **Excluded Channel**: Respects excluded channel ID 1394958907943817326 to prevent tracking test signals.

### Data Flow
- **Command Input**: User executes slash command.
- **Parameter Validation**: Bot validates input.
- **Calculation Engine**: Performs TP/SL calculations.
- **Message Generation**: Formats output professionally.
- **Multi-channel Distribution**: Broadcasts to selected channels.
- **Role Notification**: Tags specified roles.

### Deployment Strategy
- **Multi-platform Support**: Deployment on Heroku, Railway, DigitalOcean App Platform, and Replit.
- **Deployment Configuration**: Standard build and start commands for Python applications.

## External Dependencies

### Required Packages
- `discord.py==2.5.2`
- `python-dotenv==1.1.0`
- `aiohttp==3.12.13`
- `asyncpg==0.30.0`
- `pyrogram==2.0.106`
- `tgcrypto==1.2.5`
- `pytz>=2025.2`
- `requests>=2.32.4`

### Discord API Requirements
- Bot token with permissions for Send Messages, Use Slash Commands, Mention Everyone, and Read Message History.
- For auto-role system: Manage Roles, Send Messages, Use Slash Commands, View Members.

### Environment Variables
- `DISCORD_TOKEN_PART1`, `DISCORD_TOKEN_PART2`
- `DISCORD_CLIENT_ID_PART1`, `DISCORD_CLIENT_ID_PART2`
- `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_PHONE_NUMBER`
- `TELEGRAM_SOURCE_CHAT_ID` (optional)
- `TELEGRAM_DEFAULT_CHANNELS` (comma-separated)
- `TELEGRAM_DEFAULT_ROLES` (comma-separated)