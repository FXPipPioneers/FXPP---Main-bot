# Discord Trading Bot

## Overview
This project is a professional Discord bot designed for trading signal distribution, automatic Take Profit (TP) and Stop Loss (SL) calculations, and comprehensive statistics tracking. Its main purpose is to provide a complete solution for forex and commodity trading communities to efficiently share and track trading signals across multiple Discord channels, enhancing communication and data management for trading insights. The project aims to streamline signal dissemination and performance analysis for trading groups, offering 24/7 persistent tracking and recovery.

## User Preferences
- Preferred communication style: Simple, everyday language.
- Bot owner restricted to Discord ID: 462707111365836801
- TP/SL calculations updated: TP1=20 pips, TP2=40 pips, TP3=70 pips, SL=50 pips (changed from TP2=50, TP3=100, SL=70)

## Recent Improvements (September 10, 2025)
- **Bulletproof TP/SL Detection**: Enhanced reliability with comprehensive API failure handling, duplicate protection, and database consistency verification
- **Instant Signal Tracking**: New trades checked immediately after `/entry` command instead of waiting 8 minutes
- **Optimized API Usage**: Reverted to 8-minute intervals respecting 15-minute API refresh cycles while maintaining instant checks for new signals
- **Enhanced Error Visibility**: All critical failures now logged to debug channel with highly visible alerts
- **Perfect Override/Tracking Unity**: Fixed synchronization between manual `/tradeoverride` and automatic tracking to prevent duplicate TP/SL notifications - when you manually set a status, the automatic system completely respects it

## System Architecture

### Core Technologies
- **Runtime**: Python 3.11
- **Framework**: Discord.py 2.5.2 (async/await architecture)
- **Environment Management**: python-dotenv for configuration
- **Web Framework**: aiohttp for web server functionality
- **Database**: PostgreSQL (managed by Render)
- **Deployment**: Render.com (24/7 hosting with automatic scaling)

### Security Architecture
- **Split Token System**: Discord bot tokens are split into two environment variables.
- **Environment-based Configuration**: All sensitive data is stored in environment variables.
- **Secure Deployment**: No hardcoded credentials in source code.

### UI/UX Decisions
- **Interactive Override System**: User-friendly dropdown menus for `/tradeoverride` command.
- **Pagination**: `/activetrades` command uses pagination to prevent Discord character limit issues.
- **Professional Embeds**: Giveaways and other bot communications utilize professional embeds.
- **Visual Indicators**: Live tracking displays visual level indicators for hit TP/SL levels.

### Feature Specifications
- **Trading Signal Management**:
    - **`/entry`**: Creates and distributes trading signals with automatic TP/SL calculation and multi-channel distribution.
    - **Live Price Tracking**: Real-time monitoring of active trades with automatic TP/SL detection and breakeven logic.
    - **Limit Order Functionality**: Handles limit orders, detects entry hits, and recalculates TP/SL based on live prices.
    - **Signal Recovery**: Automatic recovery of active trades from the database on startup.
    - **Override System**: Allows manual override of trade statuses (SL hit, TP1-3 hit) with instant notifications.
- **Community Management**:
    - **`/giveaway`**: Comprehensive giveaway management.
    - **`/timedautorole`**: Manages automatic role assignment for new members.
    - **`/dmstatus`**: Tracks follow-up messages for timed auto-role users.
    - **`/invitetracking`**: Tracks server invites and member retention.
    - **`/level`**: Tracks user activity, displays individual levels, and shows a server leaderboard.
    - **`/antiabuse`**: Manages a system for blocking abusive users based on account age and join patterns.
- **System Monitoring**:
    - **`/dbstatus`**: Monitors database health and performance.
    - **`/activetrades`**: Views detailed status of all tracked signals with live price analysis.
    - **`/pricetest`**: Tests live price retrieval from APIs.

### System Design Choices
- **Persistent Data Storage**: PostgreSQL database for all active trading signals, ensuring data persistence across restarts.
- **24/7 Operation**: Designed for continuous operation with automatic price tracking and API fallback mechanisms.
- **Optimized Price Tracking**: Sequential API checking and reduced price check intervals for responsiveness and efficiency.
- **Robust Error Handling**: Critical errors (API failures, database errors) are logged to a debug channel with visible alerts.
- **Reliable TP/SL Detection**: Comprehensive system to prevent missed TP hits, including API failure handling, duplicate protection, and data consistency verification.

## External Dependencies

### Required Packages
- `discord.py==2.5.2`
- `python-dotenv==1.1.0`
- `aiohttp==3.12.13`
- `asyncpg==0.30.0`
- `pytz>=2025.2`
- `requests>=2.32.4`

### APIs and Services
- **Discord API**: For bot functionality and interaction.
- **PostgreSQL**: Managed database service (Render PostgreSQL).
- **Price Tracking APIs**:
    - CurrencyBeacon
    - ExchangeRate-API
    - Currencylayer
    - AbstractAPI
    - FXApi
    - Twelve Data
    - Alpha Vantage
    - Financial Modeling Prep (FMP)

### Environment Variables
- `DISCORD_TOKEN_PART1`, `DISCORD_TOKEN_PART2`
- `DISCORD_CLIENT_ID_PART1`, `DISCORD_CLIENT_ID_PART2`
- `DATABASE_URL`
- `FXAPI_KEY`, `ALPHA_VANTAGE_KEY`, `TWELVE_DATA_KEY`, `FMP_KEY`