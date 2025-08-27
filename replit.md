# Discord Trading Bot

## Overview
This project is a professional Discord bot designed for trading signal distribution, automatic Take Profit (TP) and Stop Loss (SL) calculations, and comprehensive statistics tracking. Its main purpose is to provide a complete solution for forex and commodity trading communities to efficiently share and track trading signals across multiple Discord channels, enhancing communication and data management for trading insights. The project aims to streamline signal dissemination and performance analysis for trading groups.

## User Preferences
- Preferred communication style: Simple, everyday language.
- Bot owner restricted to Discord ID: 462707111365836801
- TP/SL calculations updated: TP1=20 pips, TP2=40 pips, TP3=70 pips, SL=50 pips (changed from TP2=50, TP3=100, SL=70)

## Recent Changes (August 27, 2025)
- **Updated TP/SL calculations**: Changed from TP1=20, TP2=50, TP3=100, SL=70 pips to TP1=20, TP2=40, TP3=70, SL=50 pips
- **Added comprehensive invite tracking**: New database tables and functions to track all server invites with nicknames, join counts, left counts, and current member counts
- **Added DM status tracking**: New `/dmstatus` command to monitor which users have received 3, 7, and 14-day follow-up messages
- **Added invite management**: New `/invitetracking` command for managing server invite statistics and collaboration tracking
- **Enhanced member tracking**: Updated member join/leave handlers to properly track invite usage and statistics
- **Enforced owner-only commands**: All commands now restricted to Discord ID 462707111365836801

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
- TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE_NUMBER
- TELEGRAM_SOURCE_CHAT_ID, TELEGRAM_DEFAULT_CHANNELS, TELEGRAM_DEFAULT_ROLES
- DATABASE_URL (automatically provided by Render PostgreSQL)

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
- **`/stats`**: Displays trading performance statistics customizable by date ranges, including TP/SL hit tracking and win rate calculations.
- **`/giveaway`**: Comprehensive giveaway management system supporting step-by-step setup, flexible durations, role-based entry, winner selection, and professional embeds.
- **`/telegram`**: Checks Telegram integration status and provides setup guidance.
- **`/dbstatus`**: Monitors database health, connection status, table verification, and performance metrics.
- **`/timedautorole`**: Manages automatic role assignment system for new members with configurable duration and weekend handling.
- **`/dmstatus`**: Tracks and displays which users have received 3, 7, or 14-day follow-up messages after their timed auto-role expires.
- **`/invitetracking`**: Comprehensive invite tracking system for managing server invites, setting nicknames, viewing statistics, and tracking member retention through specific invite links.

#### Trading Logic
- **Pip Calculation Engine**: Instrument-specific pip value calculations.
- **Price Formatting**: Decimal precision based on trading pair requirements.
- **Automatic TP/SL Generation**: Standardized TP1, TP2, TP3, and SL levels based on pip values from entry.

#### Message Distribution
- **Multi-channel Broadcasting**: Simultaneous signal distribution to selected channels.
- **Role Tagging System**: Configurable role mentions at the bottom of messages.

#### Telegram Integration
- **Automatic Signal Forwarding**: Monitors Telegram groups for trading signals, intelligently parses them, and relays them to Discord instantly.
- **Broker Warnings**: Adds specific warnings for volatile pairs.
- **Configurable Monitoring**: Can monitor specific chat IDs or all accessible chats.

#### Timed Auto-Role System
- **Automatic Role Assignment**: Assigns specified roles to new members upon joining with a configurable duration.
- **Automatic Role Removal**: Removes expired roles via a background monitoring task.
- **Custom DM Notifications**: Sends personalized messages when roles expire.
- **Persistent Storage**: Maintains member tracking across bot restarts.
- **Admin Controls**: `/timedautorole` command for management.

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