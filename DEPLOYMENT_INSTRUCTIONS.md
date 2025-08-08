# Discord Trading Bot Deployment Instructions

## Updated Deployment for Render.com (with Telegram Integration)

### Build Configuration
- **Build Command**: `pip install --upgrade pip && pip install discord.py==2.5.2 python-dotenv==1.1.0 aiohttp==3.12.13 asyncpg==0.30.0 requests pyrogram==2.0.106 tgcrypto==1.2.5`
- **Start Command**: `python main.py`
- **Runtime**: `python-3.11.0`

### Alternative Build Commands (if primary fails)
1. With no-cache: `pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir discord.py python-dotenv aiohttp asyncpg requests pyrogram tgcrypto`
2. Force reinstall: `pip install --upgrade pip --force-reinstall && pip install --force-reinstall pyrogram==2.0.106 tgcrypto==1.2.5 discord.py==2.5.2 python-dotenv==1.1.0 aiohttp==3.12.13 asyncpg==0.30.0 requests`

### Required Environment Variables

#### Discord Configuration
- `DISCORD_TOKEN_PART1` - First half of Discord bot token
- `DISCORD_TOKEN_PART2` - Second half of Discord bot token  
- `DISCORD_CLIENT_ID_PART1` - First half of Discord client ID
- `DISCORD_CLIENT_ID_PART2` - Second half of Discord client ID

#### Telegram Configuration (for signal forwarding)
- `TELEGRAM_API_ID` - Telegram API ID from my.telegram.org
- `TELEGRAM_API_HASH` - Telegram API hash from my.telegram.org
- `TELEGRAM_PHONE_NUMBER` - Phone number associated with Telegram account
- `TELEGRAM_SOURCE_CHAT_ID` - Chat ID of the source trading group
- `TELEGRAM_DEFAULT_CHANNELS` - Default Discord channels for forwarding (comma-separated)
- `TELEGRAM_DEFAULT_ROLES` - Default roles to mention (comma-separated)

### Project Files
- `main.py` - Main bot application
- `pyproject.toml` - Python project configuration with dependencies
- `DEPLOYMENT_INSTRUCTIONS.md` - This deployment guide
- `README.md` - Project overview and setup guide

### Troubleshooting

If you get "Telegram integration not installed" error on Render:
1. Check that the build command ran successfully in deployment logs
2. Verify all packages were installed: `pyrogram==2.0.106` and `tgcrypto==1.2.5`
3. Use one of the alternative build commands above if needed

### Manual Installation (if needed)
If automatic deployment fails, you can manually install dependencies:
```bash
pip install --upgrade pip
pip install discord.py==2.5.2
pip install python-dotenv==1.1.0  
pip install aiohttp==3.12.13
pip install asyncpg==0.30.0
pip install pyrogram==2.0.106
pip install tgcrypto==1.2.5
```

### Health Check
The bot provides health check endpoints at:
- `/` - Basic status
- `/health` - Health check endpoint

Both endpoints return "Discord Trading Bot is running!" when the service is active.