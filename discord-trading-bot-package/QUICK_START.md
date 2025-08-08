# Discord Trading Bot - Quick Start Guide

## 🚀 One-Click Installation

### Windows Users:
1. **Download** this entire `discord-trading-bot-package` folder
2. **Double-click** `install.bat`
3. **Follow** the on-screen instructions

### Mac/Linux Users:
1. **Download** this entire `discord-trading-bot-package` folder
2. **Open terminal** in the folder
3. **Run**: `./install.sh`
4. **Follow** the on-screen instructions

## ⚙️ Configuration

### 1. Discord Bot Setup
1. Go to https://discord.com/developers/applications
2. Create a new application
3. Go to "Bot" section
4. Copy your bot token
5. Split the token in half and set environment variables:
   - `DISCORD_TOKEN_PART1=first_half_of_token`
   - `DISCORD_TOKEN_PART2=second_half_of_token`

### 2. Environment Variables
- **Copy** `.env.example` to `.env`
- **Edit** `.env` with your actual values
- **Never share** your `.env` file with anyone

### 3. Bot Permissions
Your Discord bot needs these permissions:
- Send Messages
- Use Slash Commands  
- Manage Roles
- Mention Everyone
- Read Message History
- View Members

## 🏃‍♂️ Running the Bot

### Local Development:
```bash
python main.py
```

### Deploy to Render (Recommended for 24/7):
1. Upload to GitHub (see `GITHUB_SETUP.md`)
2. Connect to Render.com
3. Use the build command from `DEPLOYMENT_INSTRUCTIONS.md`
4. Set environment variables in Render dashboard

## 📋 Available Commands

- `/entry` - Create trading signals
- `/stats` - View trading statistics
- `/telegram` - Check Telegram integration
- `/timedautorole` - Manage auto-role system
- `/tracking` - Monitor Telegram forwarding

## 🆘 Need Help?

1. **Installation Issues**: Check `install.bat` or `install.sh` output
2. **Configuration**: See `DEPLOYMENT_INSTRUCTIONS.md`
3. **GitHub Setup**: See `GITHUB_SETUP.md`
4. **Features**: See `README.md`

## ✅ Verification

After installation, the bot should:
1. Start without errors
2. Connect to Discord
3. Show "✅ Pytz loaded" and "Pyrogram loaded" messages
4. Respond to slash commands

**Your bot is production-ready for 24/7 operation on Render!**