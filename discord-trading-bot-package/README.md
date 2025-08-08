
# Discord Trading Bot with Timed Auto-Role System

A comprehensive Discord bot for trading signal distribution with automatic TP/SL calculations, statistics tracking, and an advanced timed auto-role system with PostgreSQL database integration.

## 🚀 Key Features

### 📊 Trading Signal Management
- **Entry Signal Creation**: `/entry` command with automatic TP/SL calculations
- **Statistics Tracking**: `/stats` command for performance analytics
- **Multi-Channel Distribution**: Send signals to multiple channels simultaneously
- **Role Tagging**: Mention specific roles in trading signals
- **Professional Formatting**: Proper decimal places and pip values per trading pair

### 🎯 Advanced Auto-Role System
- **Timed Role Assignment**: 24-hour temporary roles for new members
- **Weekend Handling**: Smart weekend detection with delayed activation
- **Persistent Memory**: PostgreSQL database for tracking across bot restarts
- **Anti-Abuse Protection**: Prevents users from rejoining to get roles again
- **Automated DM System**: Follow-up messages at 3, 7, and 14 days
- **Background Tasks**: Automatic role removal and notifications

### 💾 Database Integration
- **PostgreSQL Support**: Full database integration with asyncpg
- **Persistent Storage**: Member tracking survives bot restarts
- **Role History**: Complete audit trail of role assignments
- **Anti-Duplicate System**: Prevents role farming by rejoining

### 📱 Telegram Integration (Optional)
- **Signal Forwarding**: Monitor Telegram groups for trading signals
- **Intelligent Parsing**: Automatic signal recognition and formatting
- **Multi-Platform**: Bridge between Telegram and Discord communities

## 🛠️ Trading Pair Support

The bot supports 25+ trading pairs with automatic pip calculations:

**Major Forex Pairs:**
- EURUSD, GBPUSD, AUDUSD, NZDUSD (4 decimals, 0.0001 pip)
- USDJPY, GBPJPY, EURJPY, AUDJPY (3 decimals, 0.01 pip)

**Commodities:**
- XAUUSD (Gold), XAGUSD (Silver) (2 decimals, 0.1 pip)

**Indices:**
- US100, US500, GER40 (variable decimals, automatic calculation)

**Cross Pairs:**
- EURGBP, EURAUD, GBPAUD, AUDNZD, and many more

## 🔧 Installation & Setup

### Prerequisites
- Python 3.11+
- PostgreSQL database (Render PostgreSQL recommended)
- Discord Bot Token
- Discord Application with proper permissions

### Environment Variables

**Discord Configuration (Required):**
```env
DISCORD_TOKEN_PART1=first_half_of_your_discord_token
DISCORD_TOKEN_PART2=second_half_of_your_discord_token
DISCORD_CLIENT_ID_PART1=first_part_of_client_id
DISCORD_CLIENT_ID_PART2=second_part_of_client_id
```

**Database Configuration (Required):**
```env
DATABASE_URL=postgresql://username:password@host:port/database
```

**Telegram Integration (Optional):**
```env
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_PHONE_NUMBER=your_phone_number
TELEGRAM_SOURCE_CHAT_ID=chat_id_to_monitor
TELEGRAM_DEFAULT_CHANNELS=channel_ids_comma_separated
TELEGRAM_DEFAULT_ROLES=role_names_comma_separated
```

### Local Development
```bash
# Install dependencies
pip install discord.py python-dotenv aiohttp asyncpg pyrogram tgcrypto

# Run the bot
python main.py
```

## 🚀 Deployment on Replit

### 1. Project Setup
1. Fork or import this repository into Replit
2. The project includes all necessary configuration files

### 2. Environment Configuration
Set these secrets in your Replit environment:
- `DISCORD_TOKEN_PART1` - First half of Discord bot token
- `DISCORD_TOKEN_PART2` - Second half of Discord bot token
- `DISCORD_CLIENT_ID_PART1` - First part of Discord client ID
- `DISCORD_CLIENT_ID_PART2` - Second part of Discord client ID
- `DATABASE_URL` - PostgreSQL connection string

### 3. Database Setup
You'll need a PostgreSQL database. Recommended options:
- **Render PostgreSQL**: Free tier available, automatic SSL
- **Supabase**: Generous free tier with dashboard
- **Neon**: Serverless PostgreSQL with free tier

### 4. Run the Bot
Click the "Run" button in Replit. The bot will:
- Install all dependencies automatically
- Connect to your database
- Start the web server on port 5000
- Initialize Discord bot connection

## 📋 Command Reference

### `/entry` - Create Trading Signal
Create and distribute trading signals with automatic calculations.

**Parameters:**
- `entry_type`: Buy limit, Sell limit, Buy execution, Sell execution
- `pair`: Trading pair (EURUSD, XAUUSD, etc.)
- `price`: Entry price
- `channels`: Comma-separated channel mentions
- `roles`: Roles to mention (required)

**Example:**
```
/entry entry_type:Buy execution pair:XAUUSD price:2650.50 channels:#signals,#trading-room roles:@traders,@vip
```

### `/stats` - Trading Statistics
Generate and send performance statistics.

**Parameters:**
- `date_range`: Period for statistics
- `total_signals`: Total signals sent
- `tp1_hits`, `tp2_hits`, `tp3_hits`: TP level hits
- `sl_hits`: Stop loss hits
- `channels`: Where to send stats
- `currently_open`: Open trades (optional)

### `/timedautorole` - Auto-Role Management
Configure the automatic role assignment system.

**Actions:**
- `enable` - Enable auto-role with specified role
- `disable` - Disable the system
- `status` - Check current configuration
- `list` - Show active temporary role holders

**Example:**
```
/timedautorole action:enable role:@Premium Access
```

## 🏗️ System Architecture

### Core Components
- **Discord.py**: Async Discord API wrapper
- **PostgreSQL**: Persistent data storage
- **Background Tasks**: Role management and cleanup
- **Web Server**: Health checks and uptime monitoring
- **Timezone Handling**: Amsterdam timezone for weekend detection

### Database Schema
The bot automatically creates necessary tables:
- `auto_role_members`: Active temporary role tracking
- `role_history`: Complete audit trail of role assignments
- `member_blacklist`: Anti-abuse tracking

### Security Features
- **Split Token System**: Enhanced security for Discord credentials
- **Environment-based Config**: No hardcoded sensitive data
- **Permission Validation**: Proper Discord permission checks
- **Error Handling**: Comprehensive exception management

## 📊 Auto-Role System Details

### Weekend Handling
- **Detection**: Automatically detects Amsterdam timezone weekends
- **Smart Activation**: 24-hour countdown starts Monday 00:01
- **User Notification**: Explains weekend delay via DM

### Follow-up DM System
Automated messages sent after role expiration:
- **Day 3**: Initial follow-up invitation
- **Day 7**: Weekly reminder with value proposition
- **Day 14**: Final invitation with benefits explanation

*Note: DMs are only sent if user doesn't have @Gold Pioneer role*

### Anti-Abuse Protection
- **One-Time Only**: Users can only receive auto-role once per Discord account
- **Persistent Tracking**: Database survives bot restarts
- **Admin Override**: Manual `/timedautorole adduser` always works

## 🔍 Monitoring & Health Checks

The bot includes built-in monitoring:
- **Health Endpoint**: `/health` for uptime monitoring
- **Status Endpoint**: `/status` for detailed bot information
- **Console Logging**: Comprehensive activity logs
- **Error Reporting**: Detailed error tracking

## 📈 Performance Features

- **Async Architecture**: Non-blocking operations
- **Connection Pooling**: Efficient database connections
- **Background Tasks**: Separate threads for role management
- **Memory Optimization**: Efficient data structures
- **Rate Limit Handling**: Discord API rate limit compliance

## 🛡️ Required Discord Permissions

For full functionality, the bot needs:
- **Send Messages**: Basic messaging capability
- **Use Slash Commands**: Command functionality
- **Manage Roles**: Auto-role assignment/removal
- **Read Message History**: Channel access
- **Mention Everyone**: Role tagging in signals
- **Send Private Messages**: DM notifications

## 📁 Project Structure
```
discord-trading-bot/
├── main.py                 # Main bot application
├── requirements.txt        # Python dependencies
├── dependencies.txt        # Alternative dependency list
├── .env.example           # Environment template
├── README.md              # This file
├── pyproject.toml         # Python project configuration
├── .replit                # Replit configuration
├── render.yaml            # Render deployment config
└── discord-bot-updated/   # Updated version directory
    ├── main.py
    ├── requirements.txt
    ├── README.md
    └── various config files
```

## 🐛 Troubleshooting

### Common Issues
1. **Token Errors**: Ensure both token parts are set correctly
2. **Database Connection**: Verify DATABASE_URL format
3. **Permission Errors**: Check Discord bot permissions
4. **Role Issues**: Ensure bot role is above managed roles

### Debug Steps
1. Check Replit console for error messages
2. Verify all environment variables are set
3. Test database connection
4. Confirm Discord bot permissions in server settings

## 📄 License

This project is for educational and personal use. Please respect Discord's Terms of Service and API guidelines.

## 🤝 Support

For issues or questions:
1. Check the console logs in Replit
2. Verify your environment configuration
3. Ensure database connectivity
4. Test with simple commands first

---

**Note**: This bot includes sophisticated features like persistent database storage, timezone-aware weekend handling, and comprehensive anti-abuse systems. Make sure your PostgreSQL database is properly configured before deployment.
