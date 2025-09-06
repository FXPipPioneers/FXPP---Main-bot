"""
Discord Trading Bot - Professional Signal Distribution System

🚀 PRODUCTION DEPLOYMENT: RENDER.COM 24/7 HOSTING
📊 DATABASE: Render PostgreSQL (managed instance)
🌍 REGION: Oregon, Python 3.11.0 runtime

DEPLOYMENT INFO:
- Hosted on Render.com web service (24/7 uptime)
- PostgreSQL database managed by Render
- Health endpoint: /health for monitoring
- Environment variables set in Render dashboard
- Manual deployments via render.yaml configuration

Author: Advanced Trading Bot System
Version: Production Ready - Render Optimized
"""

import discord
from discord.ext import commands, tasks
from discord import app_commands
import os
from dotenv import load_dotenv
import asyncio
import aiohttp
from aiohttp import web
import json
from datetime import datetime, timedelta, timezone
import asyncpg
import logging
from typing import Optional, Dict
import re

# Try to import pytz for proper timezone handling, fallback to basic timezone if not available
try:
    import pytz
    PYTZ_AVAILABLE = True
    print("✅ Pytz loaded - Full timezone support enabled")
except ImportError:
    PYTZ_AVAILABLE = False
    print("⚠️ Pytz not available - Using basic timezone handling")

# Telegram integration removed as per user request

# Price tracking APIs
import requests
import re
from typing import Dict, List, Optional, Tuple

# Load environment variables
load_dotenv()

# Reconstruct tokens from split parts for enhanced security
DISCORD_TOKEN_PART1 = os.getenv("DISCORD_TOKEN_PART1", "")
DISCORD_TOKEN_PART2 = os.getenv("DISCORD_TOKEN_PART2", "")
DISCORD_TOKEN = DISCORD_TOKEN_PART1 + DISCORD_TOKEN_PART2


DISCORD_CLIENT_ID_PART1 = os.getenv("DISCORD_CLIENT_ID_PART1", "")
DISCORD_CLIENT_ID_PART2 = os.getenv("DISCORD_CLIENT_ID_PART2", "")
DISCORD_CLIENT_ID = DISCORD_CLIENT_ID_PART1 + DISCORD_CLIENT_ID_PART2

# Bot owner user ID for command restrictions
BOT_OWNER_USER_ID = os.getenv("BOT_OWNER_USER_ID", "462707111365836801")
if BOT_OWNER_USER_ID:
    print(f"✅ Bot owner ID loaded: {BOT_OWNER_USER_ID}")
else:
    print("⚠️ BOT_OWNER_USER_ID not set - all users can use commands")

# Bot setup with intents
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.members = True  # Required for member join events

# Auto-role system storage with weekend handling and memory system
AUTO_ROLE_CONFIG = {
    "enabled": False,
    "role_id": None,
    "duration_hours": 24,  # Fixed at 24 hours
    "custom_message":
    "Hey! Your **24-hour free access** to the <#1350929852299214999> channel has unfortunately **ran out**. We truly hope you were able to benefit with us & we hope to see you back soon! For now, feel free to continue following our trade signals in ⁠<#1350929790148022324>",
    "active_members":
    {},  # member_id: {"role_added_time": datetime, "role_id": role_id, "guild_id": guild_id, "expiry_time": datetime}
    "weekend_pending":
    {},  # member_id: {"join_time": datetime, "guild_id": guild_id} for weekend joiners
    "role_history":
    {},  # member_id: {"first_granted": datetime, "times_granted": int, "last_expired": datetime, "guild_id": guild_id}
    "dm_schedule":
    {}  # member_id: {"role_expired": datetime, "guild_id": guild_id, "dm_3_sent": bool, "dm_7_sent": bool, "dm_14_sent": bool}
}

# Log channel ID for Discord logging
LOG_CHANNEL_ID = 1350888185487429642
# Price tracking debug channel ID
PRICE_DEBUG_CHANNEL_ID = 1412344974871105567

# Gold Pioneer role ID for checking membership before sending follow-up DMs
GOLD_PIONEER_ROLE_ID = 1384489575187091466

# Giveaway channel ID (always post giveaways here)
GIVEAWAY_CHANNEL_ID = 1405490561963786271

# Global storage for active giveaways
ACTIVE_GIVEAWAYS = {}  # giveaway_id: {message_id, participants, settings, etc}

# Global storage for invite tracking
INVITE_TRACKING = {}  # invite_code: {"nickname": str, "total_joins": int, "total_left": int, "current_members": int, "creator_id": int, "guild_id": int}

# Live price tracking system configuration - 13 API POWERHOUSE
PRICE_TRACKING_CONFIG = {
    "enabled": True,  # 24/7 monitoring enabled by default
    "excluded_channel_id": "1394958907943817326",
    "owner_user_id": "462707111365836801",
    "signal_keyword": "Trade Signal For:",
    "active_trades": {},  # message_id: {trade_data}
    "api_keys": {
        # Original 4 APIs (FIXED)
        "fxapi_key": os.getenv("FXAPI_KEY", ""),
        "alpha_vantage_key": os.getenv("ALPHA_VANTAGE_KEY", ""),
        "twelve_data_key": os.getenv("TWELVE_DATA_KEY", ""),
        "fmp_key": os.getenv("FMP_KEY", ""),
        # 9 NEW APIs for maximum accuracy
        "exchangerate_key": os.getenv("EXCHANGERATE_API_KEY", ""),  # Free 1,500/month
        "currencylayer_key": os.getenv("CURRENCYLAYER_KEY", ""),   # Free 1,000/month
        "fixer_key": os.getenv("FIXER_API_KEY", ""),               # Free 1,000/month
        "openexchange_key": os.getenv("OPENEXCHANGE_KEY", ""),     # Free 1,000/month
        "currencyapi_key": os.getenv("CURRENCYAPI_KEY", ""),       # Free 300/month
        "apilayer_key": os.getenv("APILAYER_KEY", ""),             # Free 1,000/month  
        "abstractapi_key": os.getenv("ABSTRACTAPI_KEY", ""),       # Free 1,000/month
        "currencybeacon_key": os.getenv("CURRENCYBEACON_KEY", ""), # Free 5,000/month
        "polygon_key": os.getenv("POLYGON_API_KEY", "")            # Free 5 calls/min
    },
    "api_endpoints": {
        # FIXED Original 4 APIs 
        "fxapi": "https://api.fxapi.com/v1/latest",                # CORRECTED ENDPOINT
        "alpha_vantage": "https://www.alphavantage.co/query",
        "twelve_data": "https://api.twelvedata.com/quote",
        "fmp": "https://financialmodelingprep.com/api/v3/quote-short",
        # 9 NEW API Endpoints  
        "exchangerate": "https://v6.exchangerate-api.com/v6",
        "currencylayer": "https://apilayer.net/api/live",
        "fixer": "https://api.fixer.io/latest",
        "openexchange": "https://openexchangerates.org/api/latest.json",
        "currencyapi": "https://api.currencyapi.com/v3/latest",
        "apilayer": "https://api.apilayer.com/exchangerates_data/latest",
        "abstractapi": "https://exchange-rates.abstractapi.com/v1/live",
        "currencybeacon": "https://api.currencybeacon.com/v1/latest",
        "polygon": "https://api.polygon.io/v2/aggs/ticker"
    },
    "last_price_check": {},  # pair: last_check_timestamp
    "check_interval": 45,   # OPTIMIZED: 45 seconds with 13 APIs = ~20 calls per API per hour (well within limits)
    "api_rotation_index": 0,  # for rotating through APIs efficiently
    "api_priority": [        # Priority order - most accurate first (only working APIs)
        "fxapi", "twelve_data", "fmp", "exchangerate", 
        "currencybeacon", "fixer", "apilayer", "currencyapi", "openexchange", 
        "abstractapi", "currencylayer", "polygon"
    ]
}

# Level system configuration
LEVEL_SYSTEM = {
    "enabled": True,
    "user_data": {},  # user_id: {"message_count": int, "current_level": int, "guild_id": guild_id}
    "level_requirements": {
        1: 10,      # Level 1: 10 messages (very easy start)
        2: 25,      # Level 2: 25 messages (easy)
        3: 50,      # Level 3: 50 messages (moderate)
        4: 100,     # Level 4: 100 messages (decent activity)
        5: 200,     # Level 5: 200 messages (good activity)
        6: 400,     # Level 6: 400 messages (high activity)
        7: 700,     # Level 7: 700 messages (very high activity)  
        8: 1200     # Level 8: 1200 messages (maximum activity)
    },
    "level_roles": {
        1: 1407632176060698725,
        2: 1407632223578095657,
        3: 1407632987029508166,
        4: 1407632891965608110,
        5: 1407632408580198440,
        6: 1407633424952332428,
        7: 1407632350543872091,
        8: 1407633380916465694
    }
}

# Amsterdam timezone handling with fallback
if PYTZ_AVAILABLE:
    AMSTERDAM_TZ = pytz.timezone(
        'Europe/Amsterdam')  # Proper Amsterdam timezone with DST support
else:
    # Fallback: Use UTC+1 (CET) as approximation
    AMSTERDAM_TZ = timezone(
        timedelta(hours=1))  # Basic Amsterdam timezone without DST


class TradingBot(commands.Bot):

    def __init__(self):
        super().__init__(command_prefix='!', intents=intents)
        self.log_channel = None
        self.price_debug_channel = None
        self.db_pool = None
        self.client_session = None
        self.last_online_time = None
        self.last_heartbeat = None
        # Initialize _cached_invites here
        self._cached_invites = {}

    async def log_to_discord(self, message):
        """Send log message to Discord channel"""
        if self.log_channel:
            try:
                await self.log_channel.send(f"📋 **Bot Log:** {message}")
            except Exception as e:
                print(f"Failed to send log to Discord: {e}")
        # Always print to console as backup
        print(message)

    async def log_price_debug(self, message):
        """Send price tracking debug message to its dedicated Discord channel"""
        if self.price_debug_channel:
            try:
                await self.price_debug_channel.send(f"💰 **Price Debug:** {message}")
            except Exception as e:
                print(f"Failed to send price debug message to Discord: {e}")
        # Always print to console as backup
        print(f"PRICE_DEBUG: {message}")

    async def close(self):
        """Cleanup when bot shuts down"""
        # Record offline time for recovery
        self.last_online_time = datetime.now(AMSTERDAM_TZ)
        if self.db_pool:
            try:
                await self.save_bot_status()
            except Exception as e:
                print(f"Failed to save bot status: {e}")

        # Close aiohttp client session to prevent unclosed client session warnings
        if self.client_session:
            await self.client_session.close()
            print("✅ Aiohttp client session closed properly")

        # Close database pool
        if self.db_pool:
            await self.db_pool.close()
            print("✅ Database connection pool closed")

        # Call parent close
        await super().close()

    async def save_bot_status(self):
        """Save bot status to database for offline recovery"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                current_time = datetime.now(AMSTERDAM_TZ)
                await conn.execute("""
                    INSERT INTO bot_status (last_online, heartbeat_time) 
                    VALUES ($1, $2)
                    ON CONFLICT (id) DO UPDATE SET 
                    last_online = $1, heartbeat_time = $2
                """, current_time, current_time)
        except Exception as e:
            print(f"Failed to save bot status: {e}")

    async def load_bot_status(self):
        """Load last known bot status from database"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchrow("SELECT last_online FROM bot_status WHERE id = 1")
                if result:
                    self.last_online_time = result['last_online']
                    print(f"✅ Loaded last online time: {self.last_online_time}")
        except Exception as e:
            print(f"Failed to load bot status: {e}")

    async def save_price_tracking_config(self):
        """Save price tracking configuration to database for persistence"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                current_time = datetime.now(AMSTERDAM_TZ)
                await conn.execute("""
                    INSERT INTO price_tracking_config (id, enabled, check_interval, last_updated) 
                    VALUES (1, $1, $2, $3)
                    ON CONFLICT (id) DO UPDATE SET 
                    enabled = $1, check_interval = $2, last_updated = $3
                """, PRICE_TRACKING_CONFIG["enabled"], 
                     PRICE_TRACKING_CONFIG["check_interval"], 
                     current_time)
                print(f"✅ Price tracking config saved to database: enabled={PRICE_TRACKING_CONFIG['enabled']}")
        except Exception as e:
            print(f"Failed to save price tracking config: {e}")

    async def load_price_tracking_config(self):
        """Load price tracking configuration from database"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchrow("SELECT enabled, check_interval FROM price_tracking_config WHERE id = 1")
                if result:
                    PRICE_TRACKING_CONFIG["enabled"] = result['enabled']
                    PRICE_TRACKING_CONFIG["check_interval"] = result['check_interval']
                    print(f"✅ Loaded price tracking config: enabled={result['enabled']}, interval={result['check_interval']}s")
                else:
                    # First time setup - ensure enabled and save to database
                    PRICE_TRACKING_CONFIG["enabled"] = True
                    await self.save_price_tracking_config()
                    print("✅ Price tracking initialized as enabled (first time setup)")
        except Exception as e:
            print(f"Failed to load price tracking config: {e}")
            # Fallback to ensure it's enabled
            PRICE_TRACKING_CONFIG["enabled"] = True

    async def recover_offline_members(self):
        """Check for members who joined while bot was offline and assign auto-roles"""
        if not AUTO_ROLE_CONFIG["enabled"] or not AUTO_ROLE_CONFIG["role_id"]:
            return

        try:
            await self.log_to_discord("🔍 Checking for members who joined while bot was offline...")

            # Get the last known online time from database or use current time - 24 hours as fallback
            offline_check_time = self.last_online_time
            if not offline_check_time:
                # If we don't know when we were last online, check last 24 hours as safety measure
                offline_check_time = datetime.now(AMSTERDAM_TZ) - timedelta(hours=24)

            recovered_count = 0

            for guild in self.guilds:
                if not guild:
                    continue

                role = guild.get_role(AUTO_ROLE_CONFIG["role_id"])
                if not role:
                    continue

                # Get all members and check join times
                async for member in guild.fetch_members(limit=None):
                    if member.bot:  # Skip bots
                        continue

                    member_id_str = str(member.id)

                    # Check if member joined after we went offline
                    if member.joined_at and member.joined_at.replace(tzinfo=timezone.utc) > offline_check_time.astimezone(timezone.utc):

                        # Check if they already have the role or are already tracked
                        if member_id_str in AUTO_ROLE_CONFIG["active_members"]:
                            continue  # Already tracked

                        if role in member.roles:
                            continue  # Already has role

                        # Check anti-abuse system
                        if member_id_str in AUTO_ROLE_CONFIG["role_history"]:
                            await self.log_to_discord(
                                f"🚫 {member.display_name} joined while offline but blocked by anti-abuse system"
                            )
                            continue

                        # Process this offline joiner
                        join_time = member.joined_at.astimezone(AMSTERDAM_TZ)

                        # Add the role
                        await member.add_roles(role, reason="Auto-role recovery for offline join")

                        # Determine if it was weekend when they joined
                        if self.is_weekend_time(join_time):
                            # Weekend join - expires Monday 23:59
                            monday_expiry = self.get_monday_expiry_time(join_time)

                            AUTO_ROLE_CONFIG["active_members"][member_id_str] = {
                                "role_added_time": join_time.isoformat(),
                                "role_id": AUTO_ROLE_CONFIG["role_id"],
                                "guild_id": guild.id,
                                "weekend_delayed": True,
                                "expiry_time": monday_expiry.isoformat()
                            }

                            # Send weekend DM
                            try:
                                weekend_message = (
                                    "**Welcome to FX Pip Pioneers!** As a welcome gift, we usually give our new members "
                                    "**access to the Premium Signals channel for 24 hours.** However, the trading markets are currently closed for the weekend. "
                                    "**Your 24-hour countdown will start on Monday at 00:01 Amsterdam time** and your premium access will expire on Tuesday at 01:00 Amsterdam time. "
                                    "Good luck trading!"
                                )
                                await member.send(weekend_message)
                            except discord.Forbidden:
                                await self.log_to_discord(f"❌ Could not send weekend DM to {member.display_name} (DMs disabled)")

                        else:
                            # Regular join - 24 hours from join time
                            expiry_time = join_time + timedelta(hours=24)

                            AUTO_ROLE_CONFIG["active_members"][member_id_str] = {
                                "role_added_time": join_time.isoformat(),
                                "role_id": AUTO_ROLE_CONFIG["role_id"],
                                "guild_id": guild.id,
                                "weekend_delayed": False,
                                "expiry_time": expiry_time.isoformat()
                            }

                            # Send regular welcome DM
                            try:
                                welcome_message = (
                                    "**Welcome to FX Pip Pioneers!** As a welcome gift, we've given you "
                                    "**access to the Premium Signals channel for 24 hours.** "
                                    "Good luck trading!"
                                )
                                await member.send(welcome_message)
                            except discord.Forbidden:
                                await self.log_to_discord(f"❌ Could not send welcome DM to {member.display_name} (DMs disabled)")

                        # Record in role history for anti-abuse
                        AUTO_ROLE_CONFIG["role_history"][member_id_str] = {
                            "first_granted": join_time.isoformat(),
                            "times_granted": 1,
                            "last_expired": None,
                            "guild_id": guild.id
                        }

                        recovered_count += 1
                        await self.log_to_discord(f"✅ Recovered offline joiner: {member.display_name}")

            # Save the updated configuration
            await self.save_auto_role_config()

            if recovered_count > 0:
                await self.log_to_discord(f"🎯 Successfully recovered {recovered_count} members who joined while bot was offline!")
            else:
                await self.log_to_discord("✅ No offline members found to recover")

        except Exception as e:
            await self.log_to_discord(f"❌ Error during offline member recovery: {str(e)}")
            print(f"Offline recovery error: {e}")

    async def recover_offline_dm_reminders(self):
        """Check for DM reminders that should have been sent while bot was offline"""
        if not AUTO_ROLE_CONFIG["enabled"] or not self.db_pool:
            return

        try:
            await self.log_to_discord("🔍 Checking for missed DM reminders while offline...")

            current_time = datetime.now(AMSTERDAM_TZ)
            recovered_dms = 0

            # Check all members in DM schedule for missed reminders
            for member_id_str, dm_data in AUTO_ROLE_CONFIG["dm_schedule"].items():
                try:
                    role_expired = datetime.fromisoformat(dm_data["role_expired"]).replace(tzinfo=AMSTERDAM_TZ)
                    guild_id = dm_data["guild_id"]

                    # Calculate when each DM should have been sent
                    dm_3_time = role_expired + timedelta(days=3)
                    dm_7_time = role_expired + timedelta(days=7)
                    dm_14_time = role_expired + timedelta(days=14)

                    guild = self.get_guild(guild_id)
                    if not guild:
                        continue

                    member = guild.get_member(int(member_id_str))
                    if not member:
                        continue

                    # Check if member has Gold Pioneer role (skip DMs if they do)
                    if any(role.id == GOLD_PIONEER_ROLE_ID for role in member.roles):
                        continue

                    # Send missed 3-day DM
                    if not dm_data["dm_3_sent"] and current_time >= dm_3_time:
                        try:
                            dm_message = "Hey! It's been 3 days since your **24-hour free access to the Premium Signals channel** ended. We hope you were able to catch good trades with us during that time.\n\nAs you've probably seen, the **free signals channel only gets about 1 signal a day**, while inside **Gold Pioneers**, members receive **8–10 high-quality signals every single day in <#1350929852299214999>**. That means way more chances to profit and grow consistently.\n\nWe'd love to **invite you back to Premium Signals** so you don't miss out on more solid opportunities.\n\n**Feel free to join us again through this link:** <https://whop.com/gold-pioneer>"
                            await member.send(dm_message)
                            AUTO_ROLE_CONFIG["dm_schedule"][member_id_str]["dm_3_sent"] = True
                            recovered_dms += 1
                            await self.log_to_discord(f"📤 Sent missed 3-day DM to {member.display_name}")
                        except discord.Forbidden:
                            await self.log_to_discord(f"❌ Could not send missed 3-day DM to {member.display_name} (DMs disabled)")

                    # Send missed 7-day DM
                    if not dm_data["dm_7_sent"] and current_time >= dm_7_time:
                        try:
                            dm_message = "It's been a week since your Premium Signals trial ended. Since then, our **Gold Pioneers  have been catching trade setups daily in <#1350929852299214999>**.\n\nIf you found value in just 24 hours, imagine the results you could be seeing by now with full access. It's all about **consistency and staying plugged into the right information**.\n\nWe'd like to **personally invite you to rejoin Premium Signals** and get back into the rhythm.\n\n\n**Feel free to join us again through this link:** <https://whop.com/gold-pioneer>"
                            await member.send(dm_message)
                            AUTO_ROLE_CONFIG["dm_schedule"][member_id_str]["dm_7_sent"] = True
                            recovered_dms += 1
                            await self.log_to_discord(f"📤 Sent missed 7-day DM to {member.display_name}")
                        except discord.Forbidden:
                            await self.log_to_discord(f"❌ Could not send missed 7-day DM to {member.display_name} (DMs disabled)")

                    # Send missed 14-day DM
                    if not dm_data["dm_14_sent"] and current_time >= dm_14_time:
                        try:
                            dm_message = "Hey! It's been two weeks since your access to Premium Signals ended. We hope you've stayed active. \n\nIf you've been trading solo or passively following the free channel, you might be feeling the difference. in <#1350929852299214999>, it's not just about more signals. It's about the **structure, support, and smarter decision-making**. That edge can make all the difference over time.\n\nWe'd love to **officially invite you back into Premium Signals** and help you start compounding results again.\n\n**Feel free to join us again through this link:** <https://whop.com/gold-pioneer>"
                            await member.send(dm_message)
                            AUTO_ROLE_CONFIG["dm_schedule"][member_id_str]["dm_14_sent"] = True
                            recovered_dms += 1
                            await self.log_to_discord(f"📤 Sent missed 14-day DM to {member.display_name}")
                        except discord.Forbidden:
                            await self.log_to_discord(f"❌ Could not send missed 14-day DM to {member.display_name} (DMs disabled)")

                except Exception as e:
                    await self.log_to_discord(f"❌ Error processing missed DM for member {member_id_str}: {str(e)}")
                    continue

            # Save updated DM schedule
            await self.save_auto_role_config()

            if recovered_dms > 0:
                await self.log_to_discord(f"📬 Successfully sent {recovered_dms} missed DM reminders!")
            else:
                await self.log_to_discord("✅ No missed DM reminders found")

        except Exception as e:
            await self.log_to_discord(f"❌ Error during offline DM recovery: {str(e)}")
            print(f"Offline DM recovery error: {e}")

    async def recover_missed_signals(self):
        """Check for trading signals that were sent while bot was offline"""
        if not PRICE_TRACKING_CONFIG["enabled"]:
            return

        try:
            await self.log_to_discord("🔍 Scanning for missed trading signals while offline...")

            # Get the last known online time  
            offline_check_time = self.last_online_time
            if not offline_check_time:
                # If we don't know when we were last online, check last 6 hours as safety measure
                offline_check_time = datetime.now(AMSTERDAM_TZ) - timedelta(hours=6)

            recovered_signals = 0

            for guild in self.guilds:
                if not guild:
                    continue

                for channel in guild.text_channels:
                    # Skip excluded channel
                    if str(channel.id) == PRICE_TRACKING_CONFIG["excluded_channel_id"]:
                        continue

                    try:
                        # Check messages sent while bot was offline
                        async for message in channel.history(after=offline_check_time, limit=100):
                            # Only process signals from owner or bot
                            if not (str(message.author.id) == PRICE_TRACKING_CONFIG["owner_user_id"] or message.author.bot):
                                continue

                            # Check if message contains trading signal
                            if PRICE_TRACKING_CONFIG["signal_keyword"] not in message.content:
                                continue

                            # Skip if already being tracked (check both memory and database)
                            current_trades = await self.get_active_trades_from_db()
                            if str(message.id) in current_trades:
                                continue

                            # Parse the signal
                            trade_data = self.parse_signal_message(message.content)
                            if not trade_data:
                                continue

                            # Get historical price at the time the message was sent
                            message_time = message.created_at.astimezone(AMSTERDAM_TZ)
                            historical_price = await self.get_historical_price(trade_data["pair"], message_time)

                            if historical_price:
                                # Calculate tracking levels based on historical price
                                live_levels = self.calculate_live_tracking_levels(
                                    historical_price, trade_data["pair"], trade_data["action"]
                                )

                                # Store both Discord prices (for reference) and live prices (for tracking)
                                trade_data["discord_entry"] = trade_data["entry"]
                                trade_data["discord_tp1"] = trade_data["tp1"]
                                trade_data["discord_tp2"] = trade_data["tp2"]
                                trade_data["discord_tp3"] = trade_data["tp3"]
                                trade_data["discord_sl"] = trade_data["sl"]

                                # Override with historical-price-based levels
                                trade_data["live_entry"] = historical_price
                                trade_data["entry"] = live_levels["entry"]
                                trade_data["tp1"] = live_levels["tp1"]
                                trade_data["tp2"] = live_levels["tp2"]
                                trade_data["tp3"] = live_levels["tp3"]
                                trade_data["sl"] = live_levels["sl"]

                                # Add metadata
                                trade_data["channel_id"] = channel.id
                                trade_data["message_id"] = str(message.id)
                                trade_data["timestamp"] = message.created_at.isoformat()
                                trade_data["recovered"] = True  # Mark as recovered signal

                                # Add to active tracking with database persistence
                                await self.save_trade_to_db(str(message.id), trade_data)
                                recovered_signals += 1

                                print(f"✅ Recovered signal: {trade_data['pair']} from {message_time.strftime('%Y-%m-%d %H:%M')}")
                            else:
                                print(f"⚠️ Could not get historical price for {trade_data['pair']} - skipping recovery")

                    except Exception as e:
                        print(f"❌ Error scanning {channel.name} for missed signals: {e}")
                        continue

            if recovered_signals > 0:
                await self.log_to_discord(f"🔄 **Signal Recovery Complete**\n"
                                          f"Found and started tracking {recovered_signals} missed trading signals")
            else:
                await self.log_to_discord("✅ No missed trading signals found during downtime")

        except Exception as e:
            await self.log_to_discord(f"❌ Error during missed signal recovery: {str(e)}")
            print(f"Missed signal recovery error: {e}")

    async def check_offline_tp_sl_hits(self):
        """Check for TP/SL hits that occurred while bot was offline"""
        if not PRICE_TRACKING_CONFIG["enabled"]:
            return

        try:
            # Load active trades from database first
            await self.load_active_trades_from_db()
            active_trades = PRICE_TRACKING_CONFIG["active_trades"]

            if not active_trades:
                return

            await self.log_to_discord("🔍 Checking for TP/SL hits that occurred while offline...")

            offline_hits_found = 0

            for message_id, trade_data in list(active_trades.items()):
                try:
                    # Get current price to check if any levels were hit
                    current_price = await self.get_live_price(trade_data["pair"], use_all_apis=True)
                    if current_price is None:
                        continue

                    # Check if message still exists (cleanup deleted signals)
                    if not await self.check_message_still_exists(message_id, trade_data):
                        await self.remove_trade_from_db(message_id)
                        continue

                    action = trade_data["action"]
                    entry = trade_data["entry"]
                    tp_hits = trade_data.get('tp_hits', [])

                    # Check for SL hit while offline
                    if action == "BUY" and current_price <= trade_data["sl"]:
                        await self.handle_sl_hit(message_id, trade_data, offline_hit=True)
                        offline_hits_found += 1
                        continue
                    elif action == "SELL" and current_price >= trade_data["sl"]:
                        await self.handle_sl_hit(message_id, trade_data, offline_hit=True)
                        offline_hits_found += 1
                        continue

                    # Check for TP hits while offline (TP3 -> TP2 -> TP1 priority)
                    if action == "BUY":
                        if "tp3" not in tp_hits and current_price >= trade_data["tp3"]:
                            await self.handle_tp_hit(message_id, trade_data, "tp3", offline_hit=True)
                            offline_hits_found += 1
                            continue
                        elif "tp2" not in tp_hits and current_price >= trade_data["tp2"]:
                            await self.handle_tp_hit(message_id, trade_data, "tp2", offline_hit=True)
                            offline_hits_found += 1
                            continue
                        elif "tp1" not in tp_hits and current_price >= trade_data["tp1"]:
                            await self.handle_tp_hit(message_id, trade_data, "tp1", offline_hit=True)
                            offline_hits_found += 1
                            continue
                    elif action == "SELL":
                        if "tp3" not in tp_hits and current_price <= trade_data["tp3"]:
                            await self.handle_tp_hit(message_id, trade_data, "tp3", offline_hit=True)
                            offline_hits_found += 1
                            continue
                        elif "tp2" not in tp_hits and current_price <= trade_data["tp2"]:
                            await self.handle_tp_hit(message_id, trade_data, "tp2", offline_hit=True)
                            offline_hits_found += 1
                            continue
                        elif "tp1" not in tp_hits and current_price <= trade_data["tp1"]:
                            await self.handle_tp_hit(message_id, trade_data, "tp1", offline_hit=True)
                            offline_hits_found += 1
                            continue

                    # Check for breakeven hits if TP2 was already hit
                    if trade_data.get("breakeven_active"):
                        if action == "BUY" and current_price <= entry:
                            await self.handle_breakeven_hit(message_id, trade_data, offline_hit=True)
                            offline_hits_found += 1
                            continue
                        elif action == "SELL" and current_price >= entry:
                            await self.handle_breakeven_hit(message_id, trade_data, offline_hit=True)
                            offline_hits_found += 1
                            continue

                except Exception as e:
                    continue

            if offline_hits_found > 0:
                await self.log_to_discord(f"⚡ Found and processed {offline_hits_found} TP/SL hits that occurred while offline")
            else:
                await self.log_to_discord("✅ No offline TP/SL hits detected")

        except Exception as e:
            await self.log_to_discord(f"❌ Error checking offline TP/SL hits: {str(e)}")
            print(f"Offline TP/SL check error: {e}")

    async def get_historical_price(self, pair: str, timestamp: datetime) -> Optional[float]:
        """Get historical price for a trading pair at a specific timestamp"""
        try:
            # For now, use current price as fallback (historical prices require different APIs)
            # This could be enhanced with time-series APIs in the future
            current_price = await self.get_live_price(pair)
            if current_price:
                return current_price
            return None
        except Exception as e:
            return None


    @tasks.loop(seconds=225)  # Check every 3 minutes 45 seconds (3 min + 45s safety buffer) for optimal API usage
    async def price_tracking_task(self):
        """Background task to monitor live prices for active trades - optimized for free API tiers"""
        if not PRICE_TRACKING_CONFIG["enabled"]:
            return

        # Get active trades from database for 24/7 persistence
        active_trades = await self.get_active_trades_from_db()
        if not active_trades:
            return

        try:
            # Check each active trade
            trades_to_remove = []
            for message_id, trade_data in list(active_trades.items()):
                try:
                    # Check if price levels have been hit
                    level_hit = await self.check_price_levels(message_id, trade_data)
                    if level_hit:
                        # Trade was closed, will be removed by the handler
                        continue

                except Exception as e:
                    trades_to_remove.append(message_id)

            # Remove failed trades from database
            for message_id in trades_to_remove:
                await self.remove_trade_from_db(message_id)

        except Exception as e:
            pass

    @tasks.loop(minutes=30)
    async def heartbeat_task(self):
        """Periodic heartbeat to track bot uptime and save status"""
        if self.db_pool:
            try:
                await self.save_bot_status()
                self.last_heartbeat = datetime.now(AMSTERDAM_TZ)
            except Exception as e:
                print(f"Heartbeat error: {e}")

    async def init_database(self):
        """Initialize database connection and create tables"""
        try:
            # Try multiple possible database environment variables (Render uses different names)
            database_url = os.getenv('DATABASE_URL') or os.getenv(
                'POSTGRES_URL') or os.getenv('POSTGRESQL_URL')

            if not database_url:
                print(
                    "❌ No database URL found - continuing without persistent memory"
                )
                print("   To enable persistent memory on Render:")
                print("   1. Add a PostgreSQL service to your Render account")
                print("   2. Set DATABASE_URL environment variable")
                return

            # Create connection pool with Render-optimized settings
            self.db_pool = await asyncpg.create_pool(
                database_url,
                min_size=1,
                max_size=5,  # Lower for Render's limits
                command_timeout=30,
                server_settings={'application_name': 'discord-trading-bot'})
            print("✅ PostgreSQL connection pool created for persistent memory")

            # Create tables
            async with self.db_pool.acquire() as conn:
                # Role history table for anti-abuse system
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS role_history (
                        member_id BIGINT PRIMARY KEY,
                        first_granted TIMESTAMP WITH TIME ZONE NOT NULL,
                        times_granted INTEGER DEFAULT 1,
                        last_expired TIMESTAMP WITH TIME ZONE,
                        guild_id BIGINT NOT NULL
                    )
                ''')

                # Active members table for current role holders
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS active_members (
                        member_id BIGINT PRIMARY KEY,
                        role_added_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        role_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL,
                        weekend_delayed BOOLEAN DEFAULT FALSE,
                        expiry_time TIMESTAMP WITH TIME ZONE,
                        custom_duration BOOLEAN DEFAULT FALSE
                    )
                ''')

                # Weekend pending table
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS weekend_pending (
                        member_id BIGINT PRIMARY KEY,
                        join_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        guild_id BIGINT NOT NULL
                    )
                ''')

                # DM schedule table for follow-up campaigns
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS dm_schedule (
                        member_id BIGINT PRIMARY KEY,
                        role_expired TIMESTAMP WITH TIME ZONE NOT NULL,
                        guild_id BIGINT NOT NULL,
                        dm_3_sent BOOLEAN DEFAULT FALSE,
                        dm_7_sent BOOLEAN DEFAULT FALSE,
                        dm_14_sent BOOLEAN DEFAULT FALSE
                    )
                ''')

                # Auto-role config table for bot settings
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS auto_role_config (
                        id SERIAL PRIMARY KEY,
                        enabled BOOLEAN DEFAULT FALSE,
                        role_id BIGINT,
                        duration_hours INTEGER DEFAULT 24,
                        custom_message TEXT
                    )
                ''')

                # Price tracking config table for persistent 24/7 monitoring
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS price_tracking_config (
                        id SERIAL PRIMARY KEY,
                        enabled BOOLEAN DEFAULT TRUE,
                        check_interval INTEGER DEFAULT 225,
                        last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                ''')

                # Bot status table for offline recovery
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS bot_status (
                        id INTEGER PRIMARY KEY DEFAULT 1,
                        last_online TIMESTAMP WITH TIME ZONE,
                        heartbeat_time TIMESTAMP WITH TIME ZONE,
                        CONSTRAINT single_row_constraint UNIQUE (id)
                    )
                ''')

                # User levels table for level system
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS user_levels (
                        user_id BIGINT PRIMARY KEY,
                        message_count INTEGER DEFAULT 0,
                        current_level INTEGER DEFAULT 0,
                        guild_id BIGINT NOT NULL
                    )
                ''')

                # Invite tracking table
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS invite_tracking (
                        invite_code VARCHAR(20) PRIMARY KEY,
                        guild_id BIGINT NOT NULL,
                        creator_id BIGINT NOT NULL,
                        nickname VARCHAR(255),
                        total_joins INTEGER DEFAULT 0,
                        total_left INTEGER DEFAULT 0,
                        current_members INTEGER DEFAULT 0,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                ''')

                # Member join tracking via invites
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS member_joins (
                        id SERIAL PRIMARY KEY,
                        member_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL,
                        invite_code VARCHAR(20),
                        joined_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        left_at TIMESTAMP WITH TIME ZONE NULL,
                        is_currently_member BOOLEAN DEFAULT TRUE
                    )
                ''')

                # Active trading signals table for 24/7 persistent tracking
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS active_trades (
                        message_id VARCHAR(20) PRIMARY KEY,
                        channel_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL,
                        pair VARCHAR(20) NOT NULL,
                        action VARCHAR(10) NOT NULL,
                        entry_price DECIMAL(12,8) NOT NULL,
                        tp1_price DECIMAL(12,8) NOT NULL,
                        tp2_price DECIMAL(12,8) NOT NULL,
                        tp3_price DECIMAL(12,8) NOT NULL,
                        sl_price DECIMAL(12,8) NOT NULL,
                        discord_entry DECIMAL(12,8),
                        discord_tp1 DECIMAL(12,8),
                        discord_tp2 DECIMAL(12,8),
                        discord_tp3 DECIMAL(12,8),
                        discord_sl DECIMAL(12,8),
                        live_entry DECIMAL(12,8),
                        status VARCHAR(50) DEFAULT 'active',
                        tp_hits TEXT DEFAULT '',
                        breakeven_active BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                ''')

            print("✅ Database tables initialized")

            # Load existing config from database
            await self.load_config_from_db()

            # Load bot status for offline recovery
            await self.load_bot_status()

            # Load level system data
            await self.load_level_system()

            # Load invite tracking data
            await self.load_invite_tracking()

            # Load active trades from database for 24/7 persistence
            await self.load_active_trades_from_db()

            # Load price tracking configuration for 24/7 monitoring persistence
            await self.load_price_tracking_config()

        except Exception as e:
            print(f"❌ Database initialization failed: {e}")
            print(
                "   Continuing with in-memory storage only (data will be lost on restart)"
            )
            print("   To fix this on Render:")
            print("   1. Add PostgreSQL service in Render dashboard")
            print("   2. Connect it to your web service")
            print("   3. Restart the service")
            self.db_pool = None

    async def setup_hook(self):
        # Initialize aiohttp client session (fixes unclosed client session errors)
        self.client_session = aiohttp.ClientSession()

        # Record bot startup time for offline recovery
        self.last_online_time = datetime.now(AMSTERDAM_TZ)

        # Sync slash commands with retry mechanism for better reliability
        max_retries = 3
        for attempt in range(max_retries):
            try:
                synced = await self.tree.sync()
                print(
                    f"✅ Successfully synced {len(synced)} command(s) on attempt {attempt + 1}"
                )
                break
            except Exception as e:
                print(
                    f"❌ Failed to sync commands on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(5)  # Wait 5 seconds before retry
                else:
                    print(
                        "⚠️ All sync attempts failed. Commands may not be available."
                    )

        # Initialize invite cache for bot invite detection
        self._cached_invites = {}

        # Backtrack existing server invites for tracking
        await self.backtrack_existing_invites()

        # Force sync after bot is ready for better reliability
        self.first_sync_done = False

        # Initialize database
        await self.init_database()

        # Set up Discord logging channels
        self.log_channel = self.get_channel(LOG_CHANNEL_ID)
        self.price_debug_channel = self.get_channel(PRICE_DEBUG_CHANNEL_ID)
        if not self.log_channel:
            print(f"⚠️ Main log channel {LOG_CHANNEL_ID} not found")
        if not self.price_debug_channel:
            print(f"⚠️ Price debug channel {PRICE_DEBUG_CHANNEL_ID} not found")

    async def backtrack_existing_invites(self):
        """Backtrack and start monitoring all existing server invites"""
        try:
            backtracked_count = 0

            for guild in self.guilds:
                try:
                    # Fetch all current invites for this guild
                    current_invites = await guild.invites()

                    # Cache invites for this guild
                    self._cached_invites[guild.id] = current_invites

                    # Add any uncached invites to tracking system
                    for invite in current_invites:
                        if invite.code not in INVITE_TRACKING:
                            # Initialize tracking for this existing invite
                            INVITE_TRACKING[invite.code] = {
                                "nickname": f"Pre-existing-{invite.code[:8]}",
                                "creator_id": invite.inviter.id if invite.inviter else 0,
                                "total_joins": invite.uses or 0,  # Start with current usage
                                "total_left": 0,  # Can't backtrack left members
                                "current_members": invite.uses or 0,  # Assume all are still here
                                "guild_id": guild.id,
                                "created_at": invite.created_at.isoformat() if invite.created_at else None,
                                "max_uses": invite.max_uses,
                                "temporary": invite.temporary,
                                "backtracked": True  # Mark as backtracked
                            }
                            backtracked_count += 1

                except Exception as e:
                    print(f"❌ Error backtracking invites for guild {guild.name}: {e}")
                    continue

            if backtracked_count > 0:
                print(f"✅ Backtracked {backtracked_count} existing server invites for monitoring")
                await self.log_to_discord(f"🔄 **Invite Backtracking Complete**\n"
                                          f"Started monitoring {backtracked_count} existing server invites")
            else:
                print("📋 No new invites found to backtrack")

        except Exception as e:
            print(f"❌ Error during invite backtracking: {e}")

    async def on_ready(self):
        """Called when bot is ready and connected to Discord"""
        print("🎉 DISCORD BOT READY EVENT TRIGGERED!")
        print(f"   Bot user: {self.user}")
        print(f"   Bot ID: {self.user.id if self.user else 'None'}")
        print(f"   Bot discriminator: {self.user.discriminator if self.user else 'None'}")
        print(f"   Connected guilds: {len(self.guilds)}")
        for guild in self.guilds:
            print(f"     - {guild.name} (ID: {guild.id})")
        print(f"   Latency: {round(self.latency * 1000)}ms")
        print(f"   Is ready: {self.is_ready()}")
        print(f"   Is closed: {self.is_closed()}")

        # Force command sync on ready for better reliability
        if not getattr(self, 'first_sync_done', False):
            try:
                synced = await self.tree.sync()
                print(f"🔄 Force synced {len(synced)} command(s) on ready")
                self.first_sync_done = True

                # Command permissions are enforced by owner_check() in each command
                if BOT_OWNER_USER_ID:
                    print(f"🔒 Bot commands restricted to owner ID: {BOT_OWNER_USER_ID}")
                else:
                    print("⚠️ BOT_OWNER_USER_ID not set - all commands blocked for security")
            except Exception as e:
                print(f"⚠️ Force sync on ready failed: {e}")

        # Start the role removal task
        if not self.role_removal_task.is_running():
            self.role_removal_task.start()

        # Start the Monday activation notification task
        if not self.weekend_activation_task.is_running():
            self.weekend_activation_task.start()

        # Start the Monday activation task for weekend joiners
        if not self.monday_activation_task.is_running():
            self.monday_activation_task.start()

        # Start the follow-up DM task
        if not self.followup_dm_task.is_running():
            self.followup_dm_task.start()

        # Start the price tracking task
        if not self.price_tracking_task.is_running():
            self.price_tracking_task.start()

        # Check for TP/SL hits that occurred while offline
        await self.check_offline_tp_sl_hits()

        # Database initialization is now handled in setup_hook

        # Set up Discord logging channel
        if self.log_channel:
            await self.log_to_discord(
                "🚀 **TradingBot Started** - All systems operational!")
        else:
            print(f"⚠️ Log channel {LOG_CHANNEL_ID} not found")

        # Cache invites for all guilds to track bot invite usage
        for guild in self.guilds:
            try:
                self._cached_invites[guild.id] = await guild.invites()
                await self.log_to_discord(
                    f"✅ Cached {len(self._cached_invites[guild.id])} invites for {guild.name}")
            except discord.Forbidden:
                await self.log_to_discord(
                    f"⚠️ No permission to fetch invites for {guild.name}")
            except Exception as e:
                await self.log_to_discord(
                    f"❌ Error caching invites for {guild.name}: {e}")


        # Check for offline members who joined while bot was offline
        await self.recover_offline_members()

        # Check for missed DM reminders while bot was offline
        await self.recover_offline_dm_reminders()

        # Check for missed trading signals while bot was offline
        await self.recover_missed_signals()

        # Update bot status and start heartbeat
        if self.db_pool:
            await self.save_bot_status()
            if not hasattr(self, 'heartbeat_task_started'):
                self.heartbeat_task.start()
                self.heartbeat_task_started = True

        # Ensure owner permissions are set up after bot is ready
        await self.setup_owner_permissions()

    async def on_connect(self):
        """Called when bot connects to Discord"""
        print("🔗 DISCORD CONNECTION ESTABLISHED!")
        print(f"   Connected as: {self.user}")
        print(f"   Connection time: {datetime.now()}")
        print(f"   Latency: {round(self.latency * 1000)}ms")

    async def on_disconnect(self):
        """Called when bot disconnects from Discord"""
        print("🔌 DISCORD CONNECTION LOST!")
        print(f"   Disconnection time: {datetime.now()}")

    async def on_resumed(self):
        """Called when bot resumes connection to Discord"""
        print("🔄 DISCORD CONNECTION RESUMED!")
        print(f"   Resume time: {datetime.now()}")

    async def on_error(self, event, *args, **kwargs):
        """Called when an error occurs"""
        print(f"❌ DISCORD BOT ERROR in event '{event}':")
        import traceback
        traceback.print_exc()

        # Try to log to Discord channel if available
        if self.log_channel:
            try:
                await self.log_channel.send(f"❌ Bot Error in event '{event}': {str(args[0]) if args else 'Unknown error'}")
            except Exception:
                pass

    def is_weekend_time(self, dt=None):
        """Check if the given datetime (or now) falls within weekend trading closure"""
        if dt is None:
            dt = datetime.now(AMSTERDAM_TZ)
        else:
            dt = dt.astimezone(AMSTERDAM_TZ)

        # Weekend period: Friday 12:00 to Sunday 23:59 (Amsterdam time)
        weekday = dt.weekday()  # Monday=0, Sunday=6
        hour = dt.hour

        if weekday == 4 and hour >= 12:  # Friday from 12:00
            return True
        elif weekday == 5 or weekday == 6:  # Saturday or Sunday
            return True
        else:
            return False

    def get_next_monday_activation_time(self):
        """Get the next Monday 00:01 Amsterdam time (when 24h countdown starts)"""
        now = datetime.now(AMSTERDAM_TZ)

        # Find next Monday
        days_ahead = 0 - now.weekday()  # Monday is 0
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7

        next_monday = now + timedelta(days=days_ahead)

        if PYTZ_AVAILABLE:
            activation_time = AMSTERDAM_TZ.localize(
                next_monday.replace(hour=0,
                                    minute=1,
                                    second=0,
                                    microsecond=0,
                                    tzinfo=None))
        else:
            activation_time = next_monday.replace(hour=0,
                                                  minute=1,
                                                  second=0,
                                                  microsecond=0,
                                                  tzinfo=AMSTERDAM_TZ)

        return activation_time

    def get_monday_expiry_time(self, join_time):
        """Get the Monday 23:59 Amsterdam time (when weekend joiners' role expires)"""
        now = join_time if join_time else datetime.now(AMSTERDAM_TZ)

        if PYTZ_AVAILABLE:
            if now.tzinfo is None:
                now = AMSTERDAM_TZ.localize(now)
            else:
                now = now.astimezone(AMSTERDAM_TZ)
        else:
            if now.tzinfo is None:
                now = now.replace(tzinfo=AMSTERDAM_TZ)
            else:
                now = now.astimezone(AMSTERDAM_TZ)

        # Find next Monday
        days_ahead = 0 - now.weekday()  # Monday is 0
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7

        next_monday = now + timedelta(days=days_ahead)

        if PYTZ_AVAILABLE:
            expiry_time = AMSTERDAM_TZ.localize(
                next_monday.replace(hour=23,
                                    minute=59,
                                    second=59,
                                    microsecond=0,
                                    tzinfo=None))
        else:
            expiry_time = next_monday.replace(hour=23,
                                              minute=59,
                                              second=59,
                                              microsecond=0,
                                              tzinfo=AMSTERDAM_TZ)

        return expiry_time

    async def on_member_join(self, member):
        """Handle new member joins and assign auto-role if enabled"""
        if not AUTO_ROLE_CONFIG["enabled"] or not AUTO_ROLE_CONFIG["role_id"]:
            return

        try:
            # Check if member joined through a bot invite
            invites_before = getattr(self, '_cached_invites',
                                     {}).get(member.guild.id, [])
            invites_after = await member.guild.invites()

            # Cache current invites for next comparison
            if not hasattr(self, '_cached_invites'):
                self._cached_invites = {}
            self._cached_invites[member.guild.id] = invites_after

            # Find which invite was used by comparing use counts
            used_invite = None
            for invite_after in invites_after:
                for invite_before in invites_before:
                    if (invite_after.code == invite_before.code
                            and invite_after.uses > invite_before.uses):
                        used_invite = invite_after
                        break
                if used_invite:
                    break

            # Track the invite usage if we found one
            if used_invite:
                # Track the member join via this specific invite
                await self.track_member_join_via_invite(member, used_invite.code)

                # Initialize tracking for this invite if not already tracked
                if used_invite.code not in INVITE_TRACKING:
                    INVITE_TRACKING[used_invite.code] = {
                        "nickname": f"Invite-{used_invite.code[:8]}",  # Default nickname
                        "total_joins": 0,
                        "total_left": 0,
                        "current_members": 0,
                        "creator_id": used_invite.inviter.id if used_invite.inviter else 0,
                        "guild_id": member.guild.id,
                        "created_at": datetime.now(AMSTERDAM_TZ).isoformat(),
                        "last_updated": datetime.now(AMSTERDAM_TZ).isoformat()
                    }

            # If we found the invite and it was created by a bot, ignore this member
            if used_invite and used_invite.inviter and used_invite.inviter.bot:
                await self.log_to_discord(
                    f"🤖 Ignoring {member.display_name} - joined via bot invite from {used_invite.inviter.display_name}"
                )
                return

            role = member.guild.get_role(AUTO_ROLE_CONFIG["role_id"])
            if not role:
                await self.log_to_discord(
                    f"❌ Auto-role not found in guild {member.guild.name}")
                return

            # Enhanced anti-abuse system checks
            member_id_str = str(member.id)

            # Check if user has already received the role before
            if member_id_str in AUTO_ROLE_CONFIG["role_history"]:
                await self.log_to_discord(
                    f"🚫 {member.display_name} has already received auto-role before - access denied (anti-abuse)"
                )
                return

            # Account age check removed - allow new Discord users to join from influencer collabs

            # Rapid join pattern detection removed - allow unlimited joins during influencer collabs

            join_time = datetime.now(AMSTERDAM_TZ)

            # Add the role immediately for all members
            await member.add_roles(role, reason="Auto-role for new member")

            # Check if it's weekend time to determine countdown behavior
            if self.is_weekend_time(join_time):
                # Weekend join - expires Monday 23:59
                monday_expiry = self.get_monday_expiry_time(join_time)

                AUTO_ROLE_CONFIG["active_members"][member_id_str] = {
                    "role_added_time": join_time.isoformat(),
                    "role_id": AUTO_ROLE_CONFIG["role_id"],
                    "guild_id": member.guild.id,
                    "weekend_delayed": True,
                    "expiry_time": monday_expiry.isoformat()
                }

                # Record in role history for anti-abuse
                AUTO_ROLE_CONFIG["role_history"][member_id_str] = {
                    "first_granted": join_time.isoformat(),
                    "times_granted": 1,
                    "last_expired": None,
                    "guild_id": member.guild.id
                }

                # Send weekend notification DM
                try:
                    weekend_message = (
                        "**Welcome to FX Pip Pioneers!** As a welcome gift, we usually give our new members "
                        "**access to the Premium Signals channel for 24 hours.** However, the trading markets are currently closed for the weekend. "
                        "We're Messaging you to let you know that your 24 hours of access to <#1384668129036075109> will start counting down from "
                        "the moment the markets open again on Monday. This way, your welcome gift won't be wasted on the weekend "
                        "and you'll actually be able to make use of it.")
                    await member.send(weekend_message)
                    await self.log_to_discord(
                        f"✅ Sent weekend notification DM to {member.display_name}"
                    )
                except discord.Forbidden:
                    await self.log_to_discord(
                        f"⚠️ Could not send weekend notification DM to {member.display_name} (DMs disabled)"
                    )
                except Exception as e:
                    await self.log_to_discord(
                        f"❌ Error sending weekend notification DM to {member.display_name}: {str(e)}"
                    )

                await self.log_to_discord(
                    f"✅ Auto-role '{role.name}' added to {member.display_name} (expires Monday 23:59)"
                )

            else:
                # Normal join - immediate 24-hour countdown
                AUTO_ROLE_CONFIG["active_members"][member_id_str] = {
                    "role_added_time": join_time.isoformat(),
                    "role_id": AUTO_ROLE_CONFIG["role_id"],
                    "guild_id": member.guild.id,
                    "weekend_delayed": False
                }

                # Record in role history for anti-abuse
                AUTO_ROLE_CONFIG["role_history"][member_id_str] = {
                    "first_granted": join_time.isoformat(),
                    "times_granted": 1,
                    "last_expired": None,
                    "guild_id": member.guild.id
                }

                # Send weekday welcome DM
                try:
                    weekday_message = (
                        ">:star2: **Welcome to FX Pip Pioneers!** :star2:<br>"
                        ":white_check_mark: As a welcome gift, we've given you access to our **Premium Signals channel for 24 hours.** "
                        "That means you can start profiting from the **8–10 trade signals** we send per day right now!<br><br>"
                        "***This is your shot at consistency, clarity, and growth in trading. Let's level up together!***"
                    )
                    await member.send(weekday_message)
                    await self.log_to_discord(
                        f"✅ Sent weekday welcome DM to {member.display_name}")
                except discord.Forbidden:
                    await self.log_to_discord(
                        f"⚠️ Could not send weekday welcome DM to {member.display_name} (DMs disabled)"
                    )
                except Exception as e:
                    await self.log_to_discord(
                        f"❌ Error sending weekday welcome DM to {member.display_name}: {str(e)}"
                    )

                await self.log_to_discord(
                    f"✅ Auto-role '{role.name}' added to {member.display_name} (24h countdown starts now)"
                )

            # Save the updated config
            await self.save_auto_role_config()

        except discord.Forbidden:
            await self.log_to_discord(
                f"❌ No permission to assign role to {member.display_name}")
        except Exception as e:
            await self.log_to_discord(
                f"❌ Error assigning auto-role to {member.display_name}: {str(e)}")

    async def on_member_remove(self, member):
        """Handle member leaving the server"""
        try:
            # Track the member leaving for invite statistics
            await self.track_member_leave(member)
            await self.log_to_discord(f"👋 {member.display_name} left the server - invite statistics updated")
        except Exception as e:
            await self.log_to_discord(f"❌ Error tracking member leave for {member.display_name}: {str(e)}")

    async def on_message(self, message):
        """Handle messages for level system and price tracking"""
        # Check for trading signals (only from owner or bot)
        if (PRICE_TRACKING_CONFIG["enabled"] and 
            str(message.channel.id) != PRICE_TRACKING_CONFIG["excluded_channel_id"] and
            (str(message.author.id) == PRICE_TRACKING_CONFIG["owner_user_id"] or message.author.bot) and
            PRICE_TRACKING_CONFIG["signal_keyword"] in message.content):

            try:
                # Parse the signal message
                trade_data = self.parse_signal_message(message.content)
                if trade_data:
                    # Get live price at the moment of signal using all APIs for accuracy
                    live_price = await self.get_live_price(trade_data["pair"], use_all_apis=True)

                    if live_price:
                        # Calculate live-price-based TP/SL levels for tracking
                        live_levels = self.calculate_live_tracking_levels(
                            live_price, trade_data["pair"], trade_data["action"]
                        )

                        # Store both Discord prices (for reference) and live prices (for tracking)
                        trade_data["discord_entry"] = trade_data["entry"]
                        trade_data["discord_tp1"] = trade_data["tp1"] 
                        trade_data["discord_tp2"] = trade_data["tp2"]
                        trade_data["discord_tp3"] = trade_data["tp3"]
                        trade_data["discord_sl"] = trade_data["sl"]

                        # Override with live-price-based levels for tracking
                        trade_data["live_entry"] = live_price
                        trade_data["entry"] = live_levels["entry"]
                        trade_data["tp1"] = live_levels["tp1"]
                        trade_data["tp2"] = live_levels["tp2"] 
                        trade_data["tp3"] = live_levels["tp3"]
                        trade_data["sl"] = live_levels["sl"]

                        pass
                    else:
                        pass

                    # Add channel and message info
                    trade_data["channel_id"] = message.channel.id
                    trade_data["message_id"] = str(message.id)
                    trade_data["timestamp"] = message.created_at.isoformat()

                    # Add to active trades with database persistence
                    await self.save_trade_to_db(str(message.id), trade_data)

                    await self.log_price_debug(f"✅ Started tracking {trade_data['pair']} {trade_data['action']} signal - Live price: ${live_price:.5f}")
                else:
                    await self.log_price_debug(f"⚠️ Could not get live price for {trade_data['pair']} - using Discord prices for tracking")
            except Exception as e:
                pass

        # Process message for level system
        await self.process_message_for_levels(message)

    async def save_auto_role_config(self):
        """Save auto-role configuration to database"""
        if not self.db_pool:
            return  # No database available

        try:
            async with self.db_pool.acquire() as conn:
                # Save main config - use upsert with a fixed ID
                await conn.execute(
                    '''
                    INSERT INTO auto_role_config (id, enabled, role_id, duration_hours, custom_message)
                    VALUES (1, $1, $2, $3, $4)
                    ON CONFLICT (id) DO UPDATE SET
                        enabled = $1,
                        role_id = $2, 
                        duration_hours = $3,
                        custom_message = $4
                ''', AUTO_ROLE_CONFIG["enabled"], AUTO_ROLE_CONFIG["role_id"],
                    AUTO_ROLE_CONFIG["duration_hours"],
                    AUTO_ROLE_CONFIG["custom_message"])

                # Save active members
                await conn.execute('DELETE FROM active_members')
                for member_id, data in AUTO_ROLE_CONFIG[
                        "active_members"].items():
                    expiry_time = None
                    if data.get("expiry_time"):
                        expiry_time = datetime.fromisoformat(
                            data["expiry_time"].replace('Z', '+00:00'))

                    await conn.execute(
                        '''
                        INSERT INTO active_members 
                        (member_id, role_added_time, role_id, guild_id, weekend_delayed, expiry_time, custom_duration)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ''', int(member_id),
                        datetime.fromisoformat(data["role_added_time"].replace(
                            'Z', '+00:00')), data["role_id"], data["guild_id"],
                        data["weekend_delayed"], expiry_time,
                        data.get("custom_duration", False))

                # Save weekend pending
                await conn.execute('DELETE FROM weekend_pending')
                for member_id, data in AUTO_ROLE_CONFIG[
                        "weekend_pending"].items():
                    await conn.execute(
                        '''
                        INSERT INTO weekend_pending (member_id, join_time, guild_id)
                        VALUES ($1, $2, $3)
                    ''', int(member_id),
                        datetime.fromisoformat(data["join_time"].replace(
                            'Z', '+00:00')), data["guild_id"])

                # Save role history using UPSERT to avoid conflicts
                for member_id, data in AUTO_ROLE_CONFIG["role_history"].items(
                ):
                    last_expired = None
                    if data.get("last_expired"):
                        last_expired = datetime.fromisoformat(
                            data["last_expired"].replace('Z', '+00:00'))

                    await conn.execute(
                        '''
                        INSERT INTO role_history (member_id, first_granted, times_granted, last_expired, guild_id)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (member_id) DO UPDATE SET
                            first_granted = $2,
                            times_granted = $3,
                            last_expired = $4,
                            guild_id = $5
                    ''', int(member_id),
                        datetime.fromisoformat(data["first_granted"].replace(
                            'Z', '+00:00')), data["times_granted"],
                        last_expired, data["guild_id"])

                # Save DM schedule using UPSERT
                for member_id, data in AUTO_ROLE_CONFIG["dm_schedule"].items():
                    await conn.execute(
                        '''
                        INSERT INTO dm_schedule (member_id, role_expired, guild_id, dm_3_sent, dm_7_sent, dm_14_sent)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (member_id) DO UPDATE SET
                            role_expired = $2,
                            guild_id = $3,
                            dm_3_sent = $4,
                            dm_7_sent = $5,
                            dm_14_sent = $6
                    ''', int(member_id),
                        datetime.fromisoformat(data["role_expired"].replace(
                            'Z',
                            '+00:00')), data["guild_id"], data["dm_3_sent"],
                        data["dm_7_sent"], data["dm_14_sent"])

        except Exception as e:
            print(f"❌ Error saving to database: {str(e)}")

    # ===== LEVEL SYSTEM FUNCTIONS =====

    async def save_level_system(self):
        """Save level system data to database"""
        if not self.db_pool:
            return  # No database available

        try:
            async with self.db_pool.acquire() as conn:
                # Save user level data using UPSERT
                for user_id, data in LEVEL_SYSTEM["user_data"].items():
                    await conn.execute('''
                        INSERT INTO user_levels (user_id, message_count, current_level, guild_id)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (user_id) DO UPDATE SET
                            message_count = $2,
                            current_level = $3,
                            guild_id = $4
                    ''', int(user_id), data["message_count"], data["current_level"], data["guild_id"])

        except Exception as e:
            print(f"❌ Error saving level system to database: {str(e)}")

    async def load_level_system(self):
        """Load level system data from database"""
        if not self.db_pool:
            return  # No database available

        try:
            async with self.db_pool.acquire() as conn:
                # Load user level data
                rows = await conn.fetch('SELECT user_id, message_count, current_level, guild_id FROM user_levels')
                for row in rows:
                    LEVEL_SYSTEM["user_data"][str(row['user_id'])] = {
                        "message_count": row['message_count'],
                        "current_level": row['current_level'],
                        "guild_id": row['guild_id']
                    }

            if LEVEL_SYSTEM["user_data"]:
                print(f"✅ Loaded level data for {len(LEVEL_SYSTEM['user_data'])} users")
            else:
                print("📊 No existing level data found - starting fresh")

        except Exception as e:
            print(f"❌ Error loading level system from database: {str(e)}")

    async def load_invite_tracking(self):
        """Load invite tracking data from database"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                # Load invite tracking data
                rows = await conn.fetch('SELECT * FROM invite_tracking')
                for row in rows:
                    INVITE_TRACKING[row['invite_code']] = {
                        "nickname": row['nickname'],
                        "total_joins": row['total_joins'],
                        "total_left": row['total_left'],
                        "current_members": row['current_members'],
                        "creator_id": row['creator_id'],
                        "guild_id": row['guild_id'],
                        "created_at": row['created_at'].isoformat(),
                        "last_updated": row['last_updated'].isoformat()
                    }

                if INVITE_TRACKING:
                    print(f"✅ Loaded invite tracking data for {len(INVITE_TRACKING)} invites")
                else:
                    print("📋 No existing invite tracking data found - starting fresh")

        except Exception as e:
            print(f"❌ Error loading invite tracking from database: {str(e)}")

    async def load_active_trades_from_db(self):
        """Load active trading signals from database for 24/7 persistence"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                # Load all active trades from database
                rows = await conn.fetch('SELECT * FROM active_trades ORDER BY created_at DESC')

                for row in rows:
                    # Convert database row to trade_data format
                    trade_data = {
                        "pair": row['pair'],
                        "action": row['action'],
                        "entry": float(row['entry_price']),
                        "tp1": float(row['tp1_price']),
                        "tp2": float(row['tp2_price']),
                        "tp3": float(row['tp3_price']),
                        "sl": float(row['sl_price']),
                        "discord_entry": float(row['discord_entry']) if row['discord_entry'] else None,
                        "discord_tp1": float(row['discord_tp1']) if row['discord_tp1'] else None,
                        "discord_tp2": float(row['discord_tp2']) if row['discord_tp2'] else None,
                        "discord_tp3": float(row['discord_tp3']) if row['discord_tp3'] else None,
                        "discord_sl": float(row['discord_sl']) if row['discord_sl'] else None,
                        "live_entry": float(row['live_entry']) if row['live_entry'] else None,
                        "status": row['status'],
                        "tp_hits": row['tp_hits'].split(',') if row['tp_hits'] else [],
                        "breakeven_active": row['breakeven_active'],
                        "channel_id": row['channel_id'],
                        "guild_id": row['guild_id'],
                        "message_id": row['message_id'],
                        "created_at": row['created_at'].isoformat(),
                        "last_updated": row['last_updated'].isoformat()
                    }

                    # Store in memory for quick access
                    PRICE_TRACKING_CONFIG["active_trades"][row['message_id']] = trade_data

                pass

        except Exception as e:
            pass

    async def save_trade_to_db(self, message_id: str, trade_data: dict):
        """Save a new trading signal to database for persistence"""
        # Always save to memory for tracking (works with or without database)
        PRICE_TRACKING_CONFIG["active_trades"][message_id] = trade_data

        # Also save to database if available
        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.execute('''
                        INSERT INTO active_trades (
                            message_id, channel_id, guild_id, pair, action,
                            entry_price, tp1_price, tp2_price, tp3_price, sl_price,
                            discord_entry, discord_tp1, discord_tp2, discord_tp3, discord_sl,
                            live_entry, status, tp_hits, breakeven_active
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
                    ''', 
                    message_id, trade_data.get("channel_id"), trade_data.get("guild_id"),
                    trade_data["pair"], trade_data["action"], 
                    trade_data["entry"], trade_data["tp1"], trade_data["tp2"], trade_data["tp3"], trade_data["sl"],
                    trade_data.get("discord_entry"), trade_data.get("discord_tp1"), trade_data.get("discord_tp2"), 
                    trade_data.get("discord_tp3"), trade_data.get("discord_sl"),
                    trade_data.get("live_entry"), trade_data.get("status", "active"),
                    ','.join(trade_data.get("tp_hits", [])), trade_data.get("breakeven_active", False)
                    )
            except Exception as e:
                pass  # Continue with in-memory storage if database fails

    async def update_trade_in_db(self, message_id: str, trade_data: dict):
        """Update an existing trade in database"""
        # Always update in-memory first
        PRICE_TRACKING_CONFIG["active_trades"][message_id] = trade_data

        # Also update database if available
        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.execute('''
                        UPDATE active_trades SET 
                            status = $2, tp_hits = $3, breakeven_active = $4, last_updated = NOW()
                        WHERE message_id = $1
                    ''', 
                    message_id, trade_data.get("status", "active"),
                    ','.join(trade_data.get("tp_hits", [])), trade_data.get("breakeven_active", False)
                    )
            except Exception as e:
                pass  # Continue with in-memory storage if database fails

    async def remove_trade_from_db(self, message_id: str):
        """Remove completed trade from database"""
        # Always remove from memory first
        if message_id in PRICE_TRACKING_CONFIG["active_trades"]:
            del PRICE_TRACKING_CONFIG["active_trades"][message_id]

        # Also remove from database if available
        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.execute('DELETE FROM active_trades WHERE message_id = $1', message_id)
            except Exception as e:
                pass  # Continue with in-memory removal if database fails

    async def get_active_trades_from_db(self):
        """Get current active trades from database (used by commands)"""
        if not self.db_pool:
            return PRICE_TRACKING_CONFIG["active_trades"]

        try:
            # First try to use in-memory data for speed
            if PRICE_TRACKING_CONFIG["active_trades"]:
                return PRICE_TRACKING_CONFIG["active_trades"]

            # If in-memory is empty, load from database
            await self.load_active_trades_from_db()
            return PRICE_TRACKING_CONFIG["active_trades"]

        except Exception as e:
            return PRICE_TRACKING_CONFIG["active_trades"]

    async def remove_trade_from_tracking(self, message_id: str):
        """Remove a trade from tracking (wrapper for manual removal)"""
        try:
            await self.remove_trade_from_db(message_id)
            return True
        except Exception:
            return False

    async def save_invite_tracking(self):
        """Save invite tracking data to database"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                # Update all tracked invites
                for invite_code, data in INVITE_TRACKING.items():
                    await conn.execute(
                        '''
                        INSERT INTO invite_tracking 
                        (invite_code, guild_id, creator_id, nickname, total_joins, total_left, current_members, last_updated)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                        ON CONFLICT (invite_code) DO UPDATE SET
                            nickname = $4,
                            total_joins = $5,
                            total_left = $6,
                            current_members = $7,
                            last_updated = NOW()
                        ''', invite_code, data["guild_id"], data["creator_id"], 
                        data["nickname"], data["total_joins"], data["total_left"], data["current_members"]
                    )
        except Exception as e:
            print(f"❌ Error saving invite tracking to database: {str(e)}")

    # ===== LIVE PRICE TRACKING METHODS =====

    async def get_live_price(self, pair: str, use_all_apis: bool = False) -> Optional[float]:
        """Get live price with smart API rotation to conserve free tier limits"""
        if not PRICE_TRACKING_CONFIG["enabled"]:
            return None

        # Normalize pair format for different APIs
        pair_clean = pair.replace("/", "").upper()

        # For regular monitoring, use only 1-2 APIs to conserve limits
        # For initial signal verification, use all APIs for maximum accuracy
        if use_all_apis:
            return await self.get_verified_price_all_apis(pair_clean)
        else:
            return await self.get_price_optimized_rotation(pair_clean)

    async def get_price_optimized_rotation(self, pair_clean: str) -> Optional[float]:
        """Get price using smart API rotation across available APIs - MAXIMUM EFFICIENCY"""
        # Use priority order for most accurate APIs first
        api_order = PRICE_TRACKING_CONFIG["api_priority"]

        # Rotate through APIs to distribute load evenly
        start_index = PRICE_TRACKING_CONFIG["api_rotation_index"] % len(api_order)

        # Try 3 APIs per check for optimal balance of speed vs accuracy
        for i in range(3):
            api_index = (start_index + i) % len(api_order)
            api_name = api_order[api_index]

            price = await self.get_price_from_single_api(api_name, pair_clean)
            if price is not None:
                # Update rotation for next check
                PRICE_TRACKING_CONFIG["api_rotation_index"] = (start_index + 1) % len(api_order)
                print(f"✅ Got price {price} for {pair_clean} from {api_name}")
                return price

        # If first 3 fail, try ALL remaining APIs one by one
        print(f"⚠️ First 3 APIs failed for {pair_clean}, trying all remaining APIs...")
        for api_name in api_order:
            if api_name not in [api_order[(start_index + i) % len(api_order)] for i in range(3)]:
                price = await self.get_price_from_single_api(api_name, pair_clean)
                if price is not None:
                    print(f"✅ Got price {price} for {pair_clean} from fallback API {api_name}")
                    return price

        print(f"❌ All APIs failed for {pair_clean}")
        return None

    def calculate_live_tracking_levels(self, live_price: float, pair: str, action: str) -> Dict[str, float]:
        """Calculate TP/SL levels based on live market price"""
        # For now, use the live price as entry and calculate standard pip distances
        # This can be enhanced with more sophisticated logic later

        if pair == "XAUUSD":
            # Gold uses different pip calculation
            pip_value = 0.50  # $0.50 per pip for gold
            tp1_distance = 10 * pip_value  # 10 pips
            tp2_distance = 20 * pip_value  # 20 pips  
            tp3_distance = 30 * pip_value  # 30 pips
            sl_distance = 15 * pip_value   # 15 pips
        else:
            # Standard forex pairs
            if "JPY" in pair:
                pip_value = 0.01  # JPY pairs
            else:
                pip_value = 0.0001  # Standard pairs

            tp1_distance = 20 * pip_value  # 20 pips
            tp2_distance = 40 * pip_value  # 40 pips
            tp3_distance = 60 * pip_value  # 60 pips
            sl_distance = 25 * pip_value   # 25 pips

        if action == "BUY":
            return {
                "entry": live_price,
                "tp1": live_price + tp1_distance,
                "tp2": live_price + tp2_distance,
                "tp3": live_price + tp3_distance,
                "sl": live_price - sl_distance
            }
        else:  # SELL
            return {
                "entry": live_price,
                "tp1": live_price - tp1_distance,
                "tp2": live_price - tp2_distance,
                "tp3": live_price - tp3_distance,
                "sl": live_price + sl_distance
            }

    def get_api_symbol(self, api_name: str, pair_clean: str) -> str:
        """Map user-friendly symbols to API-specific symbols"""
        # Let's try multiple variations for each API to find what works
        symbol_mappings = {
            "fxapi": {
                "US100": ["NASDAQ", "QQQ", "USTEC", "NQ"],       # Nasdaq 100 alternatives (QQQ ETF tracks Nasdaq 100)
                "GER40": ["GDAXI", "DE40", "FDAX", "GER40"],     # German DAX alternatives (GDAXI is correct DAX)  
                "GER30": ["GDAXI", "DE30", "FDAX", "GER30"],     # German DAX alternatives
                "NAS100": ["NASDAQ", "QQQ", "USTEC", "NQ"],      # Alternative name for US100
                "US500": ["US500", "SPX", "SPY"],               # S&P 500
                "UK100": ["UK100", "UKX", "FTSE"],              # FTSE 100
                "JPN225": ["JPN225", "N225", "NKY"],             # Nikkei 225
                "AUS200": ["AUS200", "ASX", "XJO"],             # ASX 200
                "XAUUSD": ["XAU", "GOLD", "XAUUSD"]             # Gold/USD pairs
            },
            "twelve_data": {
                "US100": ["QQQ", "NASDAQ", "USTEC", "NQ"],       # Nasdaq 100 alternatives (QQQ ETF available on free tier)
                "GER40": ["GDAXI", "FDAX", "DAX30", "DE40"],     # German DAX alternatives (GDAXI should give correct ~24051 price)
                "GER30": ["GDAXI", "FDAX", "DAX30", "DE30"],     # German DAX alternatives
                "NAS100": ["QQQ", "NASDAQ", "USTEC", "NQ"],      # Alternative name for Nasdaq 100
                "US500": ["SPX", "GSPC"],                       # S&P 500
                "UK100": ["UKX", "FTSE"],                       # FTSE 100
                "JPN225": ["N225", "NKY"],                      # Nikkei 225
                "AUS200": ["XJO", "AXJO"],                      # ASX 200
                "XAUUSD": ["XAU/USD", "GOLD"]         # Gold/USD pairs - XAU/USD works
            },
            "alpha_vantage": {
                # Alpha Vantage doesn't support indices through currency exchange rate
                # These symbols won't work with the current implementation
                # Skip XAUUSD for Alpha Vantage - GLD ETF gives inaccurate prices vs spot gold
            },
            "fmp": {
                "US100": ["QQQ", "^NDX", "NDAQ", "ONEQ"],       # Nasdaq 100 ETF and index symbols
                "GER40": ["^GDAXI", "EXS1", "DAXEX", "FDAX"],   # German DAX alternatives (^GDAXI should give correct price)
                "GER30": ["^GDAXI", "EXS1", "DAXEX", "FDAX"],   # German DAX alternatives
                "NAS100": ["QQQ", "^NDX", "NDAQ", "ONEQ"],      # Alternative name for Nasdaq 100
                "US500": ["^SPX", "^GSPC"],                    # S&P 500
                "UK100": ["^UKX", "^FTSE"],                    # FTSE 100
                "JPN225": ["^N225", "^NKY"],                   # Nikkei 225
                "AUS200": ["^AXJO", "^XJO"],                   # ASX 200
                "XAUUSD": ["XAUUSD"]                           # Gold spot only (avoid futures GC=F which can differ from spot)
            }
        }

        # Get API-specific mapping - return first symbol to try
        if api_name in symbol_mappings and pair_clean in symbol_mappings[api_name]:
            symbol_list = symbol_mappings[api_name][pair_clean]
            if isinstance(symbol_list, list) and symbol_list:
                mapped_symbol = symbol_list[0]  # Use first symbol for now
                return mapped_symbol
            elif isinstance(symbol_list, str):
                return symbol_list

        # Return original symbol if no mapping found
        return pair_clean

    async def get_price_from_single_api(self, api_name: str, pair_clean: str) -> Optional[float]:
        """Get price from specific API - FIXED ALL API IMPLEMENTATIONS"""
        try:
            api_key = PRICE_TRACKING_CONFIG["api_keys"].get(f"{api_name}_key")

            if not api_key:
                return None

            # ===== ORIGINAL 4 APIS (COMPLETELY FIXED) =====

            if api_name == "fxapi":
                # FIXED: Correct FXApi implementation
                url = "https://api.fxapi.com/v1/latest"
                if pair_clean == "XAUUSD":
                    params = {"api_key": api_key, "base_currency": "XAU", "currencies": "USD"}
                elif len(pair_clean) == 6:  # Standard forex pair
                    base = pair_clean[:3]
                    quote = pair_clean[3:]
                    params = {"api_key": api_key, "base_currency": base, "currencies": quote}
                else:
                    return None

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "data" in data and len(data["data"]) > 0:
                                # Get the first rate from the data
                                for currency, rate in data["data"].items():
                                    if pair_clean == "XAUUSD" and currency == "USD":
                                        return float(rate)
                                    elif currency == pair_clean[3:]:  # Quote currency
                                        return float(rate)

            elif api_name == "twelve_data":
                # FIXED: TwelveData implementation
                url = "https://api.twelvedata.com/price"
                if pair_clean == "XAUUSD":
                    symbol = "XAU/USD"
                elif len(pair_clean) == 6:
                    symbol = f"{pair_clean[:3]}/{pair_clean[3:]}"
                else:
                    return None

                params = {"symbol": symbol, "apikey": api_key}

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "price" in data:
                                return float(data["price"])

            elif api_name == "alpha_vantage":
                # FIXED: Alpha Vantage for forex pairs only
                if pair_clean == "XAUUSD" or len(pair_clean) != 6:
                    return None  # Skip non-forex pairs

                url = "https://www.alphavantage.co/query"
                params = {
                    "function": "CURRENCY_EXCHANGE_RATE",
                    "from_currency": pair_clean[:3],
                    "to_currency": pair_clean[3:],
                    "apikey": api_key
                }

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "Realtime Currency Exchange Rate" in data:
                                rate_data = data["Realtime Currency Exchange Rate"]
                                if "5. Exchange Rate" in rate_data:
                                    return float(rate_data["5. Exchange Rate"])

            elif api_name == "fmp":
                # FIXED: Financial Modeling Prep
                if pair_clean == "XAUUSD":
                    symbol = "XAUUSD"
                elif len(pair_clean) == 6:
                    symbol = pair_clean
                else:
                    return None

                url = f"https://financialmodelingprep.com/api/v3/fx/{symbol}"
                params = {"apikey": api_key}

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if isinstance(data, list) and len(data) > 0:
                                if "bid" in data[0]:
                                    return float(data[0]["bid"])
                                elif "price" in data[0]:
                                    return float(data[0]["price"])

            # ===== NEW APIS (FIXED IMPLEMENTATIONS) =====

            elif api_name == "exchangerate":
                # FIXED: ExchangeRate-API
                if pair_clean == "XAUUSD" or len(pair_clean) != 6:
                    return None  # Only supports standard currencies

                base_currency = pair_clean[:3]
                target_currency = pair_clean[3:]
                url = f"https://v6.exchangerate-api.com/v6/{api_key}/pair/{base_currency}/{target_currency}"

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "conversion_rate" in data:
                                return float(data["conversion_rate"])

            elif api_name == "currencylayer":
                # FIXED: CurrencyLayer
                if pair_clean == "XAUUSD" or len(pair_clean) != 6:
                    return None

                url = "http://apilayer.net/api/live"
                base_currency = pair_clean[:3]
                target_currency = pair_clean[3:]
                params = {"access_key": api_key, "currencies": target_currency, "source": base_currency}

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "quotes" in data:
                                quote_key = f"{base_currency}{target_currency}"
                                if quote_key in data["quotes"]:
                                    return float(data["quotes"][quote_key])

            elif api_name == "fixer":
                # FIXED: Fixer.io
                if pair_clean == "XAUUSD" or len(pair_clean) != 6:
                    return None

                url = "http://data.fixer.io/api/latest"
                base_currency = pair_clean[:3]
                target_currency = pair_clean[3:]
                params = {"access_key": api_key, "symbols": target_currency, "base": base_currency}

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "rates" in data and target_currency in data["rates"]:
                                return float(data["rates"][target_currency])

            elif api_name == "openexchange":
                # FIXED: Open Exchange Rates (USD base only)
                if pair_clean == "XAUUSD" or not pair_clean.endswith("USD") or len(pair_clean) != 6:
                    return None  # Only supports USD as quote currency

                url = "https://openexchangerates.org/api/latest.json"
                base_currency = pair_clean[:3]
                params = {"app_id": api_key, "symbols": base_currency}

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "rates" in data and base_currency in data["rates"]:
                                # This gives us USD/BASE, we need BASE/USD
                                usd_to_base = float(data["rates"][base_currency])
                                return 1.0 / usd_to_base

            elif api_name == "currencyapi":
                # FIXED: CurrencyAPI.com
                if pair_clean == "XAUUSD" or len(pair_clean) != 6:
                    return None

                url = "https://api.currencyapi.com/v3/latest"
                base_currency = pair_clean[:3]
                target_currency = pair_clean[3:]
                params = {"apikey": api_key, "currencies": target_currency, "base_currency": base_currency}

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "data" in data and target_currency in data["data"]:
                                return float(data["data"][target_currency]["value"])

            elif api_name == "apilayer":
                # FIXED: APILayer Exchange Rates
                if pair_clean == "XAUUSD" or len(pair_clean) != 6:
                    return None

                url = "https://api.apilayer.com/exchangerates_data/latest"
                base_currency = pair_clean[:3]
                target_currency = pair_clean[3:]
                params = {"symbols": target_currency, "base": base_currency}
                headers = {"apikey": api_key}

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "rates" in data and target_currency in data["rates"]:
                                return float(data["rates"][target_currency])

            elif api_name == "abstractapi":
                # FIXED: AbstractAPI Exchange Rates
                if pair_clean == "XAUUSD" or len(pair_clean) != 6:
                    return None

                url = "https://exchange-rates.abstractapi.com/v1/live"
                base_currency = pair_clean[:3]
                target_currency = pair_clean[3:]
                params = {"api_key": api_key, "base": base_currency, "target": target_currency}

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "exchange_rate" in data:
                                return float(data["exchange_rate"])

            elif api_name == "currencybeacon":
                # FIXED: CurrencyBeacon
                if pair_clean == "XAUUSD" or len(pair_clean) != 6:
                    return None

                url = "https://api.currencybeacon.com/v1/latest"
                base_currency = pair_clean[:3]
                target_currency = pair_clean[3:]
                params = {"api_key": api_key, "from": base_currency, "to": target_currency}

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "rates" in data and target_currency in data["rates"]:
                                return float(data["rates"][target_currency])

            elif api_name == "polygon":
                # FIXED: Polygon.io
                if pair_clean == "XAUUSD":
                    url = f"https://api.polygon.io/v1/last/currencies/XAU/USD"
                elif len(pair_clean) == 6:
                    from_curr = pair_clean[:3]
                    to_curr = pair_clean[3:]
                    url = f"https://api.polygon.io/v1/last/currencies/{from_curr}/{to_curr}"
                else:
                    return None

                params = {"apikey": api_key}

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "last" in data:
                                if "bid" in data["last"]:
                                    return float(data["last"]["bid"])
                                elif "price" in data["last"]:
                                    return float(data["last"]["price"])

        except Exception as e:
            # Silent fail - let other APIs try
            pass

        return None

    async def get_verified_price_all_apis(self, pair_clean: str) -> Optional[float]:
        """Get price from all APIs for maximum accuracy verification (used sparingly)"""
        # Collect prices from multiple APIs for cross-verification
        prices = {}
        api_errors = {}

        # Try FXApi first
        try:
            if PRICE_TRACKING_CONFIG["api_keys"]["fxapi_key"]:
                api_symbol = self.get_api_symbol("fxapi", pair_clean)
                url = f"{PRICE_TRACKING_CONFIG['api_endpoints']['fxapi']}"
                params = {
                    "api_key": PRICE_TRACKING_CONFIG["api_keys"]["fxapi_key"],
                    "symbols": api_symbol
                }

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "rates" in data and api_symbol in data["rates"]:
                                prices["fxapi"] = float(data["rates"][api_symbol])
                        elif response.status == 429:
                            api_errors["fxapi"] = "rate_limit"
                            await self.log_api_limit_warning("FXApi", "Rate limit exceeded - consider upgrading plan")
                        elif response.status == 403:
                            api_errors["fxapi"] = "access_denied"
                            await self.log_api_limit_warning("FXApi", "Access denied - API key may be invalid or expired")
        except Exception as e:
            api_errors["fxapi"] = str(e)

        # Try Twelve Data API
        try:
            if PRICE_TRACKING_CONFIG["api_keys"]["twelve_data_key"]:
                api_symbol = self.get_api_symbol("twelve_data", pair_clean)
                url = f"{PRICE_TRACKING_CONFIG['api_endpoints']['twelve_data']}"
                params = {
                    "symbol": api_symbol,
                    "apikey": PRICE_TRACKING_CONFIG["api_keys"]["twelve_data_key"]
                }

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "price" in data:
                                prices["twelve_data"] = float(data["price"])
                            elif "message" in data and "limit" in data["message"].lower():
                                api_errors["twelve_data"] = "usage_limit"
                                await self.log_api_limit_warning("Twelve Data", f"Usage limit reached: {data['message']}")
                        elif response.status == 429:
                            api_errors["twelve_data"] = "rate_limit"
                            await self.log_api_limit_warning("Twelve Data", "Rate limit exceeded - upgrade for higher limits")
        except Exception as e:
            api_errors["twelve_data"] = str(e)

        # Try Alpha Vantage API (skip for indices)
        try:
            if (PRICE_TRACKING_CONFIG["api_keys"]["alpha_vantage_key"] and 
                pair_clean not in ["US100", "GER40", "GER30", "NAS100", "US500", "UK100", "JPN225", "AUS200"]):
                api_symbol = self.get_api_symbol("alpha_vantage", pair_clean)
                url = f"{PRICE_TRACKING_CONFIG['api_endpoints']['alpha_vantage']}"
                params = {
                    "function": "CURRENCY_EXCHANGE_RATE",
                    "from_currency": api_symbol[:3],
                    "to_currency": api_symbol[3:],
                    "apikey": PRICE_TRACKING_CONFIG["api_keys"]["alpha_vantage_key"]
                }

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "Realtime Currency Exchange Rate" in data:
                                rate_data = data["Realtime Currency Exchange Rate"]
                                if "5. Exchange Rate" in rate_data:
                                    prices["alpha_vantage"] = float(rate_data["5. Exchange Rate"])
                            elif "Note" in data and "call frequency" in data["Note"]:
                                api_errors["alpha_vantage"] = "frequency_limit"
                                await self.log_api_limit_warning("Alpha Vantage", "Daily API limit reached - upgrade for unlimited calls")
                        elif response.status == 429:
                            api_errors["alpha_vantage"] = "rate_limit"
                            await self.log_api_limit_warning("Alpha Vantage", "Rate limit exceeded")
        except Exception as e:
            api_errors["alpha_vantage"] = str(e)

        # Try Financial Modeling Prep API
        try:
            if PRICE_TRACKING_CONFIG["api_keys"]["fmp_key"]:
                api_symbol = self.get_api_symbol("fmp", pair_clean)
                url = f"{PRICE_TRACKING_CONFIG['api_endpoints']['fmp']}/{api_symbol}"
                params = {
                    "apikey": PRICE_TRACKING_CONFIG["api_keys"]["fmp_key"]
                }

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if isinstance(data, list) and len(data) > 0 and "price" in data[0]:
                                prices["fmp"] = float(data[0]["price"])
                            elif isinstance(data, dict) and "Error Message" in data:
                                if "limit" in data["Error Message"].lower():
                                    api_errors["fmp"] = "usage_limit"
                                    await self.log_api_limit_warning("Financial Modeling Prep", f"Usage limit: {data['Error Message']}")
                        elif response.status == 429:
                            api_errors["fmp"] = "rate_limit"
                            await self.log_api_limit_warning("Financial Modeling Prep", "Rate limit exceeded - upgrade plan needed")
        except Exception as e:
            api_errors["fmp"] = str(e)
            print(f"FMP API failed for {pair_clean}: {e}")

        # Verify price accuracy using multiple sources
        return await self.verify_price_accuracy(pair_clean, prices, api_errors)

    async def verify_price_accuracy(self, pair: str, prices: Dict[str, float], api_errors: Dict[str, str]) -> Optional[float]:
        """Verify price accuracy by cross-checking multiple API sources"""
        if not prices:
            print(f"❌ No valid prices obtained for {pair} - all APIs failed")
            if api_errors:
                error_summary = ", ".join([f"{api}: {error}" for api, error in api_errors.items()])
                print(f"   API Errors: {error_summary}")
            return None

        if len(prices) == 1:
            # Only one source - use it but log warning
            api_name, price = next(iter(prices.items()))
            print(f"⚠️ Only {api_name} provided price for {pair}: ${price}")
            return price

        # Multiple sources - verify consistency
        price_values = list(prices.values())
        avg_price = sum(price_values) / len(price_values)

        # Check if all prices are within 0.1% of average (very tight tolerance)
        tolerance = 0.001  # 0.1%
        consistent_prices = []

        for api_name, price in prices.items():
            deviation = abs(price - avg_price) / avg_price
            if deviation <= tolerance:
                consistent_prices.append((api_name, price))
            else:
                print(f"⚠️ {api_name} price for {pair} deviates significantly: ${price} (avg: ${avg_price:.5f})")

        if len(consistent_prices) >= 2:
            # Use average of consistent prices
            final_price = sum([price for _, price in consistent_prices]) / len(consistent_prices)
            api_names = ", ".join([api for api, _ in consistent_prices])
            print(f"✅ Price verified for {pair}: ${final_price:.5f} (sources: {api_names})")
            return final_price
        elif len(prices) >= 2:
            # Use median if we have multiple sources but they're not very consistent
            sorted_prices = sorted(price_values)
            median_price = sorted_prices[len(sorted_prices)//2]
            print(f"⚠️ Using median price for {pair}: ${median_price:.5f} (prices varied across sources)")
            return median_price
        else:
            # Fallback to single source
            api_name, price = next(iter(prices.items()))
            return price

    async def log_api_limit_warning(self, api_name: str, message: str):
        """Log API limit warnings to Discord and console"""
        warning_msg = f"🚨 **{api_name} API Limit Warning**\n{message}\n\n" + \
                     f"**Action Required:**\n" + \
                     f"• Check your {api_name} dashboard for usage details\n" + \
                     f"• Consider upgrading your plan for higher limits\n" + \
                     f"• Bot will continue using other API sources\n\n" + \
                     f"**Impact:** Price tracking accuracy may be reduced if multiple APIs are limited."

        await self.log_to_discord(warning_msg)
        print(f"API LIMIT WARNING: {api_name} - {message}")

    def parse_signal_message(self, content: str) -> Optional[Dict]:
        """Parse a trading signal message to extract trade data"""
        try:
            lines = content.split('\n')
            trade_data = {
                "pair": None,
                "action": None,
                "entry": None,
                "tp1": None,
                "tp2": None,
                "tp3": None,
                "sl": None,
                "status": "active",
                "tp_hits": [],
                "breakeven_active": False
            }

            # Find pair from "Trade Signal For: PAIR"
            for line in lines:
                if "Trade Signal For:" in line:
                    parts = line.split("Trade Signal For:")
                    if len(parts) > 1:
                        trade_data["pair"] = parts[1].strip()
                        break

            # Extract action from "Entry Type: Buy execution" or "Entry Type: Sell execution"
            entry_type_match = re.search(r'Entry Type:\s*(Buy|Sell)', content, re.IGNORECASE)
            if entry_type_match:
                trade_data["action"] = entry_type_match.group(1).upper()
            else:
                # Fallback to old format detection
                for line in lines:
                    if "BUY" in line.upper() or "SELL" in line.upper():
                        if "BUY" in line.upper():
                            trade_data["action"] = "BUY"
                        else:
                            trade_data["action"] = "SELL"
                        break

            # Extract entry price from "Entry Price: $3473.50" (handles $ symbol)
            entry_match = re.search(r'Entry Price:\s*\$?([0-9]+\.?[0-9]*)', content, re.IGNORECASE)
            if entry_match:
                trade_data["entry"] = float(entry_match.group(1))
            else:
                # Fallback to old format "Entry: price"
                entry_match = re.search(r'Entry[:\s]*\$?([0-9]+\.?[0-9]*)', content, re.IGNORECASE)
                if entry_match:
                    trade_data["entry"] = float(entry_match.group(1))

            # Extract Take Profit levels
            tp1_match = re.search(r'Take Profit 1:\s*\$?([0-9]+\.?[0-9]*)', content, re.IGNORECASE)
            if tp1_match:
                trade_data["tp1"] = float(tp1_match.group(1))
            else:
                # Fallback to old format "TP1: price"
                tp1_match = re.search(r'TP1[:\s]*\$?([0-9]+\.?[0-9]*)', content, re.IGNORECASE)
                if tp1_match:
                    trade_data["tp1"] = float(tp1_match.group(1))

            tp2_match = re.search(r'Take Profit 2:\s*\$?([0-9]+\.?[0-9]*)', content, re.IGNORECASE)
            if tp2_match:
                trade_data["tp2"] = float(tp2_match.group(1))
            else:
                # Fallback to old format "TP2: price"
                tp2_match = re.search(r'TP2[:\s]*\$?([0-9]+\.?[0-9]*)', content, re.IGNORECASE)
                if tp2_match:
                    trade_data["tp2"] = float(tp2_match.group(1))

            tp3_match = re.search(r'Take Profit 3:\s*\$?([0-9]+\.?[0-9]*)', content, re.IGNORECASE)
            if tp3_match:
                trade_data["tp3"] = float(tp3_match.group(1))
            else:
                # Fallback to old format "TP3: price"
                tp3_match = re.search(r'TP3[:\s]*\$?([0-9]+\.?[0-9]*)', content, re.IGNORECASE)
                if tp3_match:
                    trade_data["tp3"] = float(tp3_match.group(1))

            # Extract Stop Loss from "Stop Loss: $3478.50"
            sl_match = re.search(r'Stop Loss:\s*\$?([0-9]+\.?[0-9]*)', content, re.IGNORECASE)
            if sl_match:
                trade_data["sl"] = float(sl_match.group(1))
            else:
                # Fallback to old format "SL: price"
                sl_match = re.search(r'SL[:\s]*\$?([0-9]+\.?[0-9]*)', content, re.IGNORECASE)
                if sl_match:
                    trade_data["sl"] = float(sl_match.group(1))

            # Debug logging to help troubleshoot parsing issues
            print(f"🔍 Parsing signal content: {content[:100]}...")
            print(f"   Extracted - Pair: {trade_data['pair']}, Action: {trade_data['action']}")
            print(f"   Extracted - Entry: {trade_data['entry']}, TP1: {trade_data['tp1']}, TP2: {trade_data['tp2']}, TP3: {trade_data['tp3']}, SL: {trade_data['sl']}")

            # Validate required fields
            if all([trade_data["pair"], trade_data["action"], trade_data["entry"], 
                   trade_data["tp1"], trade_data["tp2"], trade_data["tp3"], trade_data["sl"]]):
                print(f"✅ Successfully parsed signal for {trade_data['pair']} ({trade_data['action']})")
                return trade_data
            else:
                missing_fields = []
                for field, value in trade_data.items():
                    if field in ["pair", "action", "entry", "tp1", "tp2", "tp3", "sl"] and value is None:
                        missing_fields.append(field)
                pass

        except Exception as e:
            pass

        return None

    async def check_message_still_exists(self, message_id: str, trade_data: Dict) -> bool:
        """Check if the original trading signal message still exists"""
        try:
            channel_id = trade_data.get("channel_id")
            if not channel_id:
                return False

            channel = self.get_channel(int(channel_id))
            if not channel:
                return False

            # Try to fetch the message
            message = await channel.fetch_message(int(message_id))
            return message is not None

        except discord.NotFound:
            # Message was deleted
            return False
        except Exception as e:
            # Other errors - assume message still exists to avoid false deletions
            return True

    async def check_price_levels(self, message_id: str, trade_data: Dict) -> bool:
        """Check if current price has hit any TP/SL levels"""
        try:
            # First check if the original message still exists
            if not await self.check_message_still_exists(message_id, trade_data):
                # Message was deleted, remove from tracking
                await self.remove_trade_from_db(message_id)
                return True  # Return True to indicate this trade should be removed from active tracking
            current_price = await self.get_live_price(trade_data["pair"])
            if current_price is None:
                return False

            action = trade_data["action"]
            entry = trade_data["entry"]

            # Determine if we should check breakeven (after TP2 hit)
            if trade_data["breakeven_active"]:
                # Check if price returned to entry (breakeven SL)
                if action == "BUY" and current_price <= entry:
                    await self.handle_breakeven_hit(message_id, trade_data)
                    return True
                elif action == "SELL" and current_price >= entry:
                    await self.handle_breakeven_hit(message_id, trade_data)
                    return True
            else:
                # Check SL first
                if action == "BUY" and current_price <= trade_data["sl"]:
                    await self.handle_sl_hit(message_id, trade_data)
                    return True
                elif action == "SELL" and current_price >= trade_data["sl"]:
                    await self.handle_sl_hit(message_id, trade_data)
                    return True

                # Check TP levels
                if action == "BUY":
                    if "tp3" not in trade_data["tp_hits"] and current_price >= trade_data["tp3"]:
                        await self.handle_tp_hit(message_id, trade_data, "tp3")
                        return True
                    elif "tp2" not in trade_data["tp_hits"] and current_price >= trade_data["tp2"]:
                        await self.handle_tp_hit(message_id, trade_data, "tp2")
                        return True
                    elif "tp1" not in trade_data["tp_hits"] and current_price >= trade_data["tp1"]:
                        await self.handle_tp_hit(message_id, trade_data, "tp1")
                        return True

                elif action == "SELL":
                    if "tp3" not in trade_data["tp_hits"] and current_price <= trade_data["tp3"]:
                        await self.handle_tp_hit(message_id, trade_data, "tp3")
                        return True
                    elif "tp2" not in trade_data["tp_hits"] and current_price <= trade_data["tp2"]:
                        await self.handle_tp_hit(message_id, trade_data, "tp2")
                        return True
                    elif "tp1" not in trade_data["tp_hits"] and current_price <= trade_data["tp1"]:
                        await self.handle_tp_hit(message_id, trade_data, "tp1")
                        return True

            return False

        except Exception as e:
            print(f"Error checking price levels for {message_id}: {e}")
            return False

    async def handle_tp_hit(self, message_id: str, trade_data: Dict, tp_level: str, offline_hit: bool = False):
        """Handle when a TP level is hit"""
        try:
            # Update trade data
            trade_data["tp_hits"].append(tp_level)

            if tp_level == "tp2":
                # After TP2, activate breakeven
                trade_data["breakeven_active"] = True
                trade_data["status"] = "active (tp2 hit - breakeven active)"
                # Update in database
                await self.update_trade_in_db(message_id, trade_data)
            elif tp_level == "tp1":
                trade_data["status"] = "active (tp1 hit)"
                # Update in database
                await self.update_trade_in_db(message_id, trade_data)
            elif tp_level == "tp3":
                trade_data["status"] = "completed (tp3 hit)"
                # Remove from active trades after TP3 (database and memory)
                await self.remove_trade_from_db(message_id)

            # Send notification
            await self.send_tp_notification(message_id, trade_data, tp_level, offline_hit)

        except Exception as e:
            print(f"Error handling TP hit: {e}")

    async def handle_sl_hit(self, message_id: str, trade_data: Dict, offline_hit: bool = False):
        """Handle when SL is hit"""
        try:
            trade_data["status"] = "closed (sl hit)"

            # Remove from active trades (database and memory)
            await self.remove_trade_from_db(message_id)

            # Send notification
            await self.send_sl_notification(message_id, trade_data, offline_hit)

        except Exception as e:
            print(f"Error handling SL hit: {e}")

    async def handle_breakeven_hit(self, message_id: str, trade_data: Dict, offline_hit: bool = False):
        """Handle when price returns to breakeven after TP2"""
        try:
            trade_data["status"] = "closed (breakeven after tp2)"

            # Remove from active trades (database and memory)
            await self.remove_trade_from_db(message_id)

            # Send breakeven notification
            await self.send_breakeven_notification(message_id, trade_data, offline_hit)

        except Exception as e:
            print(f"Error handling breakeven hit: {e}")

    async def send_tp_notification(self, message_id: str, trade_data: Dict, tp_level: str, offline_hit: bool = False):
        """Send TP hit notification with random message selection"""
        import random

        try:
            # Random messages for each TP level
            tp1_messages = [
                "@everyone TP1 has been hit. First target secured, let's keep it going. Next stop: TP2 📈🔥",
                "@everyone TP1 smashed. Secure some profits if you'd like and let's aim for TP2 🎯💪",
                "@everyone TP1 has been hit! Keep your eyes on the next level. TP2 up next 👀💸",
                "@everyone TP1 has been reached. Let's keep the discipline and push for TP2 🚀📊",
                "@everyone TP1 locked in. Let's keep monitoring price action and go for TP2 💰📍",
                "@everyone TP1 has been reached. Trade is moving as planned. Next stop: TP2 🔄📊",
                "@everyone TP1 hit. Great entry. now let's trail it smart toward TP2 🧠📈"
            ]

            tp2_messages = [
                "@everyone TP1 & TP2 have both been hit :rocket::rocket: move your SL to breakeven and lets get TP3 :money_with_wings:",
                "@everyone TP2 has been hit :rocket::rocket: move your SL to breakeven and lets get TP3 :money_with_wings:",
                "@everyone TP2 has been hit :rocket::rocket: move your sl to breakeven, partially close the trade and lets get tp3 :dart::dart::dart:",
                "@everyone TP2 has been hit:money_with_wings: please move your SL to breakeven, partially close the trade and lets go for TP3 :rocket:",
                "@everyone TP2 has been hit. Move your SL to breakeven and secure those profits. Let's push for TP3. we're not done yet 🚀💰",
                "@everyone TP2 has been smashed. Move SL to breakeven, partial close if you haven't already. TP3 is calling 📈🔥",
                "@everyone TP2 has been hit. Move SL to breakeven and lock it in. Eyes on TP3 now so let's finish strong 🧠🎯",
                "@everyone TP2 has been tagged. Time to shift SL to breakeven and secure the bag. TP3 is the final boss and we're coming for it 💼⚔️"
            ]

            tp3_messages = [
                "@everyone TP3 hit. Full target smashed, perfect execution 🔥🔥🔥",
                "@everyone Huge win, TP3 reached. Congrats to everyone who followed 📊🚀",
                "@everyone TP3 just got hit. Close it out and lock in profits 💸🎯",
                "@everyone TP3 tagged. That wraps up the full setup — solid trade 💪💼",
                "@everyone TP3 locked in. Flawless setup from entry to exit 🙌📈",
                "@everyone TP3 hit. This one went exactly as expected. Great job ✅💰",
                "@everyone TP3 has been reached. Hope you secured profits all the way through 🏁📊",
                "@everyone TP3 reached. Strategy and patience paid off big time 🔍🚀",
                "@everyone TP3 secured. That's the result of following the plan 💼💎"
            ]

            message = await self.get_channel(trade_data.get("channel_id")).fetch_message(int(message_id))

            # Select random message based on TP level
            if tp_level == "tp1":
                notification = random.choice(tp1_messages)
            elif tp_level == "tp2":
                notification = random.choice(tp2_messages)
            elif tp_level == "tp3":
                notification = random.choice(tp3_messages)
            else:
                # Fallback to original message
                notification = f"@everyone **{tp_level.upper()} HAS BEEN HIT!** 🎯"

            # Add offline hit indication if applicable
            if offline_hit:
                notification += "\n⚠️ *This level was hit while the bot was offline - notification sent upon restart*"

            await message.reply(notification)

        except Exception as e:
            print(f"Error sending TP notification: {e}")

    async def send_sl_notification(self, message_id: str, trade_data: Dict, offline_hit: bool = False):
        """Send SL hit notification with random message selection"""
        import random

        try:
            # Random messages for SL hits
            sl_messages = [
                "@everyone This one hit SL. It happens. Let's stay focused and get the next one 🔄🧠",
                "@everyone SL has been hit. Risk was managed, we move on 💪📉",
                "@everyone This setup didn't go as planned and hit SL. On to the next 📊",
                "@everyone SL hit. Discipline keeps us in the game. We´ll get the loss back next trade💼🧘‍♂️"
            ]

            message = await self.get_channel(trade_data.get("channel_id")).fetch_message(int(message_id))
            notification = random.choice(sl_messages)

            # Add offline hit indication if applicable
            if offline_hit:
                notification += "\n⚠️ *This level was hit while the bot was offline - notification sent upon restart*"

            await message.reply(notification)

        except Exception as e:
            print(f"Error sending SL notification: {e}")

    async def send_breakeven_notification(self, message_id: str, trade_data: Dict, offline_hit: bool = False):
        """Send breakeven hit notification"""
        try:
            message = await self.get_channel(trade_data.get("channel_id")).fetch_message(int(message_id))
            notification = f"This pair reversed to breakeven after we hit TP2. As we stated, we had already moved our SL to breakeven, so we were out safe.\n@everyone"

            # Add offline hit indication if applicable
            if offline_hit:
                notification += "\n⚠️ *This level was hit while the bot was offline - notification sent upon restart*"

            await message.reply(notification)

        except Exception as e:
            print(f"Error sending breakeven notification: {e}")

    async def track_member_join_via_invite(self, member, invite_code):
        """Track a member joining via specific invite"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                # Record the join
                await conn.execute(
                    '''
                    INSERT INTO member_joins (member_id, guild_id, invite_code, joined_at, is_currently_member)
                    VALUES ($1, $2, $3, NOW(), TRUE)
                    ''', member.id, member.guild.id, invite_code
                )

                # Update invite tracking statistics
                if invite_code in INVITE_TRACKING:
                    INVITE_TRACKING[invite_code]["total_joins"] += 1
                    INVITE_TRACKING[invite_code]["current_members"] += 1
                    await self.save_invite_tracking()

        except Exception as e:
            print(f"❌ Error tracking member join via invite: {str(e)}")

    async def track_member_leave(self, member):
        """Track a member leaving and update invite statistics"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                # Find the member's join record and update it
                join_record = await conn.fetchrow(
                    '''
                    SELECT invite_code FROM member_joins 
                    WHERE member_id = $1 AND guild_id = $2 AND is_currently_member = TRUE
                    ORDER BY joined_at DESC LIMIT 1
                    ''', member.id, member.guild.id
                )

                if join_record and join_record['invite_code']:
                    invite_code = join_record['invite_code']

                    # Update the member's record
                    await conn.execute(
                        '''
                        UPDATE member_joins 
                        SET left_at = NOW(), is_currently_member = FALSE
                        WHERE member_id = $1 AND guild_id = $2 AND is_currently_member = TRUE
                        ''', member.id, member.guild.id
                    )

                    # Update invite tracking statistics
                    if invite_code in INVITE_TRACKING:
                        INVITE_TRACKING[invite_code]["total_left"] += 1
                        INVITE_TRACKING[invite_code]["current_members"] = max(0, INVITE_TRACKING[invite_code]["current_members"] - 1)
                        await self.save_invite_tracking()

        except Exception as e:
            print(f"❌ Error tracking member leave: {str(e)}")

    def calculate_level(self, message_count):
        """Calculate user level based on message count"""
        for level in sorted(LEVEL_SYSTEM["level_requirements"].keys(), reverse=True):
            if message_count >= LEVEL_SYSTEM["level_requirements"][level]:
                return level
        return 0  # No level achieved yet

    async def handle_level_up(self, user, guild, old_level, new_level):
        """Handle level up - assign roles and send DM"""
        try:
            # Assign new level role (don't remove old ones as requested)
            new_role_id = LEVEL_SYSTEM["level_roles"][new_level]
            new_role = guild.get_role(new_role_id)

            if new_role:
                member = guild.get_member(user.id)
                if member:
                    try:
                        await member.add_roles(new_role, reason=f"Level {new_level} achieved")
                        await self.log_to_discord(f"🎉 {member.display_name} leveled up to Level {new_level}! Role '{new_role.name}' assigned.")

                        # Send congratulations DM
                        try:
                            dm_message = f"Congratulations! You've leveled up to level {new_level}!"
                            await user.send(dm_message)
                            await self.log_to_discord(f"📬 Sent level-up DM to {member.display_name} for Level {new_level}")
                        except discord.Forbidden:
                            await self.log_to_discord(f"⚠️ Could not send level-up DM to {member.display_name} (DMs disabled)")

                    except discord.Forbidden:
                        await self.log_to_discord(f"❌ No permission to assign Level {new_level} role to {member.display_name}")
                else:
                    await self.log_to_discord(f"❌ Could not find member {user.display_name} in guild for level-up")
            else:
                await self.log_to_discord(f"❌ Could not find Level {new_level} role (ID: {new_role_id})")

        except Exception as e:
            await self.log_to_discord(f"❌ Error handling level up for {user.display_name}: {str(e)}")

    async def process_message_for_levels(self, message):
        """Process a message for level system"""
        if not LEVEL_SYSTEM["enabled"]:
            return

        # Skip bots and DMs
        if message.author.bot or not message.guild:
            return

        user_id = str(message.author.id)
        guild_id = message.guild.id

        # Initialize user data if not exists
        if user_id not in LEVEL_SYSTEM["user_data"]:
            LEVEL_SYSTEM["user_data"][user_id] = {
                "message_count": 0,
                "current_level": 0,
                "guild_id": guild_id
            }

        # Increment message count
        LEVEL_SYSTEM["user_data"][user_id]["message_count"] += 1
        current_count = LEVEL_SYSTEM["user_data"][user_id]["message_count"]
        old_level = LEVEL_SYSTEM["user_data"][user_id]["current_level"]

        # Calculate new level
        new_level = self.calculate_level(current_count)

        # Check if leveled up
        if new_level > old_level:
            LEVEL_SYSTEM["user_data"][user_id]["current_level"] = new_level
            await self.handle_level_up(message.author, message.guild, old_level, new_level)

            # Save to database
            await self.save_level_system()

    @tasks.loop(seconds=30)  # Check every 30 seconds for instant role removal
    async def role_removal_task(self):
        """Background task to remove expired roles and send DMs"""
        if not AUTO_ROLE_CONFIG["enabled"] or not AUTO_ROLE_CONFIG[
                "active_members"]:
            return

        current_time = datetime.now(AMSTERDAM_TZ)
        expired_members = []

        for member_id, data in AUTO_ROLE_CONFIG["active_members"].items():
            try:
                # Handle weekend delayed members with custom expiry time
                if data.get("weekend_delayed", False) and "expiry_time" in data:
                    # Weekend joiners have specific expiry time (Monday 23:59)
                    expiry_time = datetime.fromisoformat(data["expiry_time"])
                    if expiry_time.tzinfo is None:
                        if PYTZ_AVAILABLE:
                            expiry_time = AMSTERDAM_TZ.localize(expiry_time)
                        else:
                            expiry_time = expiry_time.replace(
                                tzinfo=AMSTERDAM_TZ)
                    else:
                        expiry_time = expiry_time.astimezone(AMSTERDAM_TZ)

                else:
                    # Normal members - 24 hours from role_added_time
                    role_added_time = datetime.fromisoformat(
                        data["role_added_time"])
                    if role_added_time.tzinfo is None:
                        if PYTZ_AVAILABLE:
                            role_added_time = AMSTERDAM_TZ.localize(
                                role_added_time)
                        else:
                            role_added_time = role_added_time.replace(
                                tzinfo=AMSTERDAM_TZ)
                    else:
                        role_added_time = role_added_time.astimezone(
                            AMSTERDAM_TZ)

                    expiry_time = role_added_time + timedelta(hours=24)

                if current_time >= expiry_time:
                    expired_members.append(member_id)

            except Exception as e:
                print(f"❌ Error processing member {member_id}: {str(e)}")
                expired_members.append(member_id)  # Remove corrupted entries

        # Process expired members
        for member_id in expired_members:
            await self.remove_expired_role(member_id)

        # Save updated config if there were changes
        if expired_members:
            await self.save_auto_role_config()

    @tasks.loop(
        minutes=1)  # Check every minute for Monday activation notifications
    async def weekend_activation_task(self):
        """Background task to send Monday activation DMs for weekend joiners"""
        if not AUTO_ROLE_CONFIG["enabled"] or not AUTO_ROLE_CONFIG[
                "weekend_pending"]:
            return

        current_time = datetime.now(AMSTERDAM_TZ)
        activation_time = self.get_next_monday_activation_time()

        # Check if it's past Monday 00:01 and send activation notifications
        if current_time >= activation_time:
            pending_members = list(AUTO_ROLE_CONFIG["weekend_pending"].keys())

            for member_id in pending_members:
                try:
                    data = AUTO_ROLE_CONFIG["weekend_pending"][member_id]
                    guild = self.get_guild(data["guild_id"])
                    if guild:
                        member = guild.get_member(int(member_id))
                        if member:
                            activation_message = (
                                "Hey! The weekend is over, so the trading markets have been opened again. "
                                "That means your 24-hour welcome gift has officially started. "
                                "You now have full access to the premium channel. "
                                "Let's make the most of it by securing some wins together!"
                            )
                            await member.send(activation_message)
                            print(f"✅ Sent Monday activation DM to {member.display_name}")

                            # Mark as processed and remove from weekend_pending
                            del AUTO_ROLE_CONFIG["weekend_pending"][member_id]
                            await self.save_auto_role_config()
                except Exception as e:
                    await self.log_to_discord(
                        f"❌ Error processing Monday activation for member {member_id}: {str(e)}")

    @tasks.loop(minutes=30)  # Check every hour for follow-up DM sending
    async def followup_dm_task(self):
        """Background task to send follow-up DMs after 3, 7, and 14 days"""
        if not AUTO_ROLE_CONFIG["dm_schedule"]:
            return

        current_time = datetime.now(AMSTERDAM_TZ)
        messages_to_send = []

        # Define the follow-up messages
        dm_messages = {
            3:
            "Hey! It's been 3 days since your **24-hour free access to the Premium Signals channel** ended. We hope you were able to catch good trades with us during that time.\n\nAs you've probably seen, the **free signals channel only gets about 1 signal a day**, while inside **Gold Pioneers**, members receive **8–10 high-quality signals every single day in <#1350929852299214999>**. That means way more chances to profit and grow consistently.\n\nWe'd love to **invite you back to Premium Signals** so you don't miss out on more solid opportunities.\n\n**Feel free to join us again through this link:** <https://whop.com/gold-pioneer>",
            7:
            "It's been a week since your Premium Signals trial ended. Since then, our **Gold Pioneers  have been catching trade setups daily in <#1350929852299214999>**.\n\nIf you found value in just 24 hours, imagine the results you could be seeing by now with full access. It's all about **consistency and staying plugged into the right information**.\n\nWe'd like to **personally invite you to rejoin Premium Signals** and get back into the rhythm.\n\n\n**Feel free to join us again through this link:** <https://whop.com/gold-pioneer>",
            14:
            "Hey! It's been two weeks since your access to Premium Signals ended. We hope you've stayed active. \n\nIf you've been trading solo or passively following the free channel, you might be feeling the difference. in <#1350929852299214999>, it's not just about more signals. It's about the **structure, support, and smarter decision-making**. That edge can make all the difference over time.\n\nWe'd love to **officially invite you back into Premium Signals** and help you start compounding results again.\n\n**Feel free to join us again through this link:** <https://whop.com/gold-pioneer>"
        }

        for member_id, schedule_data in AUTO_ROLE_CONFIG["dm_schedule"].items(
        ):
            try:
                expiry_time = datetime.fromisoformat(
                    schedule_data["role_expired"])
                if expiry_time.tzinfo is None:
                    expiry_time = expiry_time.replace(tzinfo=AMSTERDAM_TZ)
                else:
                    expiry_time = expiry_time.astimezone(AMSTERDAM_TZ)

                # Check for each follow-up period
                for days, message in dm_messages.items():
                    sent_key = f"dm_{days}_sent"

                    if not schedule_data.get(sent_key, False):
                        time_diff = current_time - expiry_time

                        if time_diff >= timedelta(days=days):
                            messages_to_send.append({
                                'member_id':
                                member_id,
                                'guild_id':
                                schedule_data['guild_id'],
                                'message':
                                message,
                                'days':
                                days,
                                'sent_key':
                                sent_key
                            })

            except Exception as e:
                await self.log_to_discord(
                    f"❌ Error processing DM schedule for member {member_id}: {str(e)}"
                )

        # Send the messages
        for msg_data in messages_to_send:
            try:
                guild = self.get_guild(msg_data['guild_id'])
                if not guild:
                    continue

                member = guild.get_member(int(msg_data['member_id']))
                if not member:
                    continue

                # Check if member has Gold Pioneer role - if so, skip the DM
                gold_pioneer_role = guild.get_role(GOLD_PIONEER_ROLE_ID)
                if gold_pioneer_role and gold_pioneer_role in member.roles:
                    await self.log_to_discord(
                        f"⏭️ Skipping {msg_data['days']}-day DM for {member.display_name} - already has Gold Pioneer role"
                    )
                    # Mark as sent even though we skipped it
                    AUTO_ROLE_CONFIG["dm_schedule"][msg_data['member_id']][
                        msg_data['sent_key']] = True
                    continue

                # Send the follow-up DM
                await member.send(msg_data['message'])
                await self.log_to_discord(
                    f"📬 Sent {msg_data['days']}-day follow-up DM to {member.display_name}"
                )

                # Mark as sent
                AUTO_ROLE_CONFIG["dm_schedule"][msg_data['member_id']][
                    msg_data['sent_key']] = True

            except discord.Forbidden:
                await self.log_to_discord(
                    f"⚠️ Could not send {msg_data['days']}-day follow-up DM to member {msg_data['member_id']} (DMs disabled)"
                )
                # Mark as sent to avoid retrying when DMs are disabled
                AUTO_ROLE_CONFIG["dm_schedule"][msg_data['member_id']][
                    msg_data['sent_key']] = True
            except Exception as e:
                # For other errors, implement retry logic
                retry_count = AUTO_ROLE_CONFIG["dm_schedule"][msg_data['member_id']].get(f"dm_{msg_data['days']}_retry_count", 0)
                max_retries = 3

                if retry_count < max_retries:
                    # Increment retry count and try again later
                    AUTO_ROLE_CONFIG["dm_schedule"][msg_data['member_id']][f"dm_{msg_data['days']}_retry_count"] = retry_count + 1
                    await self.log_to_discord(
                        f"🔄 DM retry {retry_count + 1}/{max_retries} for {msg_data['days']}-day message to member {msg_data['member_id']}: {str(e)}"
                    )
                else:
                    # Max retries reached, mark as sent to stop trying
                    AUTO_ROLE_CONFIG["dm_schedule"][msg_data['member_id']][
                        msg_data['sent_key']] = True
                    await self.log_to_discord(
                        f"❌ Failed to send {msg_data['days']}-day DM to member {msg_data['member_id']} after {max_retries} retries: {str(e)}"
                    )

        # Save config if any changes were made
        if messages_to_send:
            await self.save_auto_role_config()

    @tasks.loop(minutes=1)  # Check every minute for real-time DM sending
    async def monday_activation_task(self):
        """Background task to send Monday activation DMs for weekend joiners"""
        if not AUTO_ROLE_CONFIG["enabled"] or not AUTO_ROLE_CONFIG[
                "active_members"]:
            return

        current_time = datetime.now(AMSTERDAM_TZ)
        weekday = current_time.weekday()  # Monday=0
        hour = current_time.hour

        # Only run on Monday between 00:00 and 01:00 to send activation messages
        if weekday != 0 or hour > 1:
            return

        for member_id, data in AUTO_ROLE_CONFIG["active_members"].items():
            try:
                # Only process weekend delayed members who haven't been notified yet
                if (data.get("weekend_delayed", False)
                        and not data.get("monday_notification_sent", False)):

                    guild = self.get_guild(data["guild_id"])
                    if guild:
                        member = guild.get_member(int(member_id))
                        if member:
                            try:
                                activation_message = (
                                    "Hey! The weekend is over, so the trading markets have been opened again. "
                                    "That means your 24-hour welcome gift has officially started. "
                                    "You now have full access to the premium channel. "
                                    "Let's make the most of it by securing some wins together!"
                                )
                                await member.send(activation_message)
                                print(
                                    f"✅ Sent Monday activation DM to {member.display_name}"
                                )

                                # Mark as notified to avoid duplicate messages
                                AUTO_ROLE_CONFIG["active_members"][member_id][
                                    "monday_notification_sent"] = True
                                await self.save_auto_role_config()

                            except discord.Forbidden:
                                print(
                                    f"⚠️ Could not send Monday activation DM to {member.display_name} (DMs disabled)"
                                )
                            except Exception as e:
                                print(
                                    f"❌ Error sending Monday activation DM to {member.display_name}: {str(e)}"
                                )

            except Exception as e:
                print(
                    f"❌ Error processing Monday activation for member {member_id}: {str(e)}"
                )

    async def remove_expired_role(self, member_id):
        """Remove expired role from member and send DM"""
        try:
            data = AUTO_ROLE_CONFIG["active_members"].get(member_id)
            if not data:
                return

            # Get the guild and member
            guild = self.get_guild(data["guild_id"])
            if not guild:
                print(f"❌ Guild not found for member {member_id}")
                del AUTO_ROLE_CONFIG["active_members"][member_id]
                return

            member = guild.get_member(int(member_id))
            if not member:
                print(f"❌ Member {member_id} not found in guild")
                del AUTO_ROLE_CONFIG["active_members"][member_id]
                return

            # Get the role
            role = guild.get_role(data["role_id"])
            if role and role in member.roles:
                await member.remove_roles(role, reason="Auto-role expired")
                await self.log_to_discord(
                    f"✅ Removed expired role '{role.name}' from {member.display_name}"
                )

            # Send DM to the member with the default message
            try:
                default_message = "Hey! Your **24-hour free access** to the premium channel has unfortunately **ran out**. We truly hope that you were able to benefit with us & we hope to see you back soon! For now, feel free to continue following our trade signals in <#1350929790148022324>."
                await member.send(default_message)
                await self.log_to_discord(
                    f"✅ Sent expiration DM to {member.display_name}")
            except discord.Forbidden:
                await self.log_to_discord(
                    f"⚠️ Could not send DM to {member.display_name} (DMs disabled)"
                )
            except Exception as e:
                await self.log_to_discord(
                    f"❌ Error sending DM to {member.display_name}: {str(e)}")

            current_time = datetime.now(AMSTERDAM_TZ)

            # Update role history with expiration time
            if member_id in AUTO_ROLE_CONFIG["role_history"]:
                AUTO_ROLE_CONFIG["role_history"][member_id][
                    "last_expired"] = current_time.isoformat()

            # Schedule follow-up DMs (3, 7, 14 days after expiration)
            AUTO_ROLE_CONFIG["dm_schedule"][member_id] = {
                "role_expired": current_time.isoformat(),
                "guild_id": data["guild_id"],
                "dm_3_sent": False,
                "dm_7_sent": False,
                "dm_14_sent": False
            }

            # Remove from active tracking
            del AUTO_ROLE_CONFIG["active_members"][member_id]

        except Exception as e:
            print(
                f"❌ Error removing expired role for member {member_id}: {str(e)}"
            )
            # Clean up corrupted entry
            if member_id in AUTO_ROLE_CONFIG["active_members"]:
                del AUTO_ROLE_CONFIG["active_members"][member_id]


# Trading pair configurations
PAIR_CONFIG = {
    'XAUUSD': {
        'decimals': 2,
        'pip_value': 0.1
    },
    'GBPJPY': {
        'decimals': 3,
        'pip_value': 0.01
    },
    'GBPUSD': {
        'decimals': 4,
        'pip_value': 0.0001
    },
    'EURUSD': {
        'decimals': 4,
        'pip_value': 0.0001
    },
    'AUDUSD': {
        'decimals': 4,
        'pip_value': 0.0001
    },
    'NZDUSD': {
        'decimals': 4,
        'pip_value': 0.0001
    },
    'US100': {
        'decimals': 1,
        'pip_value': 1.0
    },
    'US500': {
        'decimals': 2,
        'pip_value': 0.1
    },
    'GER40': {
        'decimals': 1,
        'pip_value': 1.0
    },  # Same as US100
    'BTCUSD': {
        'decimals': 1,
        'pip_value': 10
    },  # Same as US100 and GER40
    'GBPCHF': {
        'decimals': 4,
        'pip_value': 0.0001
    },  # Same as GBPUSD
    'USDCHF': {
        'decimals': 4,
        'pip_value': 0.0001
    },  # Same as GBPUSD
    'CHFJPY': {
        'decimals': 3,
        'pip_value': 0.01
    },  # Same as GBPJPY
    'CADJPY': {
        'decimals': 3,
        'pip_value': 0.01
    },  # Same as GBPJPY
    'AUDJPY': {
        'decimals': 3,
        'pip_value': 0.01
    },  # Same as GBPJPY
    'USDCAD': {
        'decimals': 4,
        'pip_value': 0.0001
    },  # Same as GBPUSD
    'GBPCAD': {
        'decimals': 4,
        'pip_value': 0.0001
    },  # Same as GBPUSD
    'EURCAD': {
        'decimals': 4,
        'pip_value': 0.0001
    },  # Same as GBPUSD
    'AUDCAD': {
        'decimals': 4,
        'pip_value': 0.0001
    },  # Same as GBPUSD
    'AUDNZD': {
        'decimals': 4,
        'pip_value': 0.0001
    }  # Same as GBPUSD
}


def calculate_levels(entry_price: float, pair: str, entry_type: str):
    """Calculate TP and SL levels based on pair configuration"""
    if pair in PAIR_CONFIG:
        pip_value = PAIR_CONFIG[pair]['pip_value']
        decimals = PAIR_CONFIG[pair]['decimals']
    else:
        # Default values for unknown pairs
        pip_value = 0.0001
        decimals = 4

    # Calculate pip amounts
    tp1_pips = 20 * pip_value
    tp2_pips = 40 * pip_value
    tp3_pips = 70 * pip_value
    sl_pips = 50 * pip_value

    # Determine direction based on entry type
    is_buy = entry_type.lower().startswith('buy')

    if is_buy:
        tp1 = entry_price + tp1_pips
        tp2 = entry_price + tp2_pips
        tp3 = entry_price + tp3_pips
        sl = entry_price - sl_pips
    else:  # Sell
        tp1 = entry_price - tp1_pips
        tp2 = entry_price - tp2_pips
        tp3 = entry_price - tp3_pips
        sl = entry_price + sl_pips

    # Format prices with correct decimals
    if pair == 'XAUUSD' or pair == 'US500':
        currency_symbol = '$'
    elif pair == 'US100':
        currency_symbol = '$'
    else:
        currency_symbol = '$'

    def format_price(price):
        return f"{currency_symbol}{price:.{decimals}f}"

    return {
        'tp1': format_price(tp1),
        'tp2': format_price(tp2),
        'tp3': format_price(tp3),
        'sl': format_price(sl),
        'entry': format_price(entry_price)
    }


def calculate_live_tracking_levels(live_price: float, pair: str, action: str):
    """Calculate TP and SL levels based on live price for backend tracking"""
    if pair in PAIR_CONFIG:
        pip_value = PAIR_CONFIG[pair]['pip_value']
    else:
        # Default values for unknown pairs
        pip_value = 0.0001

    # Calculate pip amounts (20, 40, 70, 50 as specified by user)
    tp1_pips = 20 * pip_value
    tp2_pips = 40 * pip_value  
    tp3_pips = 70 * pip_value
    sl_pips = 50 * pip_value

    # Determine direction based on action
    is_buy = action.upper() == "BUY"

    if is_buy:
        tp1 = live_price + tp1_pips
        tp2 = live_price + tp2_pips
        tp3 = live_price + tp3_pips
        sl = live_price - sl_pips
    else:  # SELL
        tp1 = live_price - tp1_pips
        tp2 = live_price - tp2_pips
        tp3 = live_price - tp3_pips
        sl = live_price + sl_pips

    return {
        'entry': live_price,
        'tp1': tp1,
        'tp2': tp2,
        'tp3': tp3,
        'sl': sl
    }


def get_remaining_time_display(member_id: str) -> str:
    """Get formatted remaining time display for a member"""
    try:
        data = AUTO_ROLE_CONFIG["active_members"].get(member_id)
        if not data:
            return "Unknown"

        current_time = datetime.now(AMSTERDAM_TZ)

        if data.get("weekend_delayed", False) and "expiry_time" in data:
            # Weekend joiners have specific expiry time (Monday 23:59)
            expiry_time = datetime.fromisoformat(data["expiry_time"])
            if expiry_time.tzinfo is None:
                if PYTZ_AVAILABLE:
                    expiry_time = AMSTERDAM_TZ.localize(expiry_time)
                else:
                    expiry_time = expiry_time.replace(tzinfo=AMSTERDAM_TZ)
            else:
                expiry_time = expiry_time.astimezone(AMSTERDAM_TZ)

            time_remaining = expiry_time - current_time

            if time_remaining.total_seconds() <= 0:
                return None  # Return None for expired members to filter them out

            hours = int(time_remaining.total_seconds() // 3600)
            minutes = int((time_remaining.total_seconds() % 3600) // 60)
            seconds = int(time_remaining.total_seconds() % 60)

            # Check if it's a custom duration
            if data.get("custom_duration", False):
                return f"Custom: {hours}h {minutes}m {seconds}s"
            else:
                return f"Weekend: {hours}h {minutes}m {seconds}s"

        else:
            # Normal member - 24 hours from role_added_time
            role_added_time = datetime.fromisoformat(data["role_added_time"])
            if role_added_time.tzinfo is None:
                if PYTZ_AVAILABLE:
                    role_added_time = AMSTERDAM_TZ.localize(role_added_time)
                else:
                    role_added_time = role_added_time.replace(
                        tzinfo=AMSTERDAM_TZ)
            else:
                role_added_time = role_added_time.astimezone(AMSTERDAM_TZ)

            expiry_time = role_added_time + timedelta(hours=24)
            time_remaining = expiry_time - current_time

            if time_remaining.total_seconds() <= 0:
                return None  # Return None for expired members to filter them out

            hours = int(time_remaining.total_seconds() // 3600)
            minutes = int((time_remaining.total_seconds() % 3600) // 60)
            seconds = int(time_remaining.total_seconds() % 60)

            return f"{hours}h {minutes}m {seconds}s"

    except Exception as e:
        print(f"Error calculating time for member {member_id}: {str(e)}")
        return "ERROR"

    async def setup_owner_permissions(self):
        """Set up command permissions to make commands visible only to bot owner"""
        if not BOT_OWNER_USER_ID:
            print("⚠️ BOT_OWNER_USER_ID not set - skipping permission setup")
            return

        try:
            owner_id = int(BOT_OWNER_USER_ID)
            print(f"🔒 Setting up{version_short}```",
                    inline=False
                )

            # Add table info
            if tables:
                table_list = []
                for table in tables:
                    table_name = table['tablename']
                    count = record_counts.get(table_name, "Unknown")
                    table_list.append(f"• {table_name} ({count} records)")

                if len(table_list) > 10:
                    table_text = "\n".join(table_list[:10]) + f"\n... and {len(table_list) - 10} more"
                else:
                    table_text = "\n".join(table_list)

                embed.add_field(
                    name="📋 Tables",
                    value=table_text,
                    inline=False
                )
            else:
                embed.add_field(
                    name="📋 Tables",
                    value="No tables found",
                    inline=False
                )

            # Add performance test
            start_time = current_time
            await conn.fetchval('SELECT 1')
            end_time = await conn.fetchval('SELECT NOW()')

            embed.add_field(
                name="⚡ Performance",
                value="Database responding normally",
                inline=False
            )

            embed.set_footer(text="Database status check completed")

            await interaction.followup.send(embed=embed)

    except Exception as e:
        embed = discord.Embed(
            title="📊 Database Status",
            description="❌ **Database Connection Error**",
            color=discord.Color.red()
        )

        embed.add_field(
            name="❌ Error Details",
            value=f"```{str(e)[:500]}```",
            inline=False
        )

        embed.add_field(
            name="💡 Troubleshooting",
            value="• Check DATABASE_URL environment variable\n• Verify database service is running\n• Check network connectivity",
            inline=False
        )

        await interaction.followup.send(embed=embed)


@bot.tree.command(name="pricetest", description="Test price retrieval for a trading pair")
@app_commands.describe(pair="Trading pair to test (e.g., XAUUSD, EURUSD)")
async def price_test_command(interaction: discord.Interaction, pair: str):
    """Test live price retrieval for a specific trading pair"""

    if not await owner_check(interaction):
        return

    await interaction.response.defer(ephemeral=True)

    try:
        # Get live price using all APIs for maximum accuracy
        live_price = await bot.get_live_price(pair.upper(), use_all_apis=True)

        if live_price:
            embed = discord.Embed(
                title="💰 Price Test Result",
                description=f"**Pair:** {pair.upper()}\n**Current Price:** ${live_price:.5f}",
                color=discord.Color.green()
            )

            # Add API status info
            embed.add_field(
                name="✅ Status",
                value="Price retrieved successfully using multiple API sources",
                inline=False
            )

            embed.set_footer(text="Price data from live market APIs")

        else:
            embed = discord.Embed(
                title="💰 Price Test Result",
                description=f"**Pair:** {pair.upper()}",
                color=discord.Color.red()
            )

            embed.add_field(
                name="❌ Error",
                value="Could not retrieve price from any API",
                inline=False
            )

            embed.add_field(
                name="💡 Possible Causes",
                value="• Trading pair not supported by APIs\n• API rate limits reached\n• Network connectivity issues\n• Invalid pair format",
                inline=False
            )

        await interaction.followup.send(embed=embed)

    except Exception as e:
        embed = discord.Embed(
            title="💰 Price Test Result",
            description=f"**Pair:** {pair.upper()}",
            color=discord.Color.red()
        )

        embed.add_field(
            name="❌ Error",
            value=f"```{str(e)[:500]}