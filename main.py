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
import math
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
    "enabled": True,  # Always enabled by default
    "role_id": 1384489575187091466,  # Gold pioneers role
    "duration_hours": 72,  # Fixed at 72 hours (3 days)
    "custom_message":
    "Hey! Your **3-day free access** to the <#1350929852299214999> channel has unfortunately **ran out**. We truly hope you were able to benefit with us & we hope to see you back soon! For now, feel free to continue following our trade signals in ⁠<#1350929790148022324>",
    "active_members":
    {},  # member_id: {"role_added_time": datetime, "role_id": role_id, "weekend_delayed": bool, "guild_id": guild_id, "expiry_time": datetime}
    "weekend_pending":
    {},  # member_id: {"join_time": datetime, "guild_id": guild_id} for weekend joiners
    "role_history":
    {},  # member_id: {"first_granted": datetime, "times_granted": int, "last_expired": datetime, "guild_id": guild_id}
    "dm_schedule": {
    }  # member_id: {"role_expired": datetime, "guild_id": guild_id, "dm_3_sent": bool, "dm_7_sent": bool, "dm_14_sent": bool}
}

# Log channel ID for Discord logging
LOG_CHANNEL_ID = 1350888185487429642

# Gold Pioneer role ID for checking membership before sending follow-up DMs
GOLD_PIONEER_ROLE_ID = 1384489575187091466

# Giveaway channel ID (always post giveaways here)
GIVEAWAY_CHANNEL_ID = 1405490561963786271

# Debug channel ID for system debug messages
DEBUG_CHANNEL_ID = 1414220633029611582

# Giveaway debug channel ID for all giveaway-related logs
GIVEAWAY_DEBUG_CHANNEL_ID = 1438105795970732103

# Global storage for active giveaways
ACTIVE_GIVEAWAYS = {}  # giveaway_id: {message_id, participants, settings, etc}

# Global storage for invite tracking
INVITE_TRACKING = {
}  # invite_code: {"nickname": str, "total_joins": int, "total_left": int, "current_members": int, "creator_id": int, "guild_id": int}

# Live price tracking system configuration
PRICE_TRACKING_CONFIG = {
    "enabled":
    True,  # 24/7 monitoring enabled by default
    "excluded_channel_id":
    "1394958907943817326",
    "owner_user_id":
    "462707111365836801",
    "signal_keyword":
    "Trade Signal For:",
    "active_trades": {},  # message_id: {trade_data}
    "api_keys": {
        # Priority order: currencybeacon -> exchangerate_api -> currencylayer -> abstractapi
        "currencybeacon_key": os.getenv("CURRENCYBEACON_KEY", ""),
        "exchangerate_api_key": os.getenv("EXCHANGERATE_API_KEY", ""),
        "currencylayer_key": os.getenv("CURRENCYLAYER_KEY", ""),
        "abstractapi_key": os.getenv("ABSTRACTAPI_KEY", "")
    },
    "api_endpoints": {
        "currencybeacon": "https://api.currencybeacon.com/v1/latest",
        "exchangerate_api": "https://v6.exchangerate-api.com/v6",
        "currencylayer": "https://api.currencylayer.com/live",
        "abstractapi": "https://exchange-rates.abstractapi.com/v1/live"
    },
    "api_priority_order":
    ["currencybeacon", "exchangerate_api", "currencylayer", "abstractapi"],
    "last_price_check": {},  # pair: last_check_timestamp
    "check_interval":
    120,  # 2 minutes - more frequent checks to catch all TP/SL hits
    "api_rotation_index":
    0  # for tracking which API failed (for debugging)
}

# Level system configuration
LEVEL_SYSTEM = {
    "enabled": True,
    "user_data":
    {},  # user_id: {"message_count": int, "current_level": int, "guild_id": guild_id}
    "level_requirements": {
        1: 5,  # Level 1: 5 messages (very easy start)
        2: 20,  # Level 2: 20 messages (easy)
        3: 50,  # Level 3: 50 messages (moderate)
        4: 100,  # Level 4: 100 messages (decent activity)
        5: 200,  # Level 5: 200 messages (good activity)
        6: 400,  # Level 6: 400 messages (high activity)
        7: 700,  # Level 7: 700 messages (very high activity)  
        8: 1200  # Level 8: 1200 messages (maximum activity)
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
        self.db_pool = None
        self.client_session = None
        self.last_online_time = None
        self.last_heartbeat = None

    async def log_to_discord(self, message):
        """Send log message to Discord channel"""
        if self.log_channel:
            try:
                await self.log_channel.send(f"📋 **Bot Log:** {message}")
            except Exception as e:
                print(f"Failed to send log to Discord: {e}")
        # Always print to console as backup
        print(message)

    async def log_giveaway_event(self, message):
        """Send giveaway-related log message to giveaway debug channel"""
        giveaway_debug_channel = self.get_channel(GIVEAWAY_DEBUG_CHANNEL_ID)
        if giveaway_debug_channel:
            try:
                await giveaway_debug_channel.send(
                    f"🎁 **Giveaway Log:** {message}")
            except Exception as e:
                print(f"Failed to send giveaway log to Discord: {e}")
        # Always print to console as backup
        print(f"[GIVEAWAY] {message}")

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

    def calculate_live_tracking_levels(self, live_price: float, pair: str,
                                       action: str):
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

    async def save_bot_status(self):
        """Save bot status to database for offline recovery"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                current_time = datetime.now(AMSTERDAM_TZ)
                await conn.execute(
                    """
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
                result = await conn.fetchrow(
                    "SELECT last_online FROM bot_status WHERE id = 1")
                if result:
                    self.last_online_time = result['last_online']
                    print(
                        f"✅ Loaded last online time: {self.last_online_time}")
        except Exception as e:
            print(f"Failed to load bot status: {e}")

    async def recover_offline_members(self):
        """Check for members who joined while bot was offline and assign auto-roles"""
        if not AUTO_ROLE_CONFIG["enabled"] or not AUTO_ROLE_CONFIG["role_id"]:
            return

        try:
            await self.log_to_discord(
                "🔍 Checking for members who joined while bot was offline...")

            # Get the last known online time from database or use current time - 24 hours as fallback
            offline_check_time = self.last_online_time
            if not offline_check_time:
                # If we don't know when we were last online, check last 24 hours as safety measure
                offline_check_time = datetime.now(AMSTERDAM_TZ) - timedelta(
                    hours=24)

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
                    if member.joined_at and member.joined_at.replace(
                            tzinfo=timezone.utc
                    ) > offline_check_time.astimezone(timezone.utc):

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
                        await member.add_roles(
                            role, reason="Auto-role recovery for offline join")

                        # Determine if it was weekend when they joined
                        if self.is_weekend_time(join_time):
                            # Weekend join - 120 hours (5 days) from join time to account for weekend
                            expiry_time = join_time + timedelta(hours=120)

                            AUTO_ROLE_CONFIG["active_members"][
                                member_id_str] = {
                                    "role_added_time": join_time.isoformat(),
                                    "role_id": AUTO_ROLE_CONFIG["role_id"],
                                    "guild_id": guild.id,
                                    "weekend_delayed": True,
                                    "expiry_time": expiry_time.isoformat()
                                }

                            # Send weekend DM
                            try:
                                weekend_message = (
                                    "**Welcome to FX Pip Pioneers!** As a welcome gift, we've given you "
                                    "**access to the Premium Signals channel for 3 trading days.** Since you joined during the weekend, "
                                    "your access will expire in 5 days (120 hours) to account for the 2 weekend days when the markets are closed. "
                                    "This way, you get the full 3 trading days of premium access. Good luck trading!")
                                await member.send(weekend_message)
                            except discord.Forbidden:
                                await self.log_to_discord(
                                    f"❌ Could not send weekend DM to {member.display_name} (DMs disabled)"
                                )

                        else:
                            # Regular join - 72 hours (3 days) from join time
                            expiry_time = join_time + timedelta(hours=72)

                            AUTO_ROLE_CONFIG["active_members"][
                                member_id_str] = {
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
                                    "**access to the Premium Signals channel for 3 days.** "
                                    "Good luck trading!")
                                await member.send(welcome_message)
                            except discord.Forbidden:
                                await self.log_to_discord(
                                    f"❌ Could not send welcome DM to {member.display_name} (DMs disabled)"
                                )

                        # Record in role history for anti-abuse
                        AUTO_ROLE_CONFIG["role_history"][member_id_str] = {
                            "first_granted": join_time.isoformat(),
                            "times_granted": 1,
                            "last_expired": None,
                            "guild_id": guild.id
                        }

                        recovered_count += 1
                        await self.log_to_discord(
                            f"✅ Recovered offline joiner: {member.display_name}"
                        )

            # Save the updated configuration
            await self.save_auto_role_config()

            if recovered_count > 0:
                await self.log_to_discord(
                    f"🎯 Successfully recovered {recovered_count} members who joined while bot was offline!"
                )
            else:
                await self.log_to_discord(
                    "✅ No offline members found to recover")

        except Exception as e:
            await self.log_to_discord(
                f"❌ Error during offline member recovery: {str(e)}")
            print(f"Offline recovery error: {e}")

    async def recover_offline_dm_reminders(self):
        """Check for DM reminders that should have been sent while bot was offline"""
        if not AUTO_ROLE_CONFIG["enabled"] or not self.db_pool:
            return

        try:
            await self.log_to_discord(
                "🔍 Checking for missed DM reminders while offline...")

            current_time = datetime.now(AMSTERDAM_TZ)
            recovered_dms = 0

            # Check all members in DM schedule for missed reminders
            for member_id_str, dm_data in AUTO_ROLE_CONFIG[
                    "dm_schedule"].items():
                try:
                    role_expired = datetime.fromisoformat(
                        dm_data["role_expired"]).replace(tzinfo=AMSTERDAM_TZ)
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
                    if any(role.id == GOLD_PIONEER_ROLE_ID
                           for role in member.roles):
                        continue

                    # Send missed 3-day DM
                    if not dm_data["dm_3_sent"] and current_time >= dm_3_time:
                        try:
                            dm_message = "Hey! It's been 3 days since your **3-day free access to the Premium Signals channel** ended. We truly hope that you were able to catch good trades with us during that time.\n\nAs you've probably seen, our free signals channel gets **1 free signal per day**, while our **Gold Pioneers** in <#1350929852299214999> receive **6+ high-quality signals per day. That means that our Premium Signals Channel offers way more chances to profit and grow consistently.\n\nWe'd love to **invite you back to Premium Signals Channel,** so you don't miss out on more solid opportunities.\n\n**Feel free to join us again through this link:** <https://whop.com/gold-pioneer/gold-pioneer/>"
                            await member.send(dm_message)
                            AUTO_ROLE_CONFIG["dm_schedule"][member_id_str][
                                "dm_3_sent"] = True
                            recovered_dms += 1
                            await self.log_to_discord(
                                f"📤 Sent missed 3-day DM to {member.display_name}"
                            )
                        except discord.Forbidden:
                            await self.log_to_discord(
                                f"❌ Could not send missed 3-day DM to {member.display_name} (DMs disabled)"
                            )

                    # Send missed 7-day DM
                    if not dm_data["dm_7_sent"] and current_time >= dm_7_time:
                        try:
                            dm_message = "It's been a week since your Premium Signals trial ended. Since then, our **Gold Pioneers have been catching trade setups daily in <#1350929852299214999>**.\n\nIf you found value in just 3 days, imagine what results you could've been seeing by now with full access. It's all about **consistency and staying connected to the right information**.\n\nWe'd like to **personally invite you to rejoin Premium Signals** and get back into the rhythm.\n\n\n**Feel free to join us again through this link:** <https://whop.com/gold-pioneer/gold-pioneer/>"
                            await member.send(dm_message)
                            AUTO_ROLE_CONFIG["dm_schedule"][member_id_str][
                                "dm_7_sent"] = True
                            recovered_dms += 1
                            await self.log_to_discord(
                                f"📤 Sent missed 7-day DM to {member.display_name}"
                            )
                        except discord.Forbidden:
                            await self.log_to_discord(
                                f"❌ Could not send missed 7-day DM to {member.display_name} (DMs disabled)"
                            )

                    # Send missed 14-day DM
                    if not dm_data["dm_14_sent"] and current_time >= dm_14_time:
                        try:
                            dm_message = "Hey! It's been two weeks since your free access to our Premium Signals Channel ended. We hope you've stayed active since then. \n\nIf you've been trading solo or passively following the free channel, you might be feeling the difference. In <#1350929852299214999>, it's not just about more signals. It's about the **structure, support, and smarter decision-making**. That edge can make all the difference over time.\n\nWe'd love to **invite you back into the Premium Signals Channel** and help you start compounding results again.\n\n**Feel free to join us again through this link:** <https://whop.com/gold-pioneer/gold-pioneer/>"
                            await member.send(dm_message)
                            AUTO_ROLE_CONFIG["dm_schedule"][member_id_str][
                                "dm_14_sent"] = True
                            recovered_dms += 1
                            await self.log_to_discord(
                                f"📤 Sent missed 14-day DM to {member.display_name}"
                            )
                        except discord.Forbidden:
                            await self.log_to_discord(
                                f"❌ Could not send missed 14-day DM to {member.display_name} (DMs disabled)"
                            )

                except Exception as e:
                    await self.log_to_discord(
                        f"❌ Error processing missed DM for member {member_id_str}: {str(e)}"
                    )
                    continue

            # Save updated DM schedule
            await self.save_auto_role_config()

            if recovered_dms > 0:
                await self.log_to_discord(
                    f"📬 Successfully sent {recovered_dms} missed DM reminders!"
                )
            else:
                await self.log_to_discord("✅ No missed DM reminders found")

        except Exception as e:
            await self.log_to_discord(
                f"❌ Error during offline DM recovery: {str(e)}")
            print(f"Offline DM recovery error: {e}")

    async def recover_missed_signals(self):
        """Check for trading signals that were sent while bot was offline"""
        if not PRICE_TRACKING_CONFIG["enabled"]:
            return

        try:
            await self.log_to_discord(
                "🔍 Scanning for missed trading signals while offline...")

            # Get the last known online time
            offline_check_time = self.last_online_time
            if not offline_check_time:
                # If we don't know when we were last online, check last 6 hours as safety measure
                offline_check_time = datetime.now(AMSTERDAM_TZ) - timedelta(
                    hours=6)

            recovered_signals = 0

            for guild in self.guilds:
                if not guild:
                    continue

                for channel in guild.text_channels:
                    # Skip excluded channel
                    if str(channel.id
                           ) == PRICE_TRACKING_CONFIG["excluded_channel_id"]:
                        continue

                    try:
                        # Check messages sent while bot was offline
                        async for message in channel.history(
                                after=offline_check_time, limit=100):
                            # Only process signals from owner or bot
                            if not (str(message.author.id)
                                    == PRICE_TRACKING_CONFIG["owner_user_id"]
                                    or message.author.bot):
                                continue

                            # Check if message contains trading signal
                            if PRICE_TRACKING_CONFIG[
                                    "signal_keyword"] not in message.content:
                                continue

                            # Skip if already being tracked (check both memory and database)
                            current_trades = await self.get_active_trades_from_db(
                            )
                            if str(message.id) in current_trades:
                                continue

                            # Parse the signal
                            trade_data = self.parse_signal_message(
                                message.content)
                            if not trade_data:
                                continue

                            # Get historical price at the time the message was sent
                            message_time = message.created_at.astimezone(
                                AMSTERDAM_TZ)
                            historical_price = await self.get_historical_price(
                                trade_data["pair"], message_time)

                            if historical_price:
                                # Calculate tracking levels based on historical price
                                # DEBUG: Verify method exists for recovery process
                                has_method = hasattr(
                                    self, 'calculate_live_tracking_levels')
                                print(
                                    f"🔍 DEBUG (recover_missed_signals) - Method available: {has_method}"
                                )
                                if not has_method:
                                    calc_methods = [
                                        attr for attr in dir(self)
                                        if attr.startswith('calculate')
                                    ]
                                    print(
                                        f"🔍 DEBUG (recover_missed_signals) - Available 'calculate' methods: {calc_methods}"
                                    )

                                live_levels = self.calculate_live_tracking_levels(
                                    historical_price, trade_data["pair"],
                                    trade_data["action"])

                                # Store both Discord and historical prices
                                trade_data["discord_entry"] = trade_data[
                                    "entry"]
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
                                trade_data[
                                    "guild_id"] = guild.id  # Fix: Add missing guild_id for database
                                trade_data["message_id"] = str(message.id)
                                trade_data[
                                    "timestamp"] = message.created_at.isoformat(
                                    )
                                trade_data[
                                    "recovered"] = True  # Mark as recovered signal

                                # Add to active tracking with database persistence
                                await self.save_trade_to_db(
                                    str(message.id), trade_data)
                                recovered_signals += 1

                                print(
                                    f"✅ Recovered signal: {trade_data['pair']} from {message_time.strftime('%Y-%m-%d %H:%M')}"
                                )
                            else:
                                print(
                                    f"⚠️ Could not get historical price for {trade_data['pair']} - skipping recovery"
                                )

                    except Exception as e:
                        print(
                            f"❌ Error scanning {channel.name} for missed signals: {e}"
                        )
                        continue

            if recovered_signals > 0:
                await self.log_to_discord(
                    f"🔄 **Signal Recovery Complete**\n"
                    f"Found and started tracking {recovered_signals} missed trading signals"
                )
            else:
                await self.log_to_discord(
                    "✅ No missed trading signals found during downtime")

        except Exception as e:
            await self.log_to_discord(
                f"❌ Error during missed signal recovery: {str(e)}")
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

            await self.log_to_discord(
                "🔍 Checking for TP/SL hits that occurred while offline...")

            offline_hits_found = 0

            for message_id, trade_data in list(active_trades.items()):
                try:
                    # Get current price to check if any levels were hit using API priority order
                    current_price = await self.get_live_price(
                        trade_data["pair"], use_all_apis=False)
                    if current_price is None:
                        continue

                    # Check if message still exists (cleanup deleted signals)
                    if not await self.check_message_still_exists(
                            message_id, trade_data):
                        await self.remove_trade_from_db(
                            message_id, "message_deleted")
                        continue

                    action = trade_data["action"]
                    entry = trade_data["entry"]
                    tp_hits = trade_data.get('tp_hits', [])

                    # Check for SL hit while offline
                    if action == "BUY" and current_price <= trade_data["sl"]:
                        await self.handle_sl_hit(message_id,
                                                 trade_data,
                                                 offline_hit=True)
                        offline_hits_found += 1
                        continue
                    elif action == "SELL" and current_price >= trade_data["sl"]:
                        await self.handle_sl_hit(message_id,
                                                 trade_data,
                                                 offline_hit=True)
                        offline_hits_found += 1
                        continue

                    # Check for TP hits while offline - detect ALL TPs that have been surpassed
                    tp_levels_hit = []

                    if action == "BUY":
                        if "tp1" not in tp_hits and current_price >= trade_data[
                                "tp1"]:
                            tp_levels_hit.append("tp1")
                        if "tp2" not in tp_hits and current_price >= trade_data[
                                "tp2"]:
                            tp_levels_hit.append("tp2")
                        if "tp3" not in tp_hits and current_price >= trade_data[
                                "tp3"]:
                            tp_levels_hit.append("tp3")
                    elif action == "SELL":
                        if "tp1" not in tp_hits and current_price <= trade_data[
                                "tp1"]:
                            tp_levels_hit.append("tp1")
                        if "tp2" not in tp_hits and current_price <= trade_data[
                                "tp2"]:
                            tp_levels_hit.append("tp2")
                        if "tp3" not in tp_hits and current_price <= trade_data[
                                "tp3"]:
                            tp_levels_hit.append("tp3")

                    # Process all TP hits found during offline period
                    if tp_levels_hit:
                        for tp_level in ["tp1", "tp2", "tp3"]:
                            if tp_level in tp_levels_hit:
                                await self.handle_tp_hit(message_id,
                                                         trade_data,
                                                         tp_level,
                                                         offline_hit=True)
                                offline_hits_found += 1
                        continue  # Skip to next trade after processing all TPs

                    # Check for breakeven hits if TP2 was already hit
                    if trade_data.get("breakeven_active"):
                        if action == "BUY" and current_price <= entry:
                            await self.handle_breakeven_hit(message_id,
                                                            trade_data,
                                                            offline_hit=True)
                            offline_hits_found += 1
                            continue
                        elif action == "SELL" and current_price >= entry:
                            await self.handle_breakeven_hit(message_id,
                                                            trade_data,
                                                            offline_hit=True)
                            offline_hits_found += 1
                            continue

                except Exception as e:
                    continue

            if offline_hits_found > 0:
                await self.log_to_discord(
                    f"⚡ Found and processed {offline_hits_found} TP/SL hits that occurred while offline"
                )
            else:
                await self.log_to_discord("✅ No offline TP/SL hits detected")

        except Exception as e:
            await self.log_to_discord(
                f"❌ Error checking offline TP/SL hits: {str(e)}")
            print(f"Offline TP/SL check error: {e}")

    async def get_historical_price(self, pair: str,
                                   timestamp: datetime) -> Optional[float]:
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

    async def get_time_until_next_refresh(self):
        """Calculate time remaining until next price tracking refresh"""
        if not hasattr(self, '_last_price_check_time'):
            self._last_price_check_time = datetime.now()

        time_since_last = (datetime.now() -
                           self._last_price_check_time).total_seconds()
        time_until_next = max(0,
                              480 - time_since_last)  # 480 seconds = 8 minutes

        if time_until_next <= 0:
            return "Due now"

        minutes = int(time_until_next // 60)
        seconds = int(time_until_next % 60)

        if minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"

    @tasks.loop(
        seconds=480
    )  # 8 minute interval - optimized for 15-minute API refresh cycles
    async def price_tracking_task(self):
        """Background task to monitor live prices for active trades - 24/7 monitoring with upgraded API limits"""
        # Record the time of this price check
        self._last_price_check_time = datetime.now()

        if not PRICE_TRACKING_CONFIG["enabled"]:
            return

        # Check if we're in weekend pause period (Friday 23:00 to Sunday 23:55 Amsterdam time)
        amsterdam_now = datetime.now(AMSTERDAM_TZ)
        weekday = amsterdam_now.weekday()  # 0=Monday, 6=Sunday
        hour = amsterdam_now.hour

        # Weekend pause: Friday 23:00 to Sunday 23:55 (markets closed)
        if (weekday == 4 and hour >= 23) or weekday == 5 or (
                weekday == 6 and hour < 23) or (weekday == 6 and hour == 23
                                                and amsterdam_now.minute < 55):
            return  # Skip tracking during weekend when markets are closed

        # Get active trades from database for 24/7 persistence
        active_trades = await self.get_active_trades_from_db()

        # Log price tracking activity to debug channel (always log, even when no trades)
        debug_channel = self.get_channel(DEBUG_CHANNEL_ID)
        if debug_channel:
            if active_trades:
                await debug_channel.send(
                    f"🔄 **Price Tracking Active** - Checking {len(active_trades)} trades at {amsterdam_now.strftime('%H:%M:%S')}"
                )
            else:
                # Log even when no active trades to confirm task is running
                await debug_channel.send(
                    f"🔄 **Price Tracking Running** - No active trades to check at {amsterdam_now.strftime('%H:%M:%S')}"
                )

        if not active_trades:
            return

        try:
            # Check each active trade
            trades_to_remove = []
            for message_id, trade_data in list(active_trades.items()):
                try:
                    # First, check if the original message still exists
                    message_deleted = await self.check_message_deleted(
                        message_id, trade_data.get("channel_id"))
                    if message_deleted:
                        print(
                            f"📝 Original message deleted for {trade_data['pair']} - removing from tracking"
                        )
                        trades_to_remove.append(message_id)
                        continue

                    # Check if this is a limit order waiting for entry
                    entry_type = trade_data.get("entry_type", "").lower()
                    status = trade_data.get("status", "active")

                    if "limit" in entry_type and status == "pending_entry":
                        # Check if limit entry has been hit
                        entry_hit = await self.check_limit_entry_hit(
                            message_id, trade_data)
                        if not entry_hit:
                            continue  # Still waiting for entry, don't check TP/SL yet

                    # Check if price levels have been hit
                    level_hit = await self.check_price_levels(
                        message_id, trade_data)
                    if level_hit:
                        # Trade was closed, will be removed by the handler
                        continue

                except Exception as e:
                    # Log the error instead of silently removing the trade
                    print(
                        f"❌ Error checking trade {message_id} for {trade_data.get('pair', 'unknown')}: {str(e)}"
                    )
                    debug_channel = self.get_channel(DEBUG_CHANNEL_ID)
                    if debug_channel:
                        await debug_channel.send(
                            f"❌ **Trade Check Error**\nMessage: {message_id[:8]}...\nPair: {trade_data.get('pair', 'unknown')}\nError: {str(e)[:200]}"
                        )
                    # Don't remove the trade, just skip this iteration
                    continue

            # Remove failed trades from database
            for message_id in trades_to_remove:
                await self.remove_trade_from_db(message_id)

        except Exception as e:
            # Send error details to debug channel for price checking failures
            debug_channel = self.get_channel(DEBUG_CHANNEL_ID)
            if debug_channel:
                await debug_channel.send(
                    f"❌ Price level checking failed: {str(e)}")
            print(f"❌ Price level checking error: {str(e)}")

    @tasks.loop(minutes=30)
    async def heartbeat_task(self):
        """Periodic heartbeat to track bot uptime and save status"""
        if self.db_pool:
            try:
                await self.save_bot_status()
                self.last_heartbeat = datetime.now(AMSTERDAM_TZ)
            except Exception as e:
                print(f"Heartbeat error: {e}")

    @tasks.loop(minutes=1)
    async def process_pending_welcome_dms(self):
        """Process pending welcome DMs that are ready to be sent"""
        if not self.db_pool:
            return
        
        try:
            now = datetime.now(AMSTERDAM_TZ)
            async with self.db_pool.acquire() as conn:
                # Get all pending DMs that should be sent now
                pending_dms = await conn.fetch('''
                    SELECT member_id, guild_id, scheduled_send_time
                    FROM pending_welcome_dms
                    WHERE sent = FALSE AND scheduled_send_time <= $1
                    ORDER BY scheduled_send_time ASC
                    LIMIT 10
                ''', now)
                
                for dm in pending_dms:
                    member_id = dm['member_id']
                    guild_id = dm['guild_id']
                    
                    try:
                        # Get the guild and member
                        guild = self.get_guild(guild_id)
                        if not guild:
                            # Guild not found, mark as sent to clean up
                            await conn.execute('''
                                UPDATE pending_welcome_dms SET sent = TRUE
                                WHERE member_id = $1
                            ''', member_id)
                            continue
                        
                        member = guild.get_member(member_id)
                        if not member:
                            # Try fetching member
                            try:
                                member = await guild.fetch_member(member_id)
                            except (discord.NotFound, discord.HTTPException):
                                # Member left or not found, mark as sent to clean up
                                await conn.execute('''
                                    UPDATE pending_welcome_dms SET sent = TRUE
                                    WHERE member_id = $1
                                ''', member_id)
                                await self.log_to_discord(
                                    f"⚠️ Could not send welcome DM to member ID {member_id} (member left or not found)"
                                )
                                continue
                        
                        # Send the welcome DM
                        try:
                            await member.send(WELCOME_DM_MESSAGE)
                            await conn.execute('''
                                UPDATE pending_welcome_dms SET sent = TRUE
                                WHERE member_id = $1
                            ''', member_id)
                            await self.log_to_discord(
                                f"✅ Sent welcome DM to {member.display_name} (ID: {member_id})"
                            )
                        except discord.Forbidden:
                            # User has DMs disabled, mark as sent to clean up
                            await conn.execute('''
                                UPDATE pending_welcome_dms SET sent = TRUE
                                WHERE member_id = $1
                            ''', member_id)
                            await self.log_to_discord(
                                f"⚠️ Could not send welcome DM to {member.display_name} (DMs disabled)"
                            )
                        
                    except Exception as e:
                        # Log error but don't mark as sent so we can retry
                        await self.log_to_discord(
                            f"❌ Error sending welcome DM to member ID {member_id}: {str(e)}"
                        )
                        print(f"Error sending welcome DM to {member_id}: {e}")
                
                # Clean up old sent DMs (older than 7 days)
                cleanup_time = now - timedelta(days=7)
                await conn.execute('''
                    DELETE FROM pending_welcome_dms
                    WHERE sent = TRUE AND created_at < $1
                ''', cleanup_time)
                
        except Exception as e:
            print(f"Error in process_pending_welcome_dms: {e}")
            await self.log_to_discord(f"❌ Error processing pending welcome DMs: {str(e)}")

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

                # Welcome DM config table for new member greetings
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS welcome_dm_config (
                        id SERIAL PRIMARY KEY,
                        enabled BOOLEAN DEFAULT FALSE,
                        delay_minutes INTEGER DEFAULT 5,
                        message TEXT DEFAULT 'Welcome to the server! 🎉'
                    )
                ''')

                # Seed default welcome DM config row
                await conn.execute('''
                    INSERT INTO welcome_dm_config (id, enabled, delay_minutes, message)
                    VALUES (1, FALSE, 5, 'Welcome to the server! 🎉')
                    ON CONFLICT (id) DO NOTHING
                ''')

                # Pending welcome DMs table for persistent tracking
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS pending_welcome_dms (
                        member_id BIGINT PRIMARY KEY,
                        guild_id BIGINT NOT NULL,
                        joined_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        scheduled_send_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        sent BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
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

                # 🛡️ INVITE ABUSE TRACKING: Track individual join events with account age analysis
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS invite_events (
                        id SERIAL PRIMARY KEY,
                        guild_id BIGINT NOT NULL,
                        member_id BIGINT NOT NULL,
                        inviter_id BIGINT,
                        invite_code VARCHAR(20),
                        joined_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        account_created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        suspicious BOOLEAN DEFAULT FALSE,
                        autorole_allowed BOOLEAN DEFAULT TRUE,
                        reason TEXT,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        UNIQUE(guild_id, member_id)
                    )
                ''')

                # 🛡️ INVITE ABUSE TRACKING: Track abuse statistics per inviter
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS inviter_abuse_stats (
                        guild_id BIGINT NOT NULL,
                        inviter_id BIGINT NOT NULL,
                        suspicious_count INTEGER DEFAULT 0,
                        banned_from_autorole BOOLEAN DEFAULT FALSE,
                        first_suspicious_at TIMESTAMP WITH TIME ZONE,
                        banned_at TIMESTAMP WITH TIME ZONE,
                        PRIMARY KEY(guild_id, inviter_id)
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
                        entry_price DECIMAL(30,15) NOT NULL,
                        tp1_price DECIMAL(30,15) NOT NULL,
                        tp2_price DECIMAL(30,15) NOT NULL,
                        tp3_price DECIMAL(30,15) NOT NULL,
                        sl_price DECIMAL(30,15) NOT NULL,
                        discord_entry DECIMAL(30,15),
                        discord_tp1 DECIMAL(30,15),
                        discord_tp2 DECIMAL(30,15),
                        discord_tp3 DECIMAL(30,15),
                        discord_sl DECIMAL(30,15),
                        live_entry DECIMAL(30,15),
                        assigned_api VARCHAR(30) DEFAULT 'currencybeacon',
                        status VARCHAR(50) DEFAULT 'active',
                        tp_hits TEXT DEFAULT '',
                        breakeven_active BOOLEAN DEFAULT FALSE,
                        entry_type VARCHAR(30),
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                ''')

                # Historical/completed trades table for date range analysis
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS completed_trades (
                        message_id VARCHAR(20) PRIMARY KEY,
                        channel_id BIGINT NOT NULL,
                        guild_id BIGINT NOT NULL,
                        pair VARCHAR(20) NOT NULL,
                        action VARCHAR(10) NOT NULL,
                        entry_price DECIMAL(30,15) NOT NULL,
                        tp1_price DECIMAL(30,15) NOT NULL,
                        tp2_price DECIMAL(30,15) NOT NULL,
                        tp3_price DECIMAL(30,15) NOT NULL,
                        sl_price DECIMAL(30,15) NOT NULL,
                        discord_entry DECIMAL(30,15),
                        discord_tp1 DECIMAL(30,15),
                        discord_tp2 DECIMAL(30,15),
                        discord_tp3 DECIMAL(30,15),
                        discord_sl DECIMAL(30,15),
                        live_entry DECIMAL(30,15),
                        assigned_api VARCHAR(30) DEFAULT 'currencybeacon',
                        final_status VARCHAR(100) NOT NULL,
                        tp_hits TEXT DEFAULT '',
                        breakeven_active BOOLEAN DEFAULT FALSE,
                        entry_type VARCHAR(30),
                        manual_overrides TEXT DEFAULT '',
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        completed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        completion_reason VARCHAR(50) NOT NULL
                    )
                ''')

                # Add missing columns for existing tables (migration)
                try:
                    await conn.execute('''
                        ALTER TABLE active_trades 
                        ADD COLUMN IF NOT EXISTS assigned_api VARCHAR(30) DEFAULT 'currencybeacon'
                    ''')
                    print(
                        "✅ Database migration: ensured assigned_api column exists"
                    )
                except Exception as e:
                    print(f"Database migration info: {e}")  # May already exist

                try:
                    await conn.execute('''
                        ALTER TABLE active_trades 
                        ADD COLUMN IF NOT EXISTS entry_type VARCHAR(30)
                    ''')
                    print(
                        "✅ Database migration: ensured entry_type column exists"
                    )
                except Exception as e:
                    print(f"Database migration info: {e}")  # May already exist

                try:
                    await conn.execute('''
                        ALTER TABLE active_trades 
                        ADD COLUMN IF NOT EXISTS manual_overrides TEXT DEFAULT ''
                    ''')
                    print(
                        "✅ Database migration: ensured manual_overrides column exists for sync integration"
                    )
                except Exception as e:
                    print(f"Database migration info: {e}")  # May already exist

                # Fix numeric field overflow by using DECIMAL(30,15) for massive prices with many decimal places
                try:
                    await conn.execute('''
                        ALTER TABLE active_trades 
                        ALTER COLUMN entry_price TYPE DECIMAL(30,15),
                        ALTER COLUMN tp1_price TYPE DECIMAL(30,15),
                        ALTER COLUMN tp2_price TYPE DECIMAL(30,15),
                        ALTER COLUMN tp3_price TYPE DECIMAL(30,15),
                        ALTER COLUMN sl_price TYPE DECIMAL(30,15),
                        ALTER COLUMN discord_entry TYPE DECIMAL(30,15),
                        ALTER COLUMN discord_tp1 TYPE DECIMAL(30,15),
                        ALTER COLUMN discord_tp2 TYPE DECIMAL(30,15),
                        ALTER COLUMN discord_tp3 TYPE DECIMAL(30,15),
                        ALTER COLUMN discord_sl TYPE DECIMAL(30,15),
                        ALTER COLUMN live_entry TYPE DECIMAL(30,15)
                    ''')
                    print(
                        "✅ Database migration: increased precision to DECIMAL(30,15) for any API decimal precision"
                    )
                except Exception as e:
                    print(f"Database migration info: {e}")  # May already exist

                # Missed hits during night pause table for chronological processing
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS missed_hits (
                        id SERIAL PRIMARY KEY,
                        message_id VARCHAR(20) NOT NULL,
                        hit_type VARCHAR(10) NOT NULL,
                        hit_level VARCHAR(10) NOT NULL,
                        hit_price DECIMAL(12,8) NOT NULL,
                        hit_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        processed BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                ''')

                # Active giveaways table for persistent giveaway storage
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS active_giveaways (
                        giveaway_id VARCHAR(50) PRIMARY KEY,
                        message_id BIGINT NOT NULL,
                        channel_id BIGINT NOT NULL,
                        creator_id BIGINT NOT NULL,
                        required_role_id BIGINT NOT NULL,
                        winner_count INTEGER NOT NULL,
                        end_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        participants TEXT DEFAULT '',
                        chosen_winners TEXT DEFAULT '',
                        message_text TEXT NOT NULL,
                        guild_id BIGINT NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
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
            print(
                f"✅ Loaded {len(PRICE_TRACKING_CONFIG['active_trades'])} active trades from database"
            )

            # Load active giveaways from database for persistence
            await self.load_giveaways_from_db()
            print(
                f"✅ Loaded {len(ACTIVE_GIVEAWAYS)} active giveaways from database"
            )

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

    async def load_config_from_db(self):
        """Load configuration from database"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                # Load auto-role config
                config_row = await conn.fetchrow(
                    'SELECT * FROM auto_role_config ORDER BY id DESC LIMIT 1')
                if config_row:
                    AUTO_ROLE_CONFIG["enabled"] = config_row['enabled']
                    AUTO_ROLE_CONFIG["role_id"] = config_row['role_id']
                    AUTO_ROLE_CONFIG["duration_hours"] = config_row[
                        'duration_hours']
                    if config_row['custom_message']:
                        AUTO_ROLE_CONFIG["custom_message"] = config_row[
                            'custom_message']

                # Load active members
                active_rows = await conn.fetch('SELECT * FROM active_members')
                for row in active_rows:
                    AUTO_ROLE_CONFIG["active_members"][str(
                        row['member_id'])] = {
                            "role_added_time":
                            row['role_added_time'].isoformat(),
                            "role_id":
                            row['role_id'],
                            "guild_id":
                            row['guild_id'],
                            "weekend_delayed":
                            row['weekend_delayed'],
                            "expiry_time":
                            row['expiry_time'].isoformat()
                            if row['expiry_time'] else None,
                            "custom_duration":
                            row['custom_duration']
                        }

                # Load weekend pending
                weekend_rows = await conn.fetch('SELECT * FROM weekend_pending'
                                                )
                for row in weekend_rows:
                    AUTO_ROLE_CONFIG["weekend_pending"][str(
                        row['member_id'])] = {
                            "join_time": row['join_time'].isoformat(),
                            "guild_id": row['guild_id']
                        }

                # Load role history
                history_rows = await conn.fetch('SELECT * FROM role_history')
                for row in history_rows:
                    AUTO_ROLE_CONFIG["role_history"][str(row['member_id'])] = {
                        "first_granted":
                        row['first_granted'].isoformat(),
                        "times_granted":
                        row['times_granted'],
                        "last_expired":
                        row['last_expired'].isoformat()
                        if row['last_expired'] else None,
                        "guild_id":
                        row['guild_id']
                    }

                # Load DM schedule
                dm_rows = await conn.fetch('SELECT * FROM dm_schedule')
                for row in dm_rows:
                    AUTO_ROLE_CONFIG["dm_schedule"][str(row['member_id'])] = {
                        "role_expired": row['role_expired'].isoformat(),
                        "guild_id": row['guild_id'],
                        "dm_3_sent": row['dm_3_sent'],
                        "dm_7_sent": row['dm_7_sent'],
                        "dm_14_sent": row['dm_14_sent']
                    }

                print("✅ Configuration loaded from database")

        except Exception as e:
            print(f"❌ Failed to load config from database: {e}")

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
                                "nickname":
                                f"Pre-existing-{invite.code[:8]}",
                                "creator_id":
                                invite.inviter.id if invite.inviter else 0,
                                "total_joins":
                                invite.uses or 0,  # Start with current usage
                                "total_left":
                                0,  # Can't backtrack left members
                                "current_members":
                                invite.uses or 0,  # Assume all are still here
                                "guild_id":
                                guild.id,
                                "created_at":
                                invite.created_at.isoformat()
                                if invite.created_at else None,
                                "max_uses":
                                invite.max_uses,
                                "temporary":
                                invite.temporary,
                                "backtracked":
                                True  # Mark as backtracked
                            }
                            backtracked_count += 1

                except Exception as e:
                    print(
                        f"❌ Error backtracking invites for guild {guild.name}: {e}"
                    )
                    continue

            if backtracked_count > 0:
                print(
                    f"✅ Backtracked {backtracked_count} existing server invites for monitoring"
                )
                await self.log_to_discord(
                    f"🔄 **Invite Backtracking Complete**\n"
                    f"Started monitoring {backtracked_count} existing server invites"
                )
            else:
                print("📋 No new invites found to backtrack")

        except Exception as e:
            print(f"❌ Error during invite backtracking: {e}")

    async def on_ready(self):
        print("🎉 DISCORD BOT READY EVENT TRIGGERED!")
        print(f"   Bot user: {self.user}")
        print(f"   Bot ID: {self.user.id if self.user else 'None'}")
        print(
            f"   Bot discriminator: {self.user.discriminator if self.user else 'None'}"
        )
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
                    print(
                        f"🔒 Bot commands restricted to owner ID: {BOT_OWNER_USER_ID}"
                    )
                else:
                    print(
                        "⚠️ BOT_OWNER_USER_ID not set - all commands blocked for security"
                    )
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
            print("🔄 Price tracking task started - 8 minute intervals (480s)")
            debug_channel = self.get_channel(DEBUG_CHANNEL_ID)
            if debug_channel:
                await debug_channel.send(
                    "🚀 **Price Tracking Started** - Monitoring every 8 minutes (480s) for thorough TP/SL detection"
                )

        # Check for TP/SL hits that occurred while offline
        await self.check_offline_tp_sl_hits()

        # Database initialization is now handled in setup_hook

        # Set up Discord logging channel
        self.log_channel = self.get_channel(LOG_CHANNEL_ID)
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
                    f"✅ Cached {len(self._cached_invites[guild.id])} invites for {guild.name}"
                )
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
            
            # Start welcome DM processing task
            if not hasattr(self, 'welcome_dm_task_started'):
                self.process_pending_welcome_dms.start()
                self.welcome_dm_task_started = True
                print("✅ Welcome DM processing task started")

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
                await self.log_channel.send(
                    f"❌ Bot Error in event '{event}': {str(args[0]) if args else 'Unknown error'}"
                )
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
        """Handle new member joins, assign auto-role if enabled, and schedule welcome DM"""
        try:
            # Handle welcome DM scheduling (independent of auto-role)
            if self.db_pool:
                async with self.db_pool.acquire() as conn:
                    config = await conn.fetchrow(
                        'SELECT * FROM welcome_dm_config WHERE id = 1')
                    if config and config['enabled']:
                        delay_minutes = config['delay_minutes']
                        
                        # Save to database for persistent tracking
                        joined_at = datetime.now(AMSTERDAM_TZ)
                        scheduled_send_time = joined_at + timedelta(minutes=delay_minutes)
                        
                        await conn.execute('''
                            INSERT INTO pending_welcome_dms (member_id, guild_id, joined_at, scheduled_send_time, sent)
                            VALUES ($1, $2, $3, $4, FALSE)
                            ON CONFLICT (member_id) DO UPDATE
                            SET scheduled_send_time = EXCLUDED.scheduled_send_time,
                                sent = FALSE
                        ''', member.id, member.guild.id, joined_at, scheduled_send_time)
                        
                        await self.log_to_discord(
                            f"⏰ Scheduled welcome DM for {member.display_name} - will send at {scheduled_send_time.strftime('%Y-%m-%d %H:%M:%S')} (in {delay_minutes} minutes)"
                        )

            # Handle auto-role (only if enabled)
            if not AUTO_ROLE_CONFIG["enabled"] or not AUTO_ROLE_CONFIG[
                    "role_id"]:
                return
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
                await self.track_member_join_via_invite(
                    member, used_invite.code)

                # Initialize tracking for this invite if not already tracked
                if used_invite.code not in INVITE_TRACKING:
                    INVITE_TRACKING[used_invite.code] = {
                        "nickname":
                        f"Invite-{used_invite.code[:8]}",  # Default nickname
                        "total_joins":
                        0,
                        "total_left":
                        0,
                        "current_members":
                        0,
                        "creator_id":
                        used_invite.inviter.id if used_invite.inviter else 0,
                        "guild_id":
                        member.guild.id,
                        "created_at":
                        datetime.now(AMSTERDAM_TZ).isoformat(),
                        "last_updated":
                        datetime.now(AMSTERDAM_TZ).isoformat()
                    }

            # If we found the invite and it was created by a bot, ignore this member
            if used_invite and used_invite.inviter and used_invite.inviter.bot:
                await self.log_to_discord(
                    f"🤖 Ignoring {member.display_name} - joined via bot invite from {used_invite.inviter.display_name}"
                )
                return

            # 🛡️ ANTI-ABUSE CHECK: Check if member is eligible for timedautorole
            inviter_id = used_invite.inviter.id if used_invite and used_invite.inviter else None
            invite_code = used_invite.code if used_invite else None

            eligibility = await self.check_timedautorole_eligibility(
                member, inviter_id, invite_code)

            if not eligibility["allowed"]:
                await self.log_to_discord(
                    f"🚫 **AUTOROLE BLOCKED** - {member.display_name}\n"
                    f"📋 Reason: {eligibility['reason']}\n"
                    f"👤 Inviter: <@{inviter_id}> {'(BANNED)' if eligibility['inviter_banned'] else ''}\n"
                    f"⚠️ Suspicious: {'Yes' if eligibility['suspicious'] else 'No'}"
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
                # Weekend join - 120 hours (5 days) from join time to account for weekend
                # This way weekend joiners still get exactly 3 trading days worth of trial
                expiry_time = join_time + timedelta(hours=120)

                AUTO_ROLE_CONFIG["active_members"][member_id_str] = {
                    "role_added_time": join_time.isoformat(),
                    "role_id": AUTO_ROLE_CONFIG["role_id"],
                    "guild_id": member.guild.id,
                    "weekend_delayed": True,
                    "expiry_time": expiry_time.isoformat()
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
                        "**Welcome to FX Pip Pioneers!** As a welcome gift, we've given you "
                        "**access to the Premium Signals channel for 3 trading days.** Since you joined during the weekend, "
                        "your access will expire in 5 days (120 hours) to account for the 2 weekend days when the markets are closed. "
                        "This way, you get the full 3 trading days of premium access. Good luck trading!")
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
                    f"✅ Auto-role '{role.name}' added to {member.display_name} (120h countdown - expires {expiry_time.strftime('%Y-%m-%d %H:%M')})"
                )

            else:
                # Normal join - immediate 72-hour (3 day) countdown
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
                        "**:star2: Welcome to FX Pip Pioneers! :star2:**\n\n"
                        ":white_check_mark: As a welcome gift, we've given you access to our **Premium Signals channel for 3 days.** "
                        "That means that you can immediately start profiting from the **6+ trade signals** we send per day in <#1384668129036075109>!\n\n"
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
                    f"✅ Auto-role '{role.name}' added to {member.display_name} (72h countdown starts now)"
                )

            # Save the updated config
            await self.save_auto_role_config()

        except discord.Forbidden:
            await self.log_to_discord(
                f"❌ No permission to assign role to {member.display_name}")
        except Exception as e:
            await self.log_to_discord(
                f"❌ Error assigning auto-role to {member.display_name}: {str(e)}"
            )

    async def on_member_remove(self, member):
        """Handle member leaving the server"""
        try:
            # Track the member leaving for invite statistics
            await self.track_member_leave(member)
            await self.log_to_discord(
                f"👋 {member.display_name} left the server - invite statistics updated"
            )
        except Exception as e:
            await self.log_to_discord(
                f"❌ Error tracking member leave for {member.display_name}: {str(e)}"
            )

    async def debug_to_channel(self,
                               step: str,
                               details: str,
                               status: str = "ℹ️"):
        """Send debugging information to specific Discord channel"""
        debug_channel_id = str(DEBUG_CHANNEL_ID)
        try:
            channel = self.get_channel(int(debug_channel_id))
            if channel:
                embed = discord.Embed(
                    title=f"{status} Signal Tracking Debug",
                    description=f"**Step:** {step}\n**Details:** {details}",
                    color=0x00ff00 if status == "✅" else
                    0xff0000 if status == "❌" else 0x0099ff,
                    timestamp=datetime.now())
                await channel.send(embed=embed)
        except Exception as e:
            print(f"Debug channel error: {e}")

    async def on_message(self, message):
        """Handle messages for level system and price tracking"""
        # Check for trading signals (only from owner or bot)
        if (PRICE_TRACKING_CONFIG["enabled"] and str(message.channel.id)
                != PRICE_TRACKING_CONFIG["excluded_channel_id"] and
            (str(message.author.id) == PRICE_TRACKING_CONFIG["owner_user_id"]
             or message.author.bot) and PRICE_TRACKING_CONFIG["signal_keyword"]
                in message.content):

            # Skip tracking for US100 and GER40 as per user requirements
            if "US100" in message.content.upper(
            ) or "GER40" in message.content.upper():
                await self.debug_to_channel(
                    "SIGNAL SKIPPED",
                    f"Skipping signal tracking for US100/GER40 as requested by user",
                    "⏭️")
                return

            await self.debug_to_channel(
                "1. SIGNAL DETECTED",
                f"Message from: {message.author.name} ({message.author.id})\n"
                + f"Channel: {message.channel.name} ({message.channel.id})\n" +
                f"Content: {message.content[:500]}...")

            try:
                # Parse the signal message
                await self.debug_to_channel(
                    "2. PARSING SIGNAL", "Starting to parse signal message...")
                trade_data = self.parse_signal_message(message.content)

                if trade_data:
                    await self.debug_to_channel(
                        "2. PARSING SIGNAL", f"✅ Successfully parsed:\n" +
                        f"Pair: {trade_data.get('pair')}\n" +
                        f"Action: {trade_data.get('action')}\n" +
                        f"Entry: {trade_data.get('entry')}\n" +
                        f"TP1: {trade_data.get('tp1')}\n" +
                        f"TP2: {trade_data.get('tp2')}\n" +
                        f"TP3: {trade_data.get('tp3')}\n" +
                        f"SL: {trade_data.get('sl')}", "✅")

                    # Determine which API works for this pair and assign it permanently
                    await self.debug_to_channel(
                        "3. API ASSIGNMENT",
                        f"Testing APIs for pair: {trade_data['pair']}")
                    assigned_api = await self.get_working_api_for_pair(
                        trade_data["pair"])
                    trade_data["assigned_api"] = assigned_api
                    await self.debug_to_channel(
                        "3. API ASSIGNMENT",
                        f"✅ Assigned API: {assigned_api} for {trade_data['pair']}",
                        "✅")

                    # Get live price using the assigned API for consistency, with fallback
                    await self.debug_to_channel(
                        "4. LIVE PRICE RETRIEVAL",
                        f"Getting live price for {trade_data['pair']} using {assigned_api}"
                    )
                    live_price = await self.get_live_price(
                        trade_data["pair"], specific_api=assigned_api)

                    # If assigned API fails, try fallback
                    if live_price is None:
                        await self.debug_to_channel(
                            "4. LIVE PRICE RETRIEVAL",
                            f"❌ Assigned API {assigned_api} failed, trying fallback...",
                            "⚠️")
                        live_price = await self.get_live_price(
                            trade_data["pair"], use_all_apis=False)

                    if live_price:
                        await self.debug_to_channel(
                            "4. LIVE PRICE RETRIEVAL",
                            f"✅ Got live price: {live_price} for {trade_data['pair']}",
                            "✅")

                        # Calculate live-price-based TP/SL levels for tracking
                        await self.debug_to_channel(
                            "5. LEVEL CALCULATION",
                            "Calculating live tracking levels...")

                        # DEBUG: Verify method exists (helpful for future troubleshooting)
                        has_method = hasattr(self,
                                             'calculate_live_tracking_levels')
                        await self.debug_to_channel(
                            "5. LEVEL CALCULATION",
                            f"🔍 DEBUG - Method available: {has_method}")

                        if not has_method:
                            calc_methods = [
                                attr for attr in dir(self)
                                if attr.startswith('calculate')
                            ]
                            await self.debug_to_channel(
                                "5. LEVEL CALCULATION",
                                f"🔍 DEBUG - Available 'calculate' methods: {calc_methods}"
                            )

                        live_levels = self.calculate_live_tracking_levels(
                            live_price, trade_data["pair"],
                            trade_data["action"])

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

                        await self.debug_to_channel(
                            "5. LEVEL CALCULATION", f"✅ Calculated levels:\n" +
                            f"Live Entry: {live_price}\n" +
                            f"Live TP1: {live_levels['tp1']}\n" +
                            f"Live TP2: {live_levels['tp2']}\n" +
                            f"Live TP3: {live_levels['tp3']}\n" +
                            f"Live SL: {live_levels['sl']}", "✅")
                    else:
                        await self.debug_to_channel(
                            "4. LIVE PRICE RETRIEVAL",
                            f"❌ Failed to get live price for {trade_data['pair']} from all APIs",
                            "❌")

                    # Add channel and message info
                    trade_data["channel_id"] = message.channel.id
                    trade_data[
                        "guild_id"] = message.guild.id if message.guild else None
                    trade_data["message_id"] = str(message.id)
                    trade_data["timestamp"] = message.created_at.isoformat()

                    # Add to active trades with database persistence
                    await self.debug_to_channel(
                        "6. DATABASE STORAGE",
                        f"Saving trade to database with message ID: {message.id}"
                    )
                    await self.save_trade_to_db(str(message.id), trade_data)
                    await self.debug_to_channel(
                        "6. DATABASE STORAGE",
                        f"✅ Trade saved to database successfully", "✅")

                    # IMMEDIATE TRACKING: Start checking this trade right away instead of waiting up to 2 minutes
                    await self.debug_to_channel(
                        "7. IMMEDIATE TRACKING",
                        f"Starting immediate price monitoring for {trade_data['pair']}",
                        "🔄")

                    # Do an immediate price check to verify current position vs levels
                    await self.check_single_trade_immediately(
                        str(message.id), trade_data)

                    await self.debug_to_channel(
                        "8. TRACKING ACTIVATED",
                        f"✅ Signal tracking activated for {trade_data['pair']} {trade_data['action']}\n"
                        +
                        f"Entry: {trade_data.get('live_entry', 'No Price')}\n"
                        + f"Assigned API: {assigned_api}\n" +
                        f"Status: Active tracking enabled", "✅")

                    await self.log_to_discord(
                        f"✅ Started tracking {trade_data['pair']} {trade_data['action']} signal"
                    )
                    print(
                        f"🔔 NEW SIGNAL DETECTED: {trade_data['pair']} {trade_data['action']} @ {trade_data.get('live_entry', 'No Price')}"
                    )
                else:
                    await self.debug_to_channel(
                        "2. PARSING SIGNAL",
                        "❌ Failed to parse signal - invalid format or missing data",
                        "❌")
            except Exception as e:
                import traceback
                full_traceback = traceback.format_exc()

                await self.debug_to_channel(
                    "ERROR",
                    f"❌ Exception during signal processing: {str(e)}\n" +
                    f"Error type: {type(e).__name__}", "❌")

                # Add detailed traceback for troubleshooting
                await self.debug_to_channel(
                    "ERROR",
                    f"🔍 FULL TRACEBACK:\n```\n{full_traceback[:1800]}\n```")

                # Method availability check at error time
                has_method = hasattr(self, 'calculate_live_tracking_levels')
                await self.debug_to_channel(
                    "ERROR",
                    f"🔍 ERROR DEBUG - Method available at error time: {has_method}"
                )

                print(f"❌ Error processing signal: {str(e)}")
                print(f"🔍 Full traceback: {full_traceback}")
                await self.log_to_discord(
                    f"❌ Error processing signal: {str(e)}")

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
                    await conn.execute(
                        '''
                        INSERT INTO user_levels (user_id, message_count, current_level, guild_id)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (user_id) DO UPDATE SET
                            message_count = $2,
                            current_level = $3,
                            guild_id = $4
                    ''', int(user_id), data["message_count"],
                        data["current_level"], data["guild_id"])

        except Exception as e:
            print(f"❌ Error saving level system to database: {str(e)}")

    async def load_level_system(self):
        """Load level system data from database"""
        if not self.db_pool:
            return  # No database available

        try:
            async with self.db_pool.acquire() as conn:
                # Load user level data
                rows = await conn.fetch(
                    'SELECT user_id, message_count, current_level, guild_id FROM user_levels'
                )
                for row in rows:
                    LEVEL_SYSTEM["user_data"][str(row['user_id'])] = {
                        "message_count": row['message_count'],
                        "current_level": row['current_level'],
                        "guild_id": row['guild_id']
                    }

            if LEVEL_SYSTEM["user_data"]:
                print(
                    f"✅ Loaded level data for {len(LEVEL_SYSTEM['user_data'])} users"
                )
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
                    print(
                        f"✅ Loaded invite tracking data for {len(INVITE_TRACKING)} invites"
                    )
                else:
                    print(
                        "📋 No existing invite tracking data found - starting fresh"
                    )

        except Exception as e:
            print(f"❌ Error loading invite tracking from database: {str(e)}")

    async def save_giveaway_to_db(self, giveaway_id: str, giveaway_data: dict):
        """Save a giveaway to database for persistence across bot restarts"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                # Convert datetime objects to ensure they're timezone-aware
                end_time = giveaway_data['end_time']
                if isinstance(end_time, str):
                    end_time = datetime.fromisoformat(end_time)
                if end_time.tzinfo is None:
                    end_time = end_time.replace(tzinfo=AMSTERDAM_TZ)

                # Convert lists to comma-separated strings for storage
                participants_str = ','.join(
                    str(p) for p in giveaway_data.get('participants', []))
                chosen_winners_str = ','.join(
                    str(w) for w in giveaway_data.get('chosen_winners', []))

                # Get message text from settings
                message_text = giveaway_data.get('settings',
                                                 {}).get('message', '')

                # Get guild_id from channel or settings
                guild_id = giveaway_data.get('guild_id')
                if not guild_id:
                    channel = self.get_channel(giveaway_data['channel_id'])
                    guild_id = channel.guild.id if channel else 0

                await conn.execute(
                    '''
                    INSERT INTO active_giveaways 
                    (giveaway_id, message_id, channel_id, creator_id, required_role_id, 
                     winner_count, end_time, participants, chosen_winners, message_text, guild_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (giveaway_id) DO UPDATE SET
                        participants = $8,
                        chosen_winners = $9
                    ''', giveaway_id, giveaway_data['message_id'],
                    giveaway_data['channel_id'], giveaway_data['creator_id'],
                    giveaway_data['required_role_id'],
                    giveaway_data['winner_count'], end_time, participants_str,
                    chosen_winners_str, message_text, guild_id)

                print(f"✅ Saved giveaway {giveaway_id} to database")

        except Exception as e:
            print(f"❌ Error saving giveaway to database: {str(e)}")
            await self.log_to_discord(
                f"❌ Error saving giveaway {giveaway_id} to database: {str(e)}")

    async def load_giveaways_from_db(self):
        """Load active giveaways from database for persistence across bot restarts"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                # Clear existing in-memory giveaways first to avoid duplicates
                ACTIVE_GIVEAWAYS.clear()

                # Load all active giveaways that haven't ended yet
                current_time = datetime.now(AMSTERDAM_TZ)
                rows = await conn.fetch(
                    'SELECT * FROM active_giveaways WHERE end_time > $1 ORDER BY created_at DESC',
                    current_time)

                loaded_count = 0
                for row in rows:
                    giveaway_id = row['giveaway_id']

                    # Convert database row to giveaway_data format
                    participants = []
                    if row['participants']:
                        participants = [
                            int(p) for p in row['participants'].split(',') if p
                        ]

                    chosen_winners = []
                    if row['chosen_winners']:
                        chosen_winners = [
                            int(w) for w in row['chosen_winners'].split(',')
                            if w
                        ]

                    # Reconstruct end_time as timezone-aware datetime
                    end_time = row['end_time']
                    if end_time.tzinfo is None:
                        end_time = end_time.replace(tzinfo=AMSTERDAM_TZ)

                    # Reconstruct giveaway data structure
                    giveaway_data = {
                        'message_id': row['message_id'],
                        'channel_id': row['channel_id'],
                        'creator_id': row['creator_id'],
                        'required_role_id': row['required_role_id'],
                        'winner_count': row['winner_count'],
                        'end_time': end_time,
                        'participants': participants,
                        'chosen_winners': chosen_winners,
                        'guild_id': row['guild_id'],
                        'settings': {
                            'message': row['message_text'],
                            'winners': row['winner_count']
                        }
                    }

                    # Store in memory
                    ACTIVE_GIVEAWAYS[giveaway_id] = giveaway_data

                    # Reschedule the giveaway end
                    asyncio.create_task(schedule_giveaway_end(giveaway_id))

                    loaded_count += 1
                    print(
                        f"✅ Loaded giveaway {giveaway_id} (ends in {(end_time - current_time).total_seconds() / 3600:.1f} hours)"
                    )

                if loaded_count > 0:
                    print(
                        f"✅ Loaded {loaded_count} active giveaways from database"
                    )
                    await self.log_to_discord(
                        f"🎉 Loaded {loaded_count} active giveaways from database"
                    )
                else:
                    print("📋 No active giveaways found in database")

        except Exception as e:
            print(f"❌ Error loading giveaways from database: {str(e)}")
            await self.log_to_discord(
                f"❌ Error loading giveaways from database: {str(e)}")

    async def remove_giveaway_from_db(self, giveaway_id: str):
        """Remove a giveaway from database when it ends"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    'DELETE FROM active_giveaways WHERE giveaway_id = $1',
                    giveaway_id)
                print(f"✅ Removed giveaway {giveaway_id} from database")

        except Exception as e:
            print(f"❌ Error removing giveaway from database: {str(e)}")

    async def load_active_trades_from_db(self):
        """Load active trading signals from database for 24/7 persistence"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                # Clear existing in-memory trades first to avoid duplicates
                PRICE_TRACKING_CONFIG["active_trades"].clear()

                # Load all active trades from database
                rows = await conn.fetch(
                    'SELECT * FROM active_trades ORDER BY created_at DESC')

                # Send debugging to Discord channel
                debug_channel = self.get_channel(DEBUG_CHANNEL_ID)
                if debug_channel:
                    await debug_channel.send(
                        f"🔍 DEBUG (load_active_trades_from_db): Found {len(rows)} rows in active_trades table"
                    )

                for row in rows:
                    # Convert database row to trade_data format
                    trade_data = {
                        "pair":
                        row['pair'],
                        "action":
                        row['action'],
                        "entry":
                        float(row['entry_price']),
                        "tp1":
                        float(row['tp1_price']),
                        "tp2":
                        float(row['tp2_price']),
                        "tp3":
                        float(row['tp3_price']),
                        "sl":
                        float(row['sl_price']),
                        "discord_entry":
                        float(row['discord_entry'])
                        if row['discord_entry'] else None,
                        "discord_tp1":
                        float(row['discord_tp1'])
                        if row['discord_tp1'] else None,
                        "discord_tp2":
                        float(row['discord_tp2'])
                        if row['discord_tp2'] else None,
                        "discord_tp3":
                        float(row['discord_tp3'])
                        if row['discord_tp3'] else None,
                        "discord_sl":
                        float(row['discord_sl'])
                        if row['discord_sl'] else None,
                        "live_entry":
                        float(row['live_entry'])
                        if row['live_entry'] else None,
                        "assigned_api":
                        row.get('assigned_api', 'currencybeacon'),
                        "status":
                        row['status'],
                        "tp_hits":
                        [tp for tp in row['tp_hits'].split(',')
                         if tp] if row['tp_hits'] else [],
                        "breakeven_active":
                        row['breakeven_active'],
                        "entry_type":
                        row.get('entry_type'),  # Add entry_type field
                        "manual_overrides": [
                            mo for mo in row.get('manual_overrides', '').split(
                                ',') if mo
                        ] if row.get('manual_overrides') else
                        [],  # Add manual_overrides field
                        "channel_id":
                        row['channel_id'],
                        "guild_id":
                        row['guild_id'],
                        "message_id":
                        row['message_id'],
                        "created_at":
                        row['created_at'].isoformat(),
                        "last_updated":
                        row['last_updated'].isoformat()
                    }

                    # Store in memory for quick access
                    PRICE_TRACKING_CONFIG["active_trades"][
                        row['message_id']] = trade_data

                if len(rows) > 0:
                    print(f"✅ Loaded {len(rows)} active trades from database")
                else:
                    print("📋 No active trades found in database")

        except Exception as e:
            print(f"❌ Error loading active trades from database: {str(e)}")
            await self.log_to_discord(
                f"❌ Error loading active trades from database: {str(e)}")

    async def save_trade_to_db(self, message_id: str, trade_data: dict):
        """Save a new trading signal to database for persistence"""
        # Always save to memory for tracking (works with or without database)
        PRICE_TRACKING_CONFIG["active_trades"][message_id] = trade_data

        # Also save to database if available
        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.execute(
                        '''
                        INSERT INTO active_trades (
                            message_id, channel_id, guild_id, pair, action,
                            entry_price, tp1_price, tp2_price, tp3_price, sl_price,
                            discord_entry, discord_tp1, discord_tp2, discord_tp3, discord_sl,
                            live_entry, assigned_api, status, tp_hits, breakeven_active, entry_type, manual_overrides
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
                    ''', message_id, trade_data.get("channel_id"),
                        trade_data.get("guild_id"), trade_data["pair"],
                        trade_data["action"], trade_data["entry"],
                        trade_data["tp1"], trade_data["tp2"],
                        trade_data["tp3"], trade_data["sl"],
                        trade_data.get("discord_entry"),
                        trade_data.get("discord_tp1"),
                        trade_data.get("discord_tp2"),
                        trade_data.get("discord_tp3"),
                        trade_data.get("discord_sl"),
                        trade_data.get("live_entry"),
                        trade_data.get("assigned_api", "currencybeacon"),
                        trade_data.get("status", "active"),
                        ','.join(trade_data.get("tp_hits", [])),
                        trade_data.get("breakeven_active", False),
                        trade_data.get("entry_type"),
                        ','.join(trade_data.get("manual_overrides", [])))
                # Send success confirmation to debug channel
                debug_channel = self.get_channel(DEBUG_CHANNEL_ID)
                if debug_channel:
                    await debug_channel.send(
                        f"✅ Database INSERT successful for message_id: {message_id}"
                    )
            except Exception as e:
                # Send error details to debug channel instead of silently failing
                debug_channel = self.get_channel(DEBUG_CHANNEL_ID)
                if debug_channel:
                    await debug_channel.send(
                        f"❌ Database INSERT failed for message_id {message_id}: {str(e)}"
                    )
                print(f"❌ Database save error: {str(e)}")

    async def update_trade_in_db(self, message_id: str, trade_data: dict):
        """Update an existing trade in database"""

        # Always update in-memory first
        PRICE_TRACKING_CONFIG["active_trades"][message_id] = trade_data

        # Also update database if available
        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.execute(
                        '''
                        UPDATE active_trades SET 
                            status = $2, tp_hits = $3, breakeven_active = $4, manual_overrides = $5, last_updated = NOW()
                        WHERE message_id = $1
                    ''', message_id, trade_data.get("status", "active"),
                        ','.join(trade_data.get("tp_hits", [])),
                        trade_data.get("breakeven_active", False),
                        ','.join(trade_data.get("manual_overrides", [])))

            except Exception as e:
                # Send error details to debug channel instead of silently failing
                debug_channel = self.get_channel(DEBUG_CHANNEL_ID)
                if debug_channel:
                    await debug_channel.send(
                        f"❌ Database UPDATE failed for message_id {message_id}: {str(e)}"
                    )
                print(f"❌ Database update error: {str(e)}")

    async def update_limit_trade_in_db(self, message_id: str,
                                       trade_data: dict):
        """Update a limit trade in database with new price levels after entry hit"""
        # Always update in-memory first
        PRICE_TRACKING_CONFIG["active_trades"][message_id] = trade_data

        # Also update database if available
        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.execute(
                        '''
                        UPDATE active_trades SET 
                            entry_price = $2, tp1_price = $3, tp2_price = $4, tp3_price = $5, sl_price = $6,
                            live_entry = $7, status = $8, tp_hits = $9, breakeven_active = $10, last_updated = NOW()
                        WHERE message_id = $1
                    ''', message_id, trade_data["entry"], trade_data["tp1"],
                        trade_data["tp2"], trade_data["tp3"], trade_data["sl"],
                        trade_data.get("live_entry"),
                        trade_data.get("status", "active"),
                        ','.join(trade_data.get("tp_hits", [])),
                        trade_data.get("breakeven_active", False))
            except Exception as e:
                # Send error details to debug channel instead of silently failing
                debug_channel = self.get_channel(DEBUG_CHANNEL_ID)
                if debug_channel:
                    await debug_channel.send(
                        f"❌ Database UPDATE failed for message_id {message_id}: {str(e)}"
                    )
                print(f"❌ Database update error: {str(e)}")

    async def remove_trade_from_db(self,
                                   message_id: str,
                                   completion_reason: str = "unknown"):
        """Remove completed trade from database and save to historical table"""
        # First, save to historical table before removing
        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    # Get the trade data before removing it
                    trade_row = await conn.fetchrow(
                        'SELECT * FROM active_trades WHERE message_id = $1',
                        message_id)

                    if trade_row:
                        # Save to completed_trades table for historical tracking
                        await conn.execute(
                            '''
                            INSERT INTO completed_trades (
                                message_id, channel_id, guild_id, pair, action,
                                entry_price, tp1_price, tp2_price, tp3_price, sl_price,
                                discord_entry, discord_tp1, discord_tp2, discord_tp3, discord_sl,
                                live_entry, assigned_api, final_status, tp_hits, breakeven_active, 
                                entry_type, manual_overrides, created_at, completion_reason
                            ) VALUES (
                                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, 
                                $16, $17, $18, $19, $20, $21, $22, $23, $24
                            )
                        ''', trade_row['message_id'], trade_row['channel_id'],
                            trade_row['guild_id'], trade_row['pair'],
                            trade_row['action'], trade_row['entry_price'],
                            trade_row['tp1_price'], trade_row['tp2_price'],
                            trade_row['tp3_price'], trade_row['sl_price'],
                            trade_row['discord_entry'],
                            trade_row['discord_tp1'], trade_row['discord_tp2'],
                            trade_row['discord_tp3'], trade_row['discord_sl'],
                            trade_row['live_entry'], trade_row['assigned_api'],
                            trade_row['status'], trade_row['tp_hits'],
                            trade_row['breakeven_active'],
                            trade_row['entry_type'],
                            trade_row.get('manual_overrides', ''),
                            trade_row['created_at'], completion_reason)

                    # Now remove from active_trades
                    await conn.execute(
                        'DELETE FROM active_trades WHERE message_id = $1',
                        message_id)

            except Exception as e:
                # Send error details to debug channel instead of silently failing
                debug_channel = self.get_channel(DEBUG_CHANNEL_ID)
                if debug_channel:
                    await debug_channel.send(
                        f"❌ Database DELETE/ARCHIVE failed for message_id {message_id}: {str(e)}"
                    )
                print(f"❌ Database delete/archive error: {str(e)}")

        # Always remove from memory after database operations
        if message_id in PRICE_TRACKING_CONFIG["active_trades"]:
            del PRICE_TRACKING_CONFIG["active_trades"][message_id]

    async def get_active_trades_from_db(self):
        """Get current active trades from database (used by commands)"""
        if not self.db_pool:
            return PRICE_TRACKING_CONFIG["active_trades"]

        try:
            # Always load from database to ensure we have the latest data
            # This ensures trades persist across bot restarts
            await self.load_active_trades_from_db()
            return PRICE_TRACKING_CONFIG["active_trades"]

        except Exception as e:
            print(f"Error loading trades from database: {e}")
            return PRICE_TRACKING_CONFIG["active_trades"]

    async def get_trade_from_db(self, message_id: str):
        """Get a single trade from database by message ID"""
        if not self.db_pool:
            return PRICE_TRACKING_CONFIG["active_trades"].get(message_id)

        try:
            async with self.db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    '''
                    SELECT * FROM active_trades WHERE message_id = $1
                ''', message_id)

                if row:
                    # Convert database row to trade data format (filter out empty strings from split)
                    tp_hits_list = [
                        tp for tp in row['tp_hits'].split(',') if tp
                    ] if row['tp_hits'] else []
                    return {
                        'pair':
                        row['pair'],
                        'action':
                        row['action'],
                        'entry':
                        float(row['entry_price']),
                        'tp1':
                        float(row['tp1_price']),
                        'tp2':
                        float(row['tp2_price']),
                        'tp3':
                        float(row['tp3_price']),
                        'sl':
                        float(row['sl_price']),
                        'discord_entry':
                        float(row['discord_entry'])
                        if row['discord_entry'] else None,
                        'discord_tp1':
                        float(row['discord_tp1'])
                        if row['discord_tp1'] else None,
                        'discord_tp2':
                        float(row['discord_tp2'])
                        if row['discord_tp2'] else None,
                        'discord_tp3':
                        float(row['discord_tp3'])
                        if row['discord_tp3'] else None,
                        'discord_sl':
                        float(row['discord_sl'])
                        if row['discord_sl'] else None,
                        'live_entry':
                        float(row['live_entry'])
                        if row['live_entry'] else None,
                        'assigned_api':
                        row.get('assigned_api', 'currencybeacon'),
                        'status':
                        row['status'],
                        'tp_hits':
                        tp_hits_list,
                        'breakeven_active':
                        row['breakeven_active'],
                        'channel_id':
                        row['channel_id'],
                        'guild_id':
                        row['guild_id']
                    }
                return None

        except Exception as e:
            print(f"Error getting trade from database: {e}")
            return PRICE_TRACKING_CONFIG["active_trades"].get(message_id)

    async def store_missed_hit(self, message_id: str, hit_type: str,
                               hit_level: str, hit_price: float):
        """Store a missed hit during night pause for chronological processing later"""
        if not self.db_pool:
            return

        try:
            amsterdam_now = datetime.now(AMSTERDAM_TZ)
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    '''
                    INSERT INTO missed_hits (message_id, hit_type, hit_level, hit_price, hit_time)
                    VALUES ($1, $2, $3, $4, $5)
                ''', message_id, hit_type, hit_level, hit_price, amsterdam_now)
        except Exception as e:
            print(f"Error storing missed hit: {e}")

    def is_night_pause(self):
        """Check if we're currently in night pause period (01:00-07:00 Amsterdam time on weekdays)"""
        amsterdam_now = datetime.now(AMSTERDAM_TZ)
        weekday = amsterdam_now.weekday()  # 0=Monday, 6=Sunday
        hour = amsterdam_now.hour

        # Night pause: 01:00-07:00 Amsterdam time on weekdays (Monday-Friday)
        return weekday <= 4 and 1 <= hour < 7

    def clean_pair_name(self, pair: str) -> str:
        """Centralized function to clean trading pair names for API consistency"""
        # Remove forward slashes, asterisks, spaces, and other special characters
        # Keep only alphanumeric characters
        cleaned = re.sub(r'[^A-Za-z0-9]', '', pair.strip())
        return cleaned.upper()

    async def get_working_api_for_pair(self, pair: str) -> str:
        """Determine which API successfully provides a price for a trading pair"""
        pair_clean = self.clean_pair_name(pair)

        # Try APIs in priority order and return the first one that works
        for api_name in PRICE_TRACKING_CONFIG["api_priority_order"]:
            try:
                # Check if API has key configured
                api_key = PRICE_TRACKING_CONFIG["api_keys"].get(
                    f"{api_name}_key")
                if not api_key:
                    continue

                price = await self.get_price_from_single_api(
                    api_name, pair_clean)
                if price is not None:
                    print(
                        f"✅ API assignment: {pair_clean} will use {api_name} (price: {price})"
                    )
                    return api_name
            except Exception as e:
                print(f"⚠️ {api_name} failed for {pair_clean}: {str(e)[:100]}")
                continue

        # If all APIs fail, default to currencybeacon
        print(
            f"⚠️ All APIs failed for {pair_clean}, defaulting to currencybeacon"
        )
        return "currencybeacon"

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
                        data["nickname"], data["total_joins"],
                        data["total_left"], data["current_members"])
        except Exception as e:
            print(f"❌ Error saving invite tracking to database: {str(e)}")

    # ===== LIVE PRICE TRACKING METHODS =====

    async def get_live_price(self,
                             pair: str,
                             use_all_apis: bool = False,
                             specific_api: str = None) -> Optional[float]:
        """Get live price with smart API rotation or from specific API for consistency"""
        if not PRICE_TRACKING_CONFIG["enabled"]:
            return None

        # Normalize pair format for different APIs
        pair_clean = self.clean_pair_name(pair)

        # If specific API requested (for signal consistency), use only that API
        if specific_api:
            return await self.get_price_from_single_api(
                specific_api, pair_clean)

        # For regular monitoring, use only 1-2 APIs to conserve limits
        # For initial signal verification, use all APIs for maximum accuracy
        if use_all_apis:
            return await self.get_verified_price_all_apis(pair_clean)
        else:
            return await self.get_price_optimized_rotation(pair_clean)

    async def get_price_optimized_rotation(self,
                                           pair_clean: str) -> Optional[float]:
        """Get price using priority order to maximize efficiency with free tier limits"""
        # Use strict priority order: currencybeacon -> exchangerate_api -> currencylayer -> abstractapi
        api_priority_order = PRICE_TRACKING_CONFIG["api_priority_order"]

        # Try each API in priority order until one succeeds
        for api_name in api_priority_order:
            try:
                price = await self.get_price_from_single_api(
                    api_name, pair_clean)
                if price is not None:
                    return price
            except Exception as e:
                print(f"⚠️ {api_name} failed for {pair_clean}: {str(e)[:100]}")
                continue

        # If all APIs fail, notify user
        await self.log_to_discord(
            f"🚨 **ALL APIS FAILED** for {pair_clean}\nAll 4 price APIs are currently unavailable. Please check API keys and limits."
        )
        return None

    def get_api_symbol(self, api_name: str, pair_clean: str) -> str:
        """Map user-friendly symbols to API-specific symbols - for original APIs"""
        # Original APIs don't need complex symbol mapping since they work with standard forex pairs
        # These APIs handle standard forex pairs directly (EURUSD, GBPUSD, etc.)
        # For special cases like XAUUSD, the APIs handle them properly in get_price_from_single_api

        # Return original symbol - the original APIs work fine with standard symbols
        return pair_clean

    async def get_price_from_single_api(self, api_name: str,
                                        pair_clean: str) -> Optional[float]:
        """Get price from a specific API - Only 4 selected APIs in priority order"""
        try:
            # Check if API key exists
            api_key = PRICE_TRACKING_CONFIG["api_keys"].get(f"{api_name}_key")
            if not api_key:
                return None

            # === 1. CURRENCYBEACON (Priority #1) ===
            if api_name == "currencybeacon":
                url = PRICE_TRACKING_CONFIG["api_endpoints"]["currencybeacon"]
                params = {"api_key": api_key}

                if pair_clean == "XAUUSD":
                    params["base"] = "USD"
                    params["symbols"] = "XAU"
                elif len(pair_clean) == 6:
                    params["base"] = pair_clean[:3]
                    params["symbols"] = pair_clean[3:]

                async with aiohttp.ClientSession() as session:
                    async with session.get(url,
                                           params=params,
                                           timeout=aiohttp.ClientTimeout(
                                               total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "response" in data and "rates" in data[
                                    "response"]:
                                rates = data["response"]["rates"]
                                if pair_clean == "XAUUSD" and "XAU" in rates:
                                    return 1.0 / float(rates["XAU"])
                                else:
                                    target_currency = pair_clean[3:]
                                    if target_currency in rates:
                                        return float(rates[target_currency])
                        elif response.status == 429:
                            await self.log_api_limit_warning(
                                "CurrencyBeacon",
                                "Monthly limit reached - switching to backup API"
                            )
                        elif response.status == 403:
                            await self.log_api_limit_warning(
                                "CurrencyBeacon", "API key invalid or expired")

            # === 2. EXCHANGERATE-API (Priority #2) ===
            elif api_name == "exchangerate_api":
                if pair_clean == "XAUUSD":
                    url = f"{PRICE_TRACKING_CONFIG['api_endpoints']['exchangerate_api']}/{api_key}/latest/USD"
                    async with aiohttp.ClientSession() as session:
                        async with session.get(url,
                                               timeout=aiohttp.ClientTimeout(
                                                   total=10)) as response:
                            if response.status == 200:
                                data = await response.json()
                                if "conversion_rates" in data and "XAU" in data[
                                        "conversion_rates"]:
                                    return 1.0 / float(
                                        data["conversion_rates"]["XAU"])
                            elif response.status == 429:
                                await self.log_api_limit_warning(
                                    "ExchangeRate-API",
                                    "Monthly limit reached - switching to backup API"
                                )
                else:
                    if len(pair_clean) == 6:
                        base_currency = pair_clean[:3]
                        target_currency = pair_clean[3:]
                        url = f"{PRICE_TRACKING_CONFIG['api_endpoints']['exchangerate_api']}/{api_key}/pair/{base_currency}/{target_currency}"
                        async with aiohttp.ClientSession() as session:
                            async with session.get(
                                    url, timeout=aiohttp.ClientTimeout(
                                        total=10)) as response:
                                if response.status == 200:
                                    data = await response.json()
                                    if "conversion_rate" in data:
                                        return float(data["conversion_rate"])
                                elif response.status == 429:
                                    await self.log_api_limit_warning(
                                        "ExchangeRate-API",
                                        "Monthly limit reached - switching to backup API"
                                    )

            # === 3. CURRENCYLAYER (Priority #3) ===
            elif api_name == "currencylayer":
                url = PRICE_TRACKING_CONFIG["api_endpoints"]["currencylayer"]
                params = {"access_key": api_key}

                if pair_clean == "XAUUSD":
                    params["currencies"] = "XAU"
                elif len(pair_clean) == 6:
                    params["currencies"] = pair_clean[3:]  # Target currency

                async with aiohttp.ClientSession() as session:
                    async with session.get(url,
                                           params=params,
                                           timeout=aiohttp.ClientTimeout(
                                               total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data.get("success") and "quotes" in data:
                                if pair_clean == "XAUUSD" and "USDXAU" in data[
                                        "quotes"]:
                                    return 1.0 / float(
                                        data["quotes"]["USDXAU"])
                                else:
                                    # Find matching quote
                                    for quote_key, quote_value in data[
                                            "quotes"].items():
                                        if quote_key.endswith(pair_clean[3:]):
                                            return float(quote_value)
                        elif response.status == 429:
                            await self.log_api_limit_warning(
                                "Currencylayer",
                                "Monthly limit reached - switching to backup API"
                            )

            # === 4. ABSTRACTAPI (Priority #4) ===
            elif api_name == "abstractapi":
                url = PRICE_TRACKING_CONFIG["api_endpoints"]["abstractapi"]
                params = {"api_key": api_key}

                if pair_clean == "XAUUSD":
                    params["base"] = "USD"
                    params["target"] = "XAU"
                elif len(pair_clean) == 6:
                    params["base"] = pair_clean[:3]
                    params["target"] = pair_clean[3:]

                async with aiohttp.ClientSession() as session:
                    async with session.get(url,
                                           params=params,
                                           timeout=aiohttp.ClientTimeout(
                                               total=10)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "exchange_rate" in data:
                                rate = float(data["exchange_rate"])
                                if pair_clean == "XAUUSD":
                                    return 1.0 / rate
                                return rate
                        elif response.status == 429:
                            await self.log_api_limit_warning(
                                "AbstractAPI",
                                "Monthly limit reached - all backup APIs exhausted"
                            )

        except Exception as e:
            print(f"⚠️ {api_name} API error for {pair_clean}: {str(e)[:100]}")

        return None

    async def get_verified_price_all_apis(self,
                                          pair_clean: str) -> Optional[float]:
        """Get price from all 4 selected APIs for cross-verification"""
        # Collect prices from the 4 selected APIs for cross-verification
        prices = {}
        api_errors = {}

        # Try all 4 APIs in priority order
        for api_name in PRICE_TRACKING_CONFIG["api_priority_order"]:
            try:
                price = await self.get_price_from_single_api(
                    api_name, pair_clean)
                if price is not None:
                    prices[api_name] = price
                else:
                    api_errors[api_name] = "no_data"
            except Exception as e:
                api_errors[api_name] = str(e)[:50]

        # Verify price accuracy using the 4 API sources
        return await self.verify_price_accuracy(pair_clean, prices, api_errors)

    async def verify_price_accuracy(
            self, pair: str, prices: Dict[str, float],
            api_errors: Dict[str, str]) -> Optional[float]:
        """Verify price accuracy by cross-checking multiple API sources"""
        if not prices:
            print(f"❌ No valid prices obtained for {pair} - all APIs failed")
            if api_errors:
                error_summary = ", ".join(
                    [f"{api}: {error}" for api, error in api_errors.items()])
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
                print(
                    f"⚠️ {api_name} price for {pair} deviates significantly: ${price} (avg: ${avg_price:.5f})"
                )

        if len(consistent_prices) >= 2:
            # Use average of consistent prices
            final_price = sum([price for _, price in consistent_prices
                               ]) / len(consistent_prices)
            api_names = ", ".join([api for api, _ in consistent_prices])
            print(
                f"✅ Price verified for {pair}: ${final_price:.5f} (sources: {api_names})"
            )
            return final_price
        elif len(prices) >= 2:
            # Use median if we have multiple sources but they're not very consistent
            sorted_prices = sorted(price_values)
            median_price = sorted_prices[len(sorted_prices) // 2]
            print(
                f"⚠️ Using median price for {pair}: ${median_price:.5f} (prices varied across sources)"
            )
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

    async def get_all_api_prices(self, pair_clean: str) -> Dict[str, any]:
        """Get prices from all 4 selected APIs for comparison - returns dict with prices and errors"""
        api_priority_order = PRICE_TRACKING_CONFIG["api_priority_order"]
        api_results = {}

        for api_name in api_priority_order:
            try:
                # Check if API key exists
                api_key_name = f"{api_name}_key"
                if api_key_name in PRICE_TRACKING_CONFIG[
                        "api_keys"] and PRICE_TRACKING_CONFIG["api_keys"][
                            api_key_name]:
                    price = await self.get_price_from_single_api(
                        api_name, pair_clean)
                    if price is not None:
                        api_results[api_name] = {
                            "price": price,
                            "status": "success"
                        }
                    else:
                        api_results[api_name] = {
                            "price": None,
                            "status": "no_data"
                        }
                else:
                    api_results[api_name] = {"price": None, "status": "no_key"}
            except Exception as e:
                api_results[api_name] = {
                    "price": None,
                    "status": f"error: {str(e)[:50]}"
                }

        return api_results

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
                "status": "active",  # Will be set based on entry type
                "tp_hits": [],
                "breakeven_active": False,
                "entry_type":
                None  # Store full entry type (Buy/Sell + limit/execution)
            }

            # Find pair from "Trade Signal For: PAIR"
            for line in lines:
                if "Trade Signal For:" in line:
                    parts = line.split("Trade Signal For:")
                    if len(parts) > 1:
                        # Extract and clean the pair name using centralized function
                        raw_pair = parts[1].strip()
                        cleaned_pair = self.clean_pair_name(raw_pair)
                        trade_data["pair"] = cleaned_pair
                        break

            # Extract full entry type from "Entry Type: Buy execution/limit" or "Entry Type: Sell execution/limit"
            entry_type_match = re.search(
                r'Entry Type:\s*(Buy|Sell)\s+(execution|limit)', content,
                re.IGNORECASE)
            if entry_type_match:
                action = entry_type_match.group(1).upper()  # BUY or SELL
                order_type = entry_type_match.group(
                    2).lower()  # execution or limit

                trade_data["action"] = action
                trade_data[
                    "entry_type"] = f"{action.lower()} {order_type}"  # "buy execution", "sell limit", etc.

                # Set status based on order type
                if order_type == "limit":
                    trade_data[
                        "status"] = "pending_entry"  # Wait for entry to be hit
                else:  # execution
                    trade_data[
                        "status"] = "active"  # Start tracking immediately
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
            # Enhanced pattern for BTCUSD and other high-value pairs
            entry_match = re.search(r'Entry Price:\s*\$?([0-9]+(?:\.[0-9]+)?)',
                                    content, re.IGNORECASE)
            if entry_match:
                trade_data["entry"] = float(entry_match.group(1))
            else:
                # Fallback to old format "Entry: price" with improved regex
                entry_match = re.search(r'Entry[:\s]*\$?([0-9]+(?:\.[0-9]+)?)',
                                        content, re.IGNORECASE)
                if entry_match:
                    trade_data["entry"] = float(entry_match.group(1))

            # Extract Take Profit levels with enhanced patterns for all pairs including BTCUSD
            tp1_match = re.search(r'Take Profit 1:\s*\$?([0-9]+(?:\.[0-9]+)?)',
                                  content, re.IGNORECASE)
            if tp1_match:
                trade_data["tp1"] = float(tp1_match.group(1))
            else:
                # Fallback to old format "TP1: price" with improved regex
                tp1_match = re.search(r'TP1[:\s]*\$?([0-9]+(?:\.[0-9]+)?)',
                                      content, re.IGNORECASE)
                if tp1_match:
                    trade_data["tp1"] = float(tp1_match.group(1))

            tp2_match = re.search(r'Take Profit 2:\s*\$?([0-9]+(?:\.[0-9]+)?)',
                                  content, re.IGNORECASE)
            if tp2_match:
                trade_data["tp2"] = float(tp2_match.group(1))
            else:
                # Fallback to old format "TP2: price" with improved regex
                tp2_match = re.search(r'TP2[:\s]*\$?([0-9]+(?:\.[0-9]+)?)',
                                      content, re.IGNORECASE)
                if tp2_match:
                    trade_data["tp2"] = float(tp2_match.group(1))

            tp3_match = re.search(r'Take Profit 3:\s*\$?([0-9]+(?:\.[0-9]+)?)',
                                  content, re.IGNORECASE)
            if tp3_match:
                trade_data["tp3"] = float(tp3_match.group(1))
            else:
                # Fallback to old format "TP3: price" with improved regex
                tp3_match = re.search(r'TP3[:\s]*\$?([0-9]+(?:\.[0-9]+)?)',
                                      content, re.IGNORECASE)
                if tp3_match:
                    trade_data["tp3"] = float(tp3_match.group(1))

            # Extract Stop Loss from "Stop Loss: $3478.50" with enhanced pattern
            sl_match = re.search(r'Stop Loss:\s*\$?([0-9]+(?:\.[0-9]+)?)',
                                 content, re.IGNORECASE)
            if sl_match:
                trade_data["sl"] = float(sl_match.group(1))
            else:
                # Fallback to old format "SL: price" with improved regex
                sl_match = re.search(r'SL[:\s]*\$?([0-9]+(?:\.[0-9]+)?)',
                                     content, re.IGNORECASE)
                if sl_match:
                    trade_data["sl"] = float(sl_match.group(1))

            # Debug logging to help troubleshoot parsing issues
            print(f"🔍 Parsing signal content: {content[:100]}...")
            print(
                f"   Extracted - Pair: {trade_data['pair']}, Action: {trade_data['action']}"
            )
            print(
                f"   Extracted - Entry: {trade_data['entry']}, TP1: {trade_data['tp1']}, TP2: {trade_data['tp2']}, TP3: {trade_data['tp3']}, SL: {trade_data['sl']}"
            )

            # Special handling for BTCUSD to improve parsing reliability
            if trade_data['pair'] == 'BTCUSD':
                print(f"🔍 BTCUSD Special Debug - Raw content: {content}")
                print(
                    f"🔍 BTCUSD Special Debug - Split lines: {content.split(chr(10))}"
                )

            # Validate required fields
            if all([
                    trade_data["pair"], trade_data["action"],
                    trade_data["entry"], trade_data["tp1"], trade_data["tp2"],
                    trade_data["tp3"], trade_data["sl"]
            ]):
                print(
                    f"✅ Successfully parsed signal for {trade_data['pair']} ({trade_data['action']})"
                )
                return trade_data
            else:
                missing_fields = []
                for field, value in trade_data.items():
                    if field in [
                            "pair", "action", "entry", "tp1", "tp2", "tp3",
                            "sl"
                    ] and value is None:
                        missing_fields.append(field)
                print(
                    f"❌ Signal parsing failed - missing fields: {missing_fields}"
                )

        except Exception as e:
            # Log signal parsing failures (can't use await in non-async function)
            print(f"❌ Signal parsing error: {str(e)}")

        return None

    async def check_message_still_exists(self, message_id: str,
                                         trade_data: Dict) -> bool:
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

    async def check_price_levels(self, message_id: str,
                                 trade_data: Dict) -> bool:
        """Check if current price has hit any TP/SL levels, including entry hits for limit orders"""
        try:
            # First check if the original message still exists
            if not await self.check_message_still_exists(
                    message_id, trade_data):
                # Message was deleted, remove from tracking
                await self.remove_trade_from_db(message_id, "message_deleted")
                return True  # Return True to indicate this trade should be removed from active tracking

            # Verify trade data consistency between memory and database to prevent missed hits
            trade_data = await self.verify_trade_data_consistency(
                message_id, trade_data)

            # Check if this is a pending limit order waiting for entry
            if trade_data.get("status") == "pending_entry":
                return await self.check_limit_entry_hit(message_id, trade_data)
            # Use the assigned API for this specific signal to ensure consistency
            assigned_api = trade_data.get("assigned_api", "currencybeacon")
            current_price = await self.get_live_price(
                trade_data["pair"], specific_api=assigned_api)

            # If assigned API fails, try fallback with comprehensive retry
            if current_price is None:
                await self.debug_to_channel(
                    "PRICE CHECK",
                    f"⚠️ Assigned API {assigned_api} failed for {trade_data['pair']}, trying fallback...",
                    "⚠️")
                current_price = await self.get_live_price(
                    trade_data["pair"],
                    use_all_apis=True)  # Use all APIs for maximum reliability
                if current_price is None:
                    await self.debug_to_channel(
                        "PRICE CHECK",
                        f"❌ ALL APIs failed for {trade_data['pair']} - CRITICAL: TP/SL check skipped!",
                        "❌")
                    # Log this as a critical issue since we're missing price checks
                    await self.log_to_discord(
                        f"🚨 CRITICAL: All APIs failed for {trade_data['pair']} - TP/SL monitoring temporarily unavailable"
                    )
                    return False

            action = trade_data["action"]
            entry = trade_data["entry"]

            # Apply trading logic validation before processing any hits
            tp_hits_set = set(trade_data.get("tp_hits", []))

            # Rule 1: If TP2 was already hit, SL cannot hit (breakeven protection)
            if "tp2" in tp_hits_set:
                trade_data["breakeven_active"] = True

            # Determine if we should check breakeven (after TP2 hit) but respect manual overrides
            manual_overrides_set = set(trade_data.get("manual_overrides", []))
            if trade_data.get(
                    "breakeven_active",
                    False) and "breakeven" not in manual_overrides_set:
                # Check if price returned to entry (breakeven SL)
                if action == "BUY" and current_price <= entry:
                    await self.handle_breakeven_hit(message_id, trade_data)
                    return True
                elif action == "SELL" and current_price >= entry:
                    await self.handle_breakeven_hit(message_id, trade_data)
                    return True
            else:
                # Check SL first - but apply trading logic validation and respect manual overrides
                manual_overrides_set = set(
                    trade_data.get("manual_overrides", []))
                sl_hit = False

                # Only check SL if it wasn't manually overridden
                if "sl" not in manual_overrides_set:
                    if action == "BUY" and current_price <= trade_data["sl"]:
                        sl_hit = True
                    elif action == "SELL" and current_price >= trade_data["sl"]:
                        sl_hit = True

                if sl_hit:
                    # Rule 2: SL cannot hit after TP2 (breakeven protection)
                    if "tp2" in tp_hits_set:
                        return False  # Ignore this SL hit due to breakeven

                    # Rule 3: SL cannot hit after TP3
                    if "tp3" in tp_hits_set:
                        return False  # Ignore this SL hit

                    # SL is valid - process it
                    await self.handle_sl_hit(message_id, trade_data)
                    return True

                # Check TP levels with comprehensive detection - detect ALL TPs that have been hit
                # ⚠️ CRITICAL: Respect manual overrides to prevent duplicate notifications
                manual_overrides_set = set(
                    trade_data.get("manual_overrides", []))
                tp_levels_hit = []

                if action == "BUY":
                    # For BUY orders, check if price has reached each TP level (but NOT if manually overridden)
                    if "tp1" not in tp_hits_set and "tp1" not in manual_overrides_set and current_price >= trade_data[
                            "tp1"]:
                        tp_levels_hit.append("tp1")
                    if "tp2" not in tp_hits_set and "tp2" not in manual_overrides_set and current_price >= trade_data[
                            "tp2"]:
                        tp_levels_hit.append("tp2")
                    if "tp3" not in tp_hits_set and "tp3" not in manual_overrides_set and current_price >= trade_data[
                            "tp3"]:
                        tp_levels_hit.append("tp3")
                elif action == "SELL":
                    # For SELL orders, check if price has reached each TP level (but NOT if manually overridden)
                    if "tp1" not in tp_hits_set and "tp1" not in manual_overrides_set and current_price <= trade_data[
                            "tp1"]:
                        tp_levels_hit.append("tp1")
                    if "tp2" not in tp_hits_set and "tp2" not in manual_overrides_set and current_price <= trade_data[
                            "tp2"]:
                        tp_levels_hit.append("tp2")
                    if "tp3" not in tp_hits_set and "tp3" not in manual_overrides_set and current_price <= trade_data[
                            "tp3"]:
                        tp_levels_hit.append("tp3")

                # Process all TP hits found (in order: TP1, TP2, TP3)
                if tp_levels_hit:
                    # Rule 4: TP cannot hit after SL (this shouldn't happen in real-time, but safety check)
                    # In real-time, if SL was hit, the trade would be closed, so this is just extra safety

                    # Process all TP hits in the correct order
                    for tp_level in ["tp1", "tp2", "tp3"]:
                        if tp_level in tp_levels_hit:
                            await self.handle_tp_hit(message_id, trade_data,
                                                     tp_level)

                            # If TP3 was hit, trade is completed and will be removed
                            if tp_level == "tp3":
                                return True

                    return True

            return False

        except Exception as e:
            # Enhanced error logging for debugging missed hits
            await self.debug_to_channel(
                "PRICE CHECK ERROR",
                f"❌ CRITICAL ERROR checking {trade_data.get('pair', 'unknown')} levels: {str(e)}",
                "❌")
            await self.log_to_discord(
                f"🚨 CRITICAL: Error checking TP/SL levels for {trade_data.get('pair', 'unknown')}: {str(e)}"
            )
            import traceback
            full_error = traceback.format_exc()
            print(
                f"❌ CRITICAL: Error checking price levels for {message_id}: {e}"
            )
            print(f"Full traceback: {full_error}")
            # Return False but ensure this error is highly visible
            return False

    async def handle_tp_hit(self,
                            message_id: str,
                            trade_data: Dict,
                            tp_level: str,
                            offline_hit: bool = False):
        """Handle when a TP level is hit"""
        try:
            # Enhanced TP hit logging for debugging
            await self.debug_to_channel(
                "TP HIT PROCESSING",
                f"🎯 Processing {tp_level.upper()} hit for {trade_data['pair']} {trade_data['action']}",
                "🎯")

            # Update trade data with duplicate protection
            current_tp_hits = trade_data.get("tp_hits", [])
            if tp_level not in current_tp_hits:
                trade_data["tp_hits"].append(tp_level)
                await self.debug_to_channel(
                    "TP HIT PROCESSING",
                    f"✅ Added {tp_level.upper()} to hits list. Current hits: {trade_data['tp_hits']}",
                    "✅")
            else:
                await self.debug_to_channel(
                    "TP HIT PROCESSING",
                    f"⚠️ {tp_level.upper()} already in hits list - duplicate protection activated",
                    "⚠️")
                return  # Prevent duplicate processing

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
                await self.remove_trade_from_db(message_id, "tp3_hit")

            # Send notification
            await self.send_tp_notification(message_id, trade_data, tp_level,
                                            offline_hit)

        except Exception as e:
            # Enhanced error logging for TP hit failures
            await self.debug_to_channel(
                "TP HIT ERROR",
                f"❌ CRITICAL ERROR processing {tp_level.upper()} hit for {trade_data.get('pair', 'unknown')}: {str(e)}",
                "❌")
            await self.log_to_discord(
                f"🚨 CRITICAL: Failed to process {tp_level.upper()} hit for {trade_data.get('pair', 'unknown')}: {str(e)}"
            )
            import traceback
            full_error = traceback.format_exc()
            print(f"❌ CRITICAL: Error handling TP hit: {e}")
            print(f"Full traceback: {full_error}")

    async def handle_sl_hit(self,
                            message_id: str,
                            trade_data: Dict,
                            offline_hit: bool = False):
        """Handle when SL is hit"""
        try:
            trade_data["status"] = "closed (sl hit)"

            # Remove from active trades (database and memory)
            await self.remove_trade_from_db(message_id, "sl_hit")

            # Send notification
            await self.send_sl_notification(message_id, trade_data,
                                            offline_hit)

        except Exception as e:
            print(f"Error handling SL hit: {e}")

    async def handle_breakeven_hit(self,
                                   message_id: str,
                                   trade_data: Dict,
                                   offline_hit: bool = False):
        """Handle when price returns to breakeven after TP2"""
        try:
            trade_data["status"] = "closed (breakeven after tp2)"

            # Remove from active trades (database and memory)
            await self.remove_trade_from_db(message_id, "breakeven_hit")

            # Send breakeven notification
            await self.send_breakeven_notification(message_id, trade_data,
                                                   offline_hit)

        except Exception as e:
            print(f"Error handling breakeven hit: {e}")

    async def check_single_trade_immediately(self, message_id: str,
                                             trade_data: Dict):
        """Check a single trade immediately for any TP/SL hits - used for immediate tracking after signal creation"""
        try:
            # Get current price to check if any levels were hit using API priority order
            current_price = await self.get_live_price(
                trade_data["pair"],
                specific_api=trade_data.get("assigned_api"))
            if current_price is None:
                # Try fallback if assigned API fails
                current_price = await self.get_live_price(trade_data["pair"],
                                                          use_all_apis=False)

            if current_price is None:
                await self.debug_to_channel(
                    "IMMEDIATE CHECK",
                    f"❌ Could not get price for immediate check of {trade_data['pair']}",
                    "⚠️")
                return

            await self.debug_to_channel(
                "IMMEDIATE CHECK",
                f"Checking {trade_data['pair']} immediately - Current price: {current_price}",
                "🔍")

            action = trade_data["action"]
            entry = trade_data["entry"]
            tp_hits = trade_data.get('tp_hits', [])

            # Check for SL hit immediately
            if action == "BUY" and current_price <= trade_data["sl"]:
                await self.debug_to_channel(
                    "IMMEDIATE CHECK",
                    f"⚠️ {trade_data['pair']} is already at/below SL level immediately after signal creation!",
                    "⚠️")
                await self.handle_sl_hit(message_id,
                                         trade_data,
                                         offline_hit=False)
                return
            elif action == "SELL" and current_price >= trade_data["sl"]:
                await self.debug_to_channel(
                    "IMMEDIATE CHECK",
                    f"⚠️ {trade_data['pair']} is already at/above SL level immediately after signal creation!",
                    "⚠️")
                await self.handle_sl_hit(message_id,
                                         trade_data,
                                         offline_hit=False)
                return

            # Check for TP hits immediately - detect ALL TPs that have been surpassed
            tp_levels_hit = []

            if action == "BUY":
                if "tp1" not in tp_hits and current_price >= trade_data["tp1"]:
                    tp_levels_hit.append("tp1")
                if "tp2" not in tp_hits and current_price >= trade_data["tp2"]:
                    tp_levels_hit.append("tp2")
                if "tp3" not in tp_hits and current_price >= trade_data["tp3"]:
                    tp_levels_hit.append("tp3")
            elif action == "SELL":
                if "tp1" not in tp_hits and current_price <= trade_data["tp1"]:
                    tp_levels_hit.append("tp1")
                if "tp2" not in tp_hits and current_price <= trade_data["tp2"]:
                    tp_levels_hit.append("tp2")
                if "tp3" not in tp_hits and current_price <= trade_data["tp3"]:
                    tp_levels_hit.append("tp3")

            # Handle any TP hits found
            for tp_level in tp_levels_hit:
                await self.debug_to_channel(
                    "IMMEDIATE CHECK",
                    f"⚠️ {trade_data['pair']} has already reached {tp_level.upper()} immediately after signal creation!",
                    "⚠️")
                await self.handle_tp_hit(message_id,
                                         trade_data,
                                         tp_level,
                                         offline_hit=False)

            if not tp_levels_hit:
                await self.debug_to_channel(
                    "IMMEDIATE CHECK",
                    f"✅ {trade_data['pair']} is in proper position - no immediate hits detected",
                    "✅")

        except Exception as e:
            await self.debug_to_channel(
                "IMMEDIATE CHECK", f"❌ Error during immediate check: {str(e)}",
                "❌")
            print(f"Immediate check error: {e}")

    async def verify_trade_data_consistency(self, message_id: str,
                                            memory_trade_data: Dict) -> Dict:
        """Verify trade data consistency between memory and database to prevent missed hits due to sync issues"""
        try:
            if not self.db_pool:
                return memory_trade_data  # No database available, use memory data

            # Get trade from database
            async with self.db_pool.acquire() as conn:
                db_trade = await conn.fetchrow(
                    'SELECT * FROM active_trades WHERE message_id = $1',
                    message_id)

                if not db_trade:
                    await self.debug_to_channel(
                        "DATA SYNC",
                        f"⚠️ Trade {message_id[:8]}... missing from database but exists in memory",
                        "⚠️")
                    return memory_trade_data

                # Convert database row to dict and compare TP hits
                db_tp_hits = db_trade['tp_hits'].split(
                    ',') if db_trade['tp_hits'] else []
                memory_tp_hits = memory_trade_data.get('tp_hits', [])

                # Remove empty strings from db_tp_hits
                db_tp_hits = [hit for hit in db_tp_hits if hit.strip()]

                if set(db_tp_hits) != set(memory_tp_hits):
                    await self.debug_to_channel(
                        "DATA SYNC",
                        f"⚠️ TP hits mismatch for {memory_trade_data.get('pair', 'unknown')}! DB: {db_tp_hits} vs Memory: {memory_tp_hits}",
                        "⚠️")
                    # Use database data as source of truth and update memory
                    memory_trade_data['tp_hits'] = db_tp_hits
                    memory_trade_data['breakeven_active'] = db_trade[
                        'breakeven_active']
                    memory_trade_data['status'] = db_trade['status']
                    await self.debug_to_channel(
                        "DATA SYNC",
                        f"✅ Synchronized memory data with database for {memory_trade_data.get('pair', 'unknown')}",
                        "✅")

                return memory_trade_data

        except Exception as e:
            await self.debug_to_channel(
                "DATA SYNC ERROR",
                f"❌ Error verifying trade data consistency: {str(e)}", "❌")
            print(f"Data consistency check error: {e}")
            return memory_trade_data  # Return original data on error

    async def check_message_deleted(self, message_id: str,
                                    channel_id: int) -> bool:
        """Check if the original trade signal message has been deleted"""
        try:
            channel = self.get_channel(channel_id)
            if not channel:
                return True  # Channel not found, treat as deleted

            message = await channel.fetch_message(int(message_id))
            return False  # Message still exists

        except discord.NotFound:
            return True  # Message was deleted
        except discord.Forbidden:
            return False  # No permission, but message exists
        except Exception as e:
            print(f"Error checking message deletion: {e}")
            return False  # Assume exists on error to avoid false removal

    async def check_limit_entry_hit(self, message_id: str,
                                    trade_data: Dict) -> bool:
        """Check if limit entry has been hit and convert to active tracking"""
        try:
            # Use the assigned API for this specific signal
            assigned_api = trade_data.get("assigned_api", "currencybeacon")
            current_price = await self.get_live_price(
                trade_data["pair"], specific_api=assigned_api)

            # If assigned API fails, try fallback
            if current_price is None:
                current_price = await self.get_live_price(trade_data["pair"],
                                                          use_all_apis=False)
                if current_price is None:
                    return False  # No price available, keep checking

            action = trade_data["action"]
            entry_price = trade_data["entry"]
            entry_type = trade_data.get("entry_type", "").lower()

            # Check if entry has been hit based on order type
            entry_hit = False
            if "buy limit" in entry_type:
                # Buy limit: price must drop to or below entry price
                entry_hit = current_price <= entry_price
            elif "sell limit" in entry_type:
                # Sell limit: price must rise to or above entry price
                entry_hit = current_price >= entry_price

            if entry_hit:
                # Entry has been hit! Send notification and switch to active tracking
                await self.handle_limit_entry_hit(message_id, trade_data,
                                                  current_price)
                return False  # Don't remove from tracking, continue with active monitoring

            return False  # Entry not hit yet, keep monitoring

        except Exception as e:
            print(f"Error checking limit entry hit: {e}")
            return False

    async def handle_limit_entry_hit(self, message_id: str, trade_data: Dict,
                                     current_price: float):
        """Handle when a limit entry gets hit - send notification and switch to active tracking"""
        try:
            # Send entry hit notification
            await self.send_entry_hit_notification(message_id, trade_data)

            # Recalculate TP/SL levels based on current live price (not original entry price)
            live_levels = self.calculate_live_tracking_levels(
                current_price, trade_data["pair"], trade_data["action"])

            # Update trade data with new live-based levels
            trade_data["live_entry"] = current_price
            trade_data["entry"] = live_levels[
                "entry"]  # Update entry to current price
            trade_data["tp1"] = live_levels["tp1"]
            trade_data["tp2"] = live_levels["tp2"]
            trade_data["tp3"] = live_levels["tp3"]
            trade_data["sl"] = live_levels["sl"]
            trade_data[
                "status"] = "active"  # Switch from pending_entry to active

            # Update in database with new price levels
            await self.update_limit_trade_in_db(message_id, trade_data)

            print(
                f"✅ Limit entry hit for {trade_data['pair']} - switched to active tracking"
            )

        except Exception as e:
            print(f"Error handling limit entry hit: {e}")

    async def send_entry_hit_notification(self, message_id: str,
                                          trade_data: Dict):
        """Send notification when limit entry gets hit"""
        try:
            channel = self.get_channel(trade_data.get("channel_id"))
            if not channel:
                return

            message = await channel.fetch_message(int(message_id))
            if not message:
                return

            action = trade_data["action"].lower()
            notification = f"@everyone our {action} limit has been hit ✅"

            await message.reply(notification)

        except Exception as e:
            print(f"Error sending entry hit notification: {e}")

    async def process_night_pause_hits(self):
        """Process all missed hits that occurred during night pause in chronological order with trading logic validation"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                # Get all unprocessed missed hits, sorted chronologically
                missed_hits = await conn.fetch('''
                    SELECT mh.*, at.action, at.pair, at.entry_price, at.tp1_price, at.tp2_price, at.tp3_price, at.sl_price,
                           at.tp_hits, at.breakeven_active, at.status
                    FROM missed_hits mh
                    JOIN active_trades at ON mh.message_id = at.message_id
                    WHERE mh.processed = FALSE
                    ORDER BY mh.hit_time ASC
                ''')

                if not missed_hits:
                    return

                # Group hits by message_id for chronological processing per trade
                trade_hits = {}
                for hit in missed_hits:
                    msg_id = hit['message_id']
                    if msg_id not in trade_hits:
                        trade_hits[msg_id] = []
                    trade_hits[msg_id].append(hit)

                processed_count = 0

                # Process each trade's hits chronologically with trading logic validation
                for message_id, hits in trade_hits.items():
                    try:
                        # Get current trade data
                        trade_data = await self.get_trade_from_db(message_id)
                        if not trade_data:
                            continue

                        # Apply chronological processing with trading logic
                        valid_hits = self.validate_chronological_hits(hits)

                        # Send notifications for valid hits in chronological order
                        for hit in valid_hits:
                            hit_type = hit['hit_type']
                            hit_level = hit['hit_level']

                            if hit_type == 'tp':
                                await self.handle_tp_hit(message_id,
                                                         trade_data,
                                                         hit_level,
                                                         offline_hit=True)
                            elif hit_type == 'sl':
                                await self.handle_sl_hit(message_id,
                                                         trade_data,
                                                         offline_hit=True)
                            elif hit_type == 'breakeven':
                                await self.handle_breakeven_hit(
                                    message_id, trade_data, offline_hit=True)

                            # Mark this hit as processed
                            await conn.execute(
                                '''
                                UPDATE missed_hits SET processed = TRUE 
                                WHERE id = $1
                            ''', hit['id'])
                            processed_count += 1

                            # Small delay between messages to avoid spam
                            await asyncio.sleep(1)

                    except Exception as e:
                        print(
                            f"Error processing hits for trade {message_id}: {e}"
                        )
                        continue

                if processed_count > 0:
                    await self.log_to_discord(
                        f"🌅 **Night pause ended** - Processed {processed_count} missed TP/SL hits that occurred during 01:00-07:00"
                    )

        except Exception as e:
            print(f"Error processing night pause hits: {e}")

    def validate_chronological_hits(self, hits: List) -> List:
        """Validate hits chronologically according to trading rules"""
        valid_hits = []
        sl_hit = False
        tp_levels_hit = set()

        for hit in sorted(hits, key=lambda x: x['hit_time']):
            hit_type = hit['hit_type']
            hit_level = hit['hit_level']

            # Rule 1: If SL was hit first, ignore all subsequent TP hits
            if sl_hit and hit_type == 'tp':
                continue

            # Rule 2: If SL hits after TP2, ignore it (breakeven protection)
            if hit_type == 'sl' and 'tp2' in tp_levels_hit:
                continue

            # Rule 3: Cannot hit both TP3 and SL
            if hit_type == 'sl' and 'tp3' in tp_levels_hit:
                continue
            if hit_type == 'tp' and hit_level == 'tp3' and sl_hit:
                continue

            # Hit is valid according to trading rules
            valid_hits.append(hit)

            # Update state tracking
            if hit_type == 'sl':
                sl_hit = True
            elif hit_type == 'tp':
                tp_levels_hit.add(hit_level)

        return valid_hits

    async def send_tp_notification(self,
                                   message_id: str,
                                   trade_data: Dict,
                                   tp_level: str,
                                   offline_hit: bool = False,
                                   manual_override: bool = False):
        """Send TP hit notification with random message selection"""
        import random

        try:
            # Random messages for each TP level
            tp1_messages = [
                "@everyone TP1 has been hit. First target secured, let's keep it going. Next stop: TP2 📈🔥",
                "@everyone TP1 smashed. Secure some profits if you'd like and let's aim for TP2 🎯💪",
                "@everyone We've just hit TP1. Nice start. The current momentum is looking good for TP2 🚀📊",
                "@everyone TP1 has been hit! Keep your eyes on the next level. TP2 up next 👀💸",
                "@everyone First milestone hit. The trade is off to a clean start 📉➡️📈",
                "@everyone TP1 has been reached. Let's keep the discipline and push for TP2 💼🔁",
                "@everyone First TP level hit! TP1 is in. Stay focused as we aim for TP2 & TP3! 💹🚀",
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
                "@everyone TP2 has officially been smashed. Move SL to breakeven, partial close if you haven't already. TP3 is calling 📈🔥",
                "@everyone TP2 just got hit. Lock in those gains by moving your SL to breakeven. TP3 is the next target so let's stay sharp and ride this momentum 💪📊",
                "@everyone Another level cleared as TP2 has been hit. Shift SL to breakeven and lock it in. Eyes on TP3 now so let's finish strong 🧠🎯",
                "@everyone TP2 has been hit. Move your SL to breakeven immediately. This setup is moving clean and TP3 is well within reach 🚀🔒",
                "@everyone Great move traders, TP2 has been tagged. Time to shift SL to breakeven and secure the bag. TP3 is the final boss and we're coming for it 💼⚔️"
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
                "@everyone Final target hit. Huge win for FX Pip Pioneers 🔥💸",
                "@everyone TP3 secured. That's the result of following the plan 💼💎"
            ]

            message = await self.get_channel(trade_data.get("channel_id")
                                             ).fetch_message(int(message_id))

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

            await message.reply(notification)

        except Exception as e:
            print(f"Error sending TP notification: {e}")

    async def send_sl_notification(self,
                                   message_id: str,
                                   trade_data: Dict,
                                   offline_hit: bool = False,
                                   manual_override: bool = False):
        """Send SL hit notification with random message selection"""
        import random

        try:
            # Random messages for SL hits (10 messages)
            sl_messages = [
                "@everyone This one hit SL. It happens. Let's stay focused and get the next one 🔄🧠",
                "@everyone SL has been hit. Risk was managed, we move on 💪📉",
                "@everyone This setup didn't go as planned and hit SL. On to the next 📊",
                "@everyone SL hit. It's all part of the process. Stay disciplined 💼📚",
                "@everyone SL hit. Losses are part of trading. We bounce back 📈⏭️",
                "@everyone SL hit. Trust the process and prepare for the next opportunity 🔄🧠",
                "@everyone SL was hit on this one. We took the loss, now let's stay sharp 🔁💪",
                "@everyone SL hit. It's part of the game. Let's stay focused on quality 📉🎯",
                "@everyone This trade hit SL. Discipline keeps us in the game. We´ll get the loss back next trade💼🧘‍♂️",
                "@everyone SL triggered. Part of proper risk management. Next setup coming soon 💪⚡"
            ]

            message = await self.get_channel(trade_data.get("channel_id")
                                             ).fetch_message(int(message_id))
            notification = random.choice(sl_messages)

            await message.reply(notification)

        except Exception as e:
            print(f"Error sending SL notification: {e}")

    async def send_breakeven_notification(self,
                                          message_id: str,
                                          trade_data: Dict,
                                          offline_hit: bool = False):
        """Send breakeven hit notification with random message selection"""
        import random

        try:
            # Random messages for breakeven hits (10 messages)
            breakeven_messages = [
                "@everyone TP2 has been hit & price has reversed to breakeven, so as usual, we're out safe 🫡",
                "@everyone Price returned to breakeven after hitting TP2. Smart exit, we secured profits and protected capital 💼✅",
                "@everyone Breakeven reached after TP2 hit. Clean trade management - we're out with gains secured 🎯🔒",
                "@everyone TP2 was hit, now back to breakeven. Perfect trade execution, we exit safe and profitable 📊🛡️",
                "@everyone Price reversed to entry after TP2. Textbook risk management - we're out with profits locked in 💰🧠",
                "@everyone Breakeven hit after TP2. Smart trading discipline pays off. We're out safe and ahead 🚀⚖️",
                "@everyone Back to breakeven post-TP2. This is how we protect profits. Clean exit, clean conscience 💎🔐",
                "@everyone TP2 secured, now at breakeven. Professional trade management - we exit with gains protected 📈🛡️",
                "@everyone Price action brought us back to entry after TP2. Strategic exit with profits in the bag 🎯💼",
                "@everyone Breakeven reached after TP2 hit. This is disciplined trading - we're out safe with profits secured 🧘‍♂️💸"
            ]

            message = await self.get_channel(trade_data.get("channel_id")
                                             ).fetch_message(int(message_id))
            notification = random.choice(breakeven_messages)

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
                    ''', member.id, member.guild.id, invite_code)

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
                    ''', member.id, member.guild.id)

                if join_record and join_record['invite_code']:
                    invite_code = join_record['invite_code']

                    # Update the member's record
                    await conn.execute(
                        '''
                        UPDATE member_joins 
                        SET left_at = NOW(), is_currently_member = FALSE
                        WHERE member_id = $1 AND guild_id = $2 AND is_currently_member = TRUE
                        ''', member.id, member.guild.id)

                    # Update invite tracking statistics
                    if invite_code in INVITE_TRACKING:
                        INVITE_TRACKING[invite_code]["total_left"] += 1
                        INVITE_TRACKING[invite_code]["current_members"] = max(
                            0,
                            INVITE_TRACKING[invite_code]["current_members"] -
                            1)
                        await self.save_invite_tracking()

        except Exception as e:
            print(f"❌ Error tracking member leave: {str(e)}")

    def _is_owner_safe(self, inviter_id):
        """Safe owner check that never raises exceptions"""
        try:
            if not inviter_id:
                return False
            # Convert to int if needed and check
            inviter_id_int = int(inviter_id) if not isinstance(
                inviter_id, int) else inviter_id
            return is_bot_owner(inviter_id_int)
        except:
            return False

    async def check_timedautorole_eligibility(self,
                                              member: discord.Member,
                                              inviter_id: int = None,
                                              invite_code: str = None):
        """
        🛡️ CHECK TIMEDAUTOROLE ELIGIBILITY - Enhanced anti-abuse system for fake accounts
        
        NEW RULES (Enhanced Security):
        1. If inviter is owner - ALWAYS ALLOW (even suspicious accounts)  
        2. If account created within 1 hour of joining - BLOCK unless invited by owner
        3. All future invites from banned inviters are blocked
        
        Returns: {
            "allowed": bool,
            "reason": str,
            "suspicious": bool,
            "inviter_banned": bool
        }
        """
        # 🚨 BULLETPROOF: Single decision point with safe computations
        is_owner_invite = False
        is_suspicious = True  # Default to suspicious for maximum security
        error_reason = None

        try:
            # Safe owner check first
            is_owner_invite = self._is_owner_safe(inviter_id)

            # Use timezone-aware UTC consistently
            join_time = member.joined_at or discord.utils.utcnow()
            account_created_at = member.created_at

            if not account_created_at:
                # STRICT FALLBACK: Block unless owner when age cannot be determined
                is_suspicious = not is_owner_invite
                if is_owner_invite:
                    return {
                        "allowed": True,
                        "reason":
                        "Owner invite - allowed despite unknown account age",
                        "suspicious": False,
                        "inviter_banned": False
                    }
                else:
                    return {
                        "allowed": False,
                        "reason":
                        "Account creation time unknown - blocked for security",
                        "suspicious": True,
                        "inviter_banned": False
                    }

            # Ensure timezone-aware for safe calculation
            if account_created_at.tzinfo is None:
                account_created_at = account_created_at.replace(
                    tzinfo=timezone.utc)
            if join_time.tzinfo is None:
                join_time = join_time.replace(tzinfo=timezone.utc)

            is_suspicious = (join_time -
                             account_created_at).total_seconds() <= 86400

        except Exception as e:
            error_reason = str(e)
            print(f"❌ Error in security computation: {error_reason}")
            # Keep defaults: is_owner_invite=False, is_suspicious=True for maximum security

        # 🛡️ SINGLE DECISION POINT: Block all suspicious accounts unless owner invited
        if is_suspicious and not is_owner_invite:
            reason = "Account created within 24 hours of joining - blocked for abuse prevention"
            if error_reason:
                reason += f" (computed despite error: {error_reason})"
            return {
                "allowed": False,
                "reason": reason,
                "suspicious": True,
                "inviter_banned": False
            }
        elif is_owner_invite:
            reason = "Owner invite - excluded from anti-abuse"
            if error_reason:
                reason += f" (verified despite error: {error_reason})"
            return {
                "allowed": True,
                "reason": reason,
                "suspicious": is_suspicious,
                "inviter_banned": False
            }

        # Non-suspicious, non-owner invite - proceed to database logic if available
        if not self.db_pool:
            return {
                "allowed": True,
                "reason": "Database unavailable - allowed (not suspicious)",
                "suspicious": False,
                "inviter_banned": False
            }

        # Database-dependent logic for non-suspicious accounts
        try:
            # Handle unknown inviter for non-suspicious accounts
            if not inviter_id:
                return {
                    "allowed": True,
                    "reason":
                    "Unknown inviter but normal account age - allowed",
                    "suspicious": False,
                    "inviter_banned": False
                }

            async with self.db_pool.acquire() as conn:
                # 🚫 CHECK: Is inviter already banned?
                ban_record = await conn.fetchrow(
                    '''
                    SELECT banned_from_autorole FROM inviter_abuse_stats 
                    WHERE guild_id = $1 AND inviter_id = $2
                ''', member.guild.id, inviter_id)

                if ban_record and ban_record['banned_from_autorole']:
                    # Log the invite event for tracking
                    await conn.execute(
                        '''
                        INSERT INTO invite_events (guild_id, member_id, inviter_id, invite_code, joined_at, account_created_at, suspicious, autorole_allowed, reason)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT (guild_id, member_id) DO UPDATE SET
                            inviter_id = EXCLUDED.inviter_id,
                            invite_code = EXCLUDED.invite_code,
                            suspicious = EXCLUDED.suspicious,
                            autorole_allowed = EXCLUDED.autorole_allowed,
                            reason = EXCLUDED.reason
                    ''', member.guild.id, member.id, inviter_id, invite_code,
                        join_time, account_created_at, is_suspicious, False,
                        "Inviter banned from autorole")

                    return {
                        "allowed": False,
                        "reason": "Inviter is banned from timedautorole",
                        "suspicious": is_suspicious,
                        "inviter_banned": True
                    }

                # This code only runs for non-suspicious accounts (already filtered above)
                await conn.execute(
                    '''
                    INSERT INTO invite_events (guild_id, member_id, inviter_id, invite_code, joined_at, account_created_at, suspicious, autorole_allowed, reason)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (guild_id, member_id) DO UPDATE SET
                        inviter_id = EXCLUDED.inviter_id,
                        invite_code = EXCLUDED.invite_code,
                        suspicious = EXCLUDED.suspicious,
                        autorole_allowed = EXCLUDED.autorole_allowed,
                        reason = EXCLUDED.reason
                ''', member.guild.id, member.id, inviter_id, invite_code,
                    join_time, account_created_at, False, True,
                    "Normal account - allowed")

                return {
                    "allowed": True,
                    "reason": "Normal account age - allowed",
                    "suspicious": False,
                    "inviter_banned": False
                }

        except Exception as e:
            print(f"❌ Error checking timedautorole eligibility: {str(e)}")

            # 🚨 EXCEPTION-SAFE: Database errors for non-suspicious accounts
            print(f"❌ Database error in timedautorole check: {str(e)}")
            return {
                "allowed": True,
                "reason":
                f"Database error but account not suspicious - allowing: {str(e)}",
                "suspicious": False,
                "inviter_banned": False
            }

    def calculate_level(self, message_count):
        """Calculate user level based on message count"""
        for level in sorted(LEVEL_SYSTEM["level_requirements"].keys(),
                            reverse=True):
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
                        await member.add_roles(
                            new_role, reason=f"Level {new_level} achieved")
                        await self.log_to_discord(
                            f"🎉 {member.display_name} leveled up to Level {new_level}! Role '{new_role.name}' assigned."
                        )

                        # Send congratulations DM
                        try:
                            dm_message = f"Congratulations! You've leveled up to level {new_level}!"
                            await user.send(dm_message)
                            await self.log_to_discord(
                                f"📬 Sent level-up DM to {member.display_name} for Level {new_level}"
                            )
                        except discord.Forbidden:
                            await self.log_to_discord(
                                f"⚠️ Could not send level-up DM to {member.display_name} (DMs disabled)"
                            )

                    except discord.Forbidden:
                        await self.log_to_discord(
                            f"❌ No permission to assign Level {new_level} role to {member.display_name}"
                        )
                else:
                    await self.log_to_discord(
                        f"❌ Could not find member {user.display_name} in guild for level-up"
                    )
            else:
                await self.log_to_discord(
                    f"❌ Could not find Level {new_level} role (ID: {new_role_id})"
                )

        except Exception as e:
            await self.log_to_discord(
                f"❌ Error handling level up for {user.display_name}: {str(e)}")

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
            await self.handle_level_up(message.author, message.guild,
                                       old_level, new_level)

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
                if data.get("weekend_delayed",
                            False) and "expiry_time" in data:
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
                    # Normal members - 72 hours (3 days) from role_added_time
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

                    expiry_time = role_added_time + timedelta(hours=72)

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
                                "That means your 3-day welcome gift has officially started. "
                                "You now have full access to the premium channel. "
                                "Let's make the most of it by securing some wins together!"
                            )
                            await member.send(activation_message)
                            print(
                                f"✅ Sent Monday activation DM to {member.display_name}"
                            )

                            # Mark as processed and remove from weekend_pending
                            del AUTO_ROLE_CONFIG["weekend_pending"][member_id]
                            await self.save_auto_role_config()
                except Exception as e:
                    await self.log_to_discord(
                        f"❌ Error processing Monday activation for member {member_id}: {str(e)}"
                    )

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
            "Hey! It's been 3 days since your **3-day free access to the Premium Signals channel** ended. We hope you were able to catch good trades with us during that time.\n\nAs you've probably seen, the **free signals channel only gets about 1 signal a day**, while inside **Gold Pioneers**, members receive **8–10 high-quality signals every single day in <#1350929852299214999>**. That means way more chances to profit and grow consistently.\n\nWe'd love to **invite you back to Premium Signals** so you don't miss out on more solid opportunities.\n\n**Feel free to join us again through this link:** <https://whop.com/gold-pioneer>",
            7:
            "It's been a week since your Premium Signals trial ended. Since then, our **Gold Pioneers  have been catching trade setups daily in <#1350929852299214999>**.\n\nIf you found value in just 3 days, imagine the results you could be seeing by now with full access. It's all about **consistency and staying plugged into the right information**.\n\nWe'd like to **personally invite you to rejoin Premium Signals** and get back into the rhythm.\n\n\n**Feel free to join us again through this link:** <https://whop.com/gold-pioneer>",
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
                retry_count = AUTO_ROLE_CONFIG["dm_schedule"][
                    msg_data['member_id']].get(
                        f"dm_{msg_data['days']}_retry_count", 0)
                max_retries = 3

                if retry_count < max_retries:
                    # Increment retry count and try again later
                    AUTO_ROLE_CONFIG["dm_schedule"][msg_data['member_id']][
                        f"dm_{msg_data['days']}_retry_count"] = retry_count + 1
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
                                    "That means your 3-day welcome gift has officially started. "
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
                default_message = "Hey! Your **3-day free access** to the premium channel has unfortunately **ran out**. We truly hope that you were able to benefit with us & we hope to see you back soon! For now, feel free to continue following our trade signals in <#1350929790148022324>."
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


bot = TradingBot()

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
    'CADCHF': {
        'decimals': 4,
        'pip_value': 0.0001
    },  # Same as GBPUSD
    'AUDCHF': {
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
            # Normal member - 72 hours from role_added_time
            role_added_time = datetime.fromisoformat(data["role_added_time"])
            if role_added_time.tzinfo is None:
                if PYTZ_AVAILABLE:
                    role_added_time = AMSTERDAM_TZ.localize(role_added_time)
                else:
                    role_added_time = role_added_time.replace(
                        tzinfo=AMSTERDAM_TZ)
            else:
                role_added_time = role_added_time.astimezone(AMSTERDAM_TZ)

            expiry_time = role_added_time + timedelta(hours=72)
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
            print(f"🔒 Setting up command permissions for owner: {owner_id}")

            # Get all commands
            commands_to_setup = []

            # Get all individual commands
            for cmd in self.tree.get_commands():
                if hasattr(cmd, 'name') and hasattr(cmd,
                                                    'default_permissions'):
                    commands_to_setup.append(cmd)

            # Set up permissions for each guild
            permissions_set = 0
            for guild in self.guilds:
                try:
                    # Create permission overwrite for bot owner
                    for command in commands_to_setup:
                        try:
                            # Set permission for owner to use the command
                            await command.set_permissions(
                                guild,
                                {discord.Object(id=owner_id): True},
                                sync=False  # Don't sync immediately
                            )
                            permissions_set += 1
                        except Exception as cmd_err:
                            print(
                                f"   ⚠️ Failed to set permission for {command.name}: {cmd_err}"
                            )

                    # Also set permissions for giveaway group
                    try:
                        await giveaway_group.set_permissions(
                            guild, {discord.Object(id=owner_id): True},
                            sync=False)
                        permissions_set += len(giveaway_group.commands)
                    except Exception as group_err:
                        print(
                            f"   ⚠️ Failed to set giveaway group permissions: {group_err}"
                        )

                    # Sync permissions for this guild
                    await self.tree.sync(guild=guild)
                    print(f"   ✅ Set permissions for commands in {guild.name}")

                except Exception as guild_err:
                    print(
                        f"   ❌ Failed to set permissions in {guild.name}: {guild_err}"
                    )

            print(
                f"✅ Owner permissions setup complete! {permissions_set} command permissions set"
            )

        except ValueError:
            print(f"❌ Invalid BOT_OWNER_USER_ID format: {BOT_OWNER_USER_ID}")
        except Exception as e:
            print(f"❌ Error setting up owner permissions: {e}")


# Owner permission check function
def is_bot_owner(user_id: int) -> bool:
    """Check if the user is the bot owner"""
    if not BOT_OWNER_USER_ID:
        return False  # If no owner ID is set, block all users for security
    return str(user_id) == BOT_OWNER_USER_ID


async def owner_check(interaction: discord.Interaction) -> bool:
    """Check if the user is the bot owner and send error message if not"""
    if not is_bot_owner(interaction.user.id):
        await interaction.response.send_message(
            "❌ This command can only be used by the bot owner.",
            ephemeral=True)
        return False
    return True


@bot.tree.command(name="timedautorole",
                  description="View timed auto-role status and active members")
@app_commands.describe(
    action="Check status or list active members"
)
async def timed_auto_role_command(interaction: discord.Interaction,
                                  action: str):
    """View the timed auto-role system status and active members"""

    # Check if user is bot owner
    if not await owner_check(interaction):
        return

    try:
        if action.lower() == "status":
            if AUTO_ROLE_CONFIG["enabled"]:
                role = interaction.guild.get_role(
                    AUTO_ROLE_CONFIG["role_id"]
                ) if interaction.guild and AUTO_ROLE_CONFIG["role_id"] else None

                # Count only members who actually have the role and aren't expired
                actual_active_count = 0
                for member_id, data in AUTO_ROLE_CONFIG[
                        "active_members"].items():
                    try:
                        # Check if member exists and has the role
                        guild = interaction.guild
                        if not guild:
                            continue

                        member = guild.get_member(int(member_id))
                        if not member:
                            continue

                        # Check if member has the role and isn't expired
                        if role and role in member.roles:
                            time_display = get_remaining_time_display(
                                member_id)
                            if time_display is not None:  # Not expired
                                actual_active_count += 1
                    except Exception:
                        continue

                weekend_pending_count = len(
                    AUTO_ROLE_CONFIG.get("weekend_pending", {}))

                status_message = f"✅ **Auto-role system is ENABLED**\n"
                if role:
                    status_message += f"• **Role:** {role.mention}\n"
                else:
                    status_message += f"• **Role:** Not found (ID: {AUTO_ROLE_CONFIG['role_id']})\n"
                status_message += f"• **Duration:** 72 hours / 3 days (fixed)\n"
                status_message += f"• **Active members:** {actual_active_count}\n"
                status_message += f"• **Weekend pending:** {weekend_pending_count}\n"
                status_message += f"• **Weekend handling:** Enabled"
            else:
                status_message = "❌ **Auto-role system is DISABLED**"

            await interaction.response.send_message(status_message,
                                                    ephemeral=True)

        elif action.lower() == "list":
            if not AUTO_ROLE_CONFIG["enabled"]:
                await interaction.response.send_message(
                    "❌ Auto-role system is disabled. No active members to display.",
                    ephemeral=True)
                return

            if not AUTO_ROLE_CONFIG["active_members"]:
                await interaction.response.send_message(
                    "📝 No members currently have temporary roles.",
                    ephemeral=True)
                return

            # Build the list of active members grouped by join date
            members_by_date = {}

            # Get the role object for checking
            role = interaction.guild.get_role(
                AUTO_ROLE_CONFIG["role_id"]
            ) if interaction.guild and AUTO_ROLE_CONFIG["role_id"] else None

            for member_id, data in AUTO_ROLE_CONFIG["active_members"].items():
                try:
                    # Get member info
                    guild = interaction.guild
                    if not guild:
                        continue

                    member = guild.get_member(int(member_id))
                    if not member:
                        continue

                    # Only show members who actually have the role
                    if role and role not in member.roles:
                        continue

                    # Get precise remaining time
                    time_display = get_remaining_time_display(member_id)
                    # Only add members who aren't expired (time_display will be None for expired)
                    if time_display is not None:
                        # Get the join date from role_added_time
                        role_added_time = data.get("role_added_time")
                        if role_added_time:
                            join_dt = datetime.fromisoformat(role_added_time)
                            if join_dt.tzinfo is None:
                                join_dt = join_dt.replace(tzinfo=AMSTERDAM_TZ)
                            join_date = join_dt.strftime("%d-%m-%Y")
                        else:
                            join_date = "Unknown"
                        
                        if join_date not in members_by_date:
                            members_by_date[join_date] = []
                        members_by_date[join_date].append(
                            f"• {member.display_name} - {time_display}")

                except Exception as e:
                    print(f"Error processing member {member_id}: {str(e)}")
                    continue

            if not members_by_date:
                await interaction.response.send_message(
                    "📝 No valid members found with temporary roles.",
                    ephemeral=True)
                return

            # Get role name for header
            role = interaction.guild.get_role(
                AUTO_ROLE_CONFIG["role_id"]
            ) if interaction.guild and AUTO_ROLE_CONFIG["role_id"] else None
            role_name = role.name if role else "Unknown Role"

            # Sort dates (newest first) and count total members
            sorted_dates = sorted(members_by_date.keys(), 
                                  key=lambda x: datetime.strptime(x, "%d-%m-%Y") if x != "Unknown" else datetime.min, 
                                  reverse=True)
            total_members = sum(len(members) for members in members_by_date.values())

            # Create embeds with date sections
            embeds = []
            current_embed = discord.Embed(
                title=f"📋 Active Temporary Role Members",
                description=f"**Role:** {role_name}\n**Total:** {total_members} members",
                color=0xFFD700
            )
            current_field_count = 0

            for join_date in sorted_dates:
                members = members_by_date[join_date]
                member_text = "\n".join(members)
                
                # Discord embed field value limit is 1024 chars
                # If too long, split into multiple fields
                if len(member_text) > 1024:
                    chunks = []
                    current_chunk = []
                    current_length = 0
                    for m in members:
                        if current_length + len(m) + 1 > 1000:
                            chunks.append("\n".join(current_chunk))
                            current_chunk = [m]
                            current_length = len(m)
                        else:
                            current_chunk.append(m)
                            current_length += len(m) + 1
                    if current_chunk:
                        chunks.append("\n".join(current_chunk))
                    
                    for i, chunk in enumerate(chunks):
                        field_name = f"📅 {join_date}" if i == 0 else f"📅 {join_date} (cont.)"
                        if current_field_count >= 25:
                            current_embed.set_footer(text=f"Duration: 72h (3 days) | Weekend: 120h (5 days)")
                            embeds.append(current_embed)
                            current_embed = discord.Embed(
                                title=f"📋 Active Temporary Role Members (continued)",
                                color=0xFFD700
                            )
                            current_field_count = 0
                        current_embed.add_field(name=field_name, value=chunk, inline=False)
                        current_field_count += 1
                else:
                    if current_field_count >= 25:
                        current_embed.set_footer(text=f"Duration: 72h (3 days) | Weekend: 120h (5 days)")
                        embeds.append(current_embed)
                        current_embed = discord.Embed(
                            title=f"📋 Active Temporary Role Members (continued)",
                            color=0xFFD700
                        )
                        current_field_count = 0
                    current_embed.add_field(
                        name=f"📅 {join_date} ({len(members)} members)",
                        value=member_text,
                        inline=False
                    )
                    current_field_count += 1

            # Add the last embed
            current_embed.set_footer(text=f"Duration: 72h (3 days) | Weekend: 120h (5 days)")
            embeds.append(current_embed)

            # Send first embed as response, rest as followups
            await interaction.response.send_message(embed=embeds[0], ephemeral=True)
            for embed in embeds[1:]:
                await interaction.followup.send(embed=embed, ephemeral=True)

        else:
            await interaction.response.send_message(
                "❌ Invalid action. Use 'status' or 'list'.",
                ephemeral=True)

    except Exception as e:
        await interaction.response.send_message(
            f"❌ Error: {str(e)}", ephemeral=True)


@timed_auto_role_command.autocomplete('action')
async def action_autocomplete(interaction: discord.Interaction, current: str):
    actions = ['status', 'list']
    return [
        app_commands.Choice(name=action, value=action) for action in actions
        if current.lower() in action.lower()
    ]


@bot.tree.command(name="entry", description="Create a trading signal")
@app_commands.describe(
    entry_type="Type of entry (Long, Short, Long Swing, Short Swing)",
    pair="Trading pair",
    price="Entry price",
    channels="Select channel destination")
async def entry_command(interaction: discord.Interaction, entry_type: str,
                        pair: str, price: float, channels: str):
    """Create and send a trading signal to specified channels"""

    if not await owner_check(interaction):
        return

    try:
        # Calculate TP and SL levels
        levels = calculate_levels(price, pair, entry_type)

        # Create the signal message
        signal_message = f"""**Trade Signal For: {pair}**
Entry Type: {entry_type}
Entry Price: {levels['entry']}

**Take Profit Levels:**
Take Profit 1: {levels['tp1']}
Take Profit 2: {levels['tp2']}
Take Profit 3: {levels['tp3']}

Stop Loss: {levels['sl']}"""

        # Always add @everyone at the bottom
        signal_message += f"\n\n@everyone"

        # Add special note for US100 & GER40
        if pair.upper() in ['US100', 'GER40']:
            signal_message += f"\n\n**Please note that prices on US100 & GER40 vary a lot from broker to broker, so it is possible that the current price in our signal is different than the current price with your broker. Execute this signal within a 5 minute window of this trade being sent and please manually recalculate the pip value for TP1/2/3 & SL depending on your broker's current price.**"

        # Channel mapping
        channel_mapping = {
            "Free channel": [1350929790148022324],
            "Premium channel": [1384668129036075109],
            "Both": [1350929790148022324, 1384668129036075109],
            "Testing": [1394958907943817326]
        }

        target_channels = channel_mapping.get(channels, [])
        sent_channels = []

        for channel_id in target_channels:
            target_channel = bot.get_channel(channel_id)

            if target_channel and isinstance(target_channel,
                                             discord.TextChannel):
                try:
                    await target_channel.send(signal_message)
                    sent_channels.append(target_channel.name)
                except discord.Forbidden:
                    await interaction.followup.send(
                        f"❌ No permission to send to #{target_channel.name}",
                        ephemeral=True)
                except Exception as e:
                    await interaction.followup.send(
                        f"❌ Error sending to #{target_channel.name}: {str(e)}",
                        ephemeral=True)

        if sent_channels:
            await interaction.response.send_message(
                f"✅ Signal sent to: {', '.join(sent_channels)}",
                ephemeral=True)

            # 🚀 INSTANT PRICE CHECK: Check new trade immediately instead of waiting 8 minutes
            # This optimizes API usage while ensuring immediate monitoring at signal creation
            try:
                await asyncio.sleep(
                    3
                )  # Brief delay to allow message processing and potential tracking setup

                # Get active trades and find this newly created one
                active_trades = await bot.get_active_trades_from_db()
                new_trade_found = False

                for message_id, trade_data in active_trades.items():
                    if (trade_data.get('pair') == pair
                            and abs(float(trade_data.get('entry', 0)) - price)
                            < 0.01):  # Match by pair and entry price
                        await bot.check_single_trade_immediately(
                            message_id, trade_data)
                        new_trade_found = True
                        await bot.debug_to_channel(
                            "INSTANT CHECK",
                            f"🚀 Instant price check completed for new {pair} signal",
                            "🚀")
                        break

                if not new_trade_found:
                    await bot.debug_to_channel(
                        "INSTANT CHECK",
                        f"⚠️ New {pair} signal not found in active trades yet - normal for limit orders",
                        "⚠️")

            except Exception as e:
                await bot.debug_to_channel(
                    "INSTANT CHECK ERROR",
                    f"❌ Error during instant check for {pair}: {str(e)}", "❌")
                print(f"Instant check error: {e}")
        else:
            await interaction.response.send_message(
                "❌ No valid channels found or no messages sent.",
                ephemeral=True)

    except Exception as e:
        await interaction.response.send_message(
            f"❌ Error creating signal: {str(e)}", ephemeral=True)


@entry_command.autocomplete('entry_type')
async def entry_type_autocomplete(interaction: discord.Interaction,
                                  current: str):
    types = ['Buy limit', 'Sell limit', 'Buy execution', 'Sell execution']
    return [
        app_commands.Choice(name=entry_type, value=entry_type)
        for entry_type in types if current.lower() in entry_type.lower()
    ]


@entry_command.autocomplete('pair')
async def pair_autocomplete(interaction: discord.Interaction, current: str):
    # Organized pairs by currency groups for easier navigation
    pairs = [
        # USD pairs
        'EURUSD',
        'GBPUSD',
        'AUDUSD',
        'NZDUSD',
        'USDCAD',
        'USDCHF',
        'XAUUSD',
        'BTCUSD',
        # JPY pairs
        'GBPJPY',
        'CHFJPY',
        'CADJPY',
        'AUDJPY',
        # CHF pairs
        'GBPCHF',
        'CADCHF',
        'AUDCHF',
        # CAD pairs
        'GBPCAD',
        'EURCAD',
        'AUDCAD',
        # Cross pairs
        'AUDNZD',
        # Indices
        'US100',
        'US500',
        'GER40'
    ]
    return [
        app_commands.Choice(name=pair, value=pair) for pair in pairs
        if current.lower() in pair.lower()
    ]


@entry_command.autocomplete('channels')
async def channels_autocomplete(interaction: discord.Interaction,
                                current: str):
    channel_choices = ['Free channel', 'Premium channel', 'Both', 'Testing']
    return [
        app_commands.Choice(name=choice, value=choice)
        for choice in channel_choices if current.lower() in choice.lower()
    ]


@bot.tree.command(name="dbstatus",
                  description="Check database connection and status")
async def database_status_command(interaction: discord.Interaction):
    """Check database connection status and show database information"""

    if not await owner_check(interaction):
        return

    await interaction.response.defer(ephemeral=True)

    if not bot.db_pool:
        embed = discord.Embed(
            title="📊 Database Status",
            description=
            "❌ **Database not configured**\n\nThe bot is running without database persistence.\nMemory-based storage is being used instead.",
            color=discord.Color.orange())
        embed.add_field(
            name="💡 To Enable Database",
            value=
            "Add a PostgreSQL service to your Render deployment and set the DATABASE_URL environment variable.",
            inline=False)
        await interaction.followup.send(embed=embed)
        return

    try:
        async with bot.db_pool.acquire() as conn:
            # Get database info
            version = await conn.fetchval('SELECT version()')
            current_time = await conn.fetchval('SELECT NOW()')

            # Get table count
            table_count = await conn.fetchval("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)

            # Get connection info
            pool_size = bot.db_pool.get_size()
            pool_idle = bot.db_pool.get_idle_size()

            embed = discord.Embed(
                title="📊 Database Status",
                description="✅ **Database Connected & Working**",
                color=discord.Color.green())

            embed.add_field(
                name="🗄️ PostgreSQL Info",
                value=
                f"Version: {version.split()[1]}\nServer Time: {current_time.strftime('%Y-%m-%d %H:%M:%S UTC')}",
                inline=True)

            embed.add_field(
                name="📊 Connection Pool",
                value=
                f"Pool Size: {pool_size}\nIdle Connections: {pool_idle}\nActive: {pool_size - pool_idle}",
                inline=True)

            embed.add_field(name="📋 Tables",
                            value=f"Total Tables: {table_count}",
                            inline=True)

            # Check specific bot tables
            bot_tables = [
                'role_history', 'active_members', 'weekend_pending',
                'dm_schedule', 'auto_role_config'
            ]
            existing_tables = []

            for table in bot_tables:
                exists = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = $1
                    )
                """, table)
                if exists:
                    existing_tables.append(table)

            if existing_tables:
                embed.add_field(
                    name="🤖 Bot Tables",
                    value=f"Created: {len(existing_tables)}/{len(bot_tables)}\n"
                    + "\n".join(f"✅ {table}" for table in existing_tables),
                    inline=False)

            embed.set_footer(
                text=
                "Database is functioning properly for persistent memory storage"
            )

    except Exception as e:
        embed = discord.Embed(title="📊 Database Status",
                              description="❌ **Database Connection Error**",
                              color=discord.Color.red())
        embed.add_field(name="Error Details",
                        value=f"```{str(e)[:500]}```",
                        inline=False)
        embed.add_field(
            name="💡 Troubleshooting",
            value=
            "1. Check DATABASE_URL environment variable\n2. Verify PostgreSQL service is running\n3. Check network connectivity",
            inline=False)

    await interaction.followup.send(embed=embed)


# Level System Command
@bot.tree.command(
    name="level",
    description=
    "Check level information for yourself or another user, or view leaderboard"
)
@app_commands.describe(
    user="User to check level for (leave empty to check your own level)",
    show_leaderboard="Show server leaderboard instead of individual level")
async def level_command(interaction: discord.Interaction,
                        user: discord.Member = None,
                        show_leaderboard: bool = False):
    """Check level information or display leaderboard"""

    if not await owner_check(interaction):
        return

    # If leaderboard is requested, show top users
    if show_leaderboard:
        await interaction.response.defer(ephemeral=True)

        if not LEVEL_SYSTEM["user_data"]:
            await interaction.followup.send(
                "📊 No level data available yet. Users need to send messages to start leveling up!",
                ephemeral=True)
            return

        # Get guild members and filter level data to current guild
        guild_users = []
        for user_id, data in LEVEL_SYSTEM["user_data"].items():
            if data.get("guild_id") == interaction.guild.id:
                guild_users.append((user_id, data))

        # Sort by level (descending), then by message count (descending)
        guild_users.sort(key=lambda x:
                         (x[1]["current_level"], x[1]["message_count"]),
                         reverse=True)

        # Create leaderboard embed
        embed = discord.Embed(
            title=f"🏆 {interaction.guild.name} Level Leaderboard",
            description=
            "Top active community members ranked by level and messages",
            color=discord.Color.gold())

        # Show top 10 users
        leaderboard_text = ""
        for i, (user_id, data) in enumerate(guild_users[:10], 1):
            try:
                member = interaction.guild.get_member(int(user_id))
                if member:
                    # Medal emojis for top 3
                    medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"**{i}.**"
                    level = data["current_level"]
                    messages = data["message_count"]

                    level_display = f"Level {level}" if level > 0 else "No Level"
                    leaderboard_text += f"{medal} **{member.display_name}**\n"
                    leaderboard_text += f"    └ {level_display} • {messages:,} messages\n\n"
            except Exception as e:
                print(
                    f"Error processing leaderboard entry for user {user_id}: {e}"
                )
                continue

        if leaderboard_text:
            embed.add_field(name="📈 Top Community Members",
                            value=leaderboard_text,
                            inline=False)

            # Add server stats
            total_users = len(guild_users)
            total_messages = sum(data["message_count"]
                                 for _, data in guild_users)
            avg_level = sum(data["current_level"] for _, data in guild_users
                            ) / total_users if total_users > 0 else 0

            embed.add_field(
                name="📊 Server Statistics",
                value=f"• **Total Active Users**: {total_users:,}\n" +
                f"• **Total Messages**: {total_messages:,}\n" +
                f"• **Average Level**: {avg_level:.1f}",
                inline=False)
        else:
            embed.add_field(name="📊 Leaderboard",
                            value="No active users found in this server.",
                            inline=False)

        embed.set_footer(
            text=
            "Use /level without leaderboard option to check your individual progress"
        )
        await interaction.followup.send(embed=embed, ephemeral=True)
        return

    # Individual level check (original functionality)
    target_user = user or interaction.user
    user_id = str(target_user.id)

    if user_id not in LEVEL_SYSTEM["user_data"]:
        await interaction.response.send_message(
            f"**{target_user.display_name}** hasn't sent any messages yet. Start chatting to level up!",
            ephemeral=True)
        return

    user_data = LEVEL_SYSTEM["user_data"][user_id]
    current_level = user_data["current_level"]
    message_count = user_data["message_count"]

    # Calculate progress to next level
    next_level = current_level + 1
    if next_level <= 8:  # Max level is 8
        messages_needed = LEVEL_SYSTEM["level_requirements"][next_level]
        remaining = messages_needed - message_count
        progress_percentage = (message_count / messages_needed) * 100
    else:
        # Max level reached
        messages_needed = None
        remaining = 0
        progress_percentage = 100

    # Create simplified embed
    embed = discord.Embed(color=discord.Color.gold())

    # Main status line
    if current_level > 0:
        status_line = f"🏆 **Level {current_level}** • {message_count:,} messages sent"
    else:
        status_line = f"📊 **Level 0** • {message_count:,} messages sent"

    embed.add_field(name="Your Status", value=status_line, inline=False)

    # Progress to next level
    if next_level <= 8:
        # Create progress bar
        bar_length = 10
        filled = int((progress_percentage / 100) * bar_length)
        bar = "█" * filled + "░" * (bar_length - filled)

        progress_text = (
            f"{bar} **{progress_percentage:.0f}%**\n"
            f"**{remaining:,}** more messages to reach **Level {next_level}**")
        embed.add_field(name="Next Level", value=progress_text, inline=False)
    else:
        embed.add_field(name="🎉 Max Level Reached!",
                        value="You've achieved the highest level!",
                        inline=False)

    embed.set_footer(text="Keep chatting to level up!")

    await interaction.response.send_message(embed=embed, ephemeral=True)


# Welcome DM Command
# Hardcoded welcome message
WELCOME_DM_MESSAGE = "Hey 👋, There's currently an active giveaway in <#1405490561963786271>. Don't forget to join! 🎉"


@bot.tree.command(
    name="welcomedm",
    description="[OWNER ONLY] Configure welcome DMs for new members")
@app_commands.describe(
    action="What do you want to do?",
    delay="[CONFIGURE] Minutes to wait before sending DM (1-1440)")
async def welcome_dm_command(interaction: discord.Interaction,
                             action: str,
                             delay: int = None):
    """Configure welcome DM system for new members"""

    if not await owner_check(interaction):
        return

    try:
        if action.lower() == "enable":
            if bot.db_pool:
                async with bot.db_pool.acquire() as conn:
                    await conn.execute(
                        'UPDATE welcome_dm_config SET enabled = TRUE WHERE id = 1'
                    )
                    result = await conn.fetchrow(
                        'SELECT * FROM welcome_dm_config WHERE id = 1')
                    if not result:
                        await conn.execute(
                            '''
                            INSERT INTO welcome_dm_config (id, enabled, delay_minutes, message)
                            VALUES (1, TRUE, 5, $1)
                        ''', WELCOME_DM_MESSAGE)
                        result = await conn.fetchrow(
                            'SELECT * FROM welcome_dm_config WHERE id = 1')

                    await interaction.response.send_message(
                        f"✅ **Welcome DM system enabled!**\n"
                        f"• **Delay:** {result['delay_minutes']} minutes\n"
                        f"• **Message:** {result['message'][:100]}...\n\n"
                        f"New members will receive a DM {result['delay_minutes']} minute(s) after joining.",
                        ephemeral=True)
            else:
                await interaction.response.send_message(
                    "❌ Database not available. Cannot enable welcome DMs.",
                    ephemeral=True)

        elif action.lower() == "disable":
            if bot.db_pool:
                async with bot.db_pool.acquire() as conn:
                    await conn.execute(
                        'UPDATE welcome_dm_config SET enabled = FALSE WHERE id = 1'
                    )
                await interaction.response.send_message(
                    "✅ Welcome DM system disabled. No DMs will be sent to new members.",
                    ephemeral=True)
            else:
                await interaction.response.send_message(
                    "❌ Database not available.", ephemeral=True)

        elif action.lower() == "configure":
            if delay is None:
                await interaction.response.send_message(
                    "❌ Please provide a delay (minutes) to configure.\n"
                    "Example: `/welcomedm action:configure delay:10`",
                    ephemeral=True)
                return

            if delay < 1 or delay > 1440:
                await interaction.response.send_message(
                    "❌ Delay must be between 1 and 1440 minutes (24 hours).",
                    ephemeral=True)
                return

            if bot.db_pool:
                async with bot.db_pool.acquire() as conn:
                    result = await conn.fetchrow(
                        'SELECT * FROM welcome_dm_config WHERE id = 1')
                    if not result:
                        await conn.execute(
                            '''
                            INSERT INTO welcome_dm_config (id, enabled, delay_minutes, message)
                            VALUES (1, FALSE, $1, $2)
                        ''', delay, WELCOME_DM_MESSAGE)
                    else:
                        await conn.execute(
                            'UPDATE welcome_dm_config SET delay_minutes = $1, message = $2 WHERE id = 1',
                            delay, WELCOME_DM_MESSAGE)

                    result = await conn.fetchrow(
                        'SELECT * FROM welcome_dm_config WHERE id = 1')

                    await interaction.response.send_message(
                        f"✅ **Welcome DM configured!**\n"
                        f"• Delay: {delay} minutes\n"
                        f"• Message: {WELCOME_DM_MESSAGE}\n\n"
                        f"Current status: {'Enabled ✅' if result['enabled'] else 'Disabled ❌'}",
                        ephemeral=True)
            else:
                await interaction.response.send_message(
                    "❌ Database not available.", ephemeral=True)

        elif action.lower() == "status":
            if bot.db_pool:
                async with bot.db_pool.acquire() as conn:
                    result = await conn.fetchrow(
                        'SELECT * FROM welcome_dm_config WHERE id = 1')

                    if result:
                        embed = discord.Embed(
                            title="📨 Welcome DM System Status",
                            color=discord.Color.green()
                            if result['enabled'] else discord.Color.red())
                        embed.add_field(name="Status",
                                        value="✅ Enabled"
                                        if result['enabled'] else "❌ Disabled",
                                        inline=True)
                        embed.add_field(
                            name="Delay",
                            value=f"{result['delay_minutes']} minutes",
                            inline=True)
                        embed.add_field(name="Message",
                                        value=WELCOME_DM_MESSAGE,
                                        inline=False)
                        await interaction.response.send_message(embed=embed,
                                                                ephemeral=True)
                    else:
                        await interaction.response.send_message(
                            "ℹ️ Welcome DM system not configured yet.\n"
                            "Use `/welcomedm action:configure` to set it up.",
                            ephemeral=True)
            else:
                await interaction.response.send_message(
                    "❌ Database not available.", ephemeral=True)
        else:
            await interaction.response.send_message(
                "❌ Invalid action. Use: `enable`, `disable`, `configure`, or `status`",
                ephemeral=True)

    except Exception as e:
        await interaction.response.send_message(f"❌ Error: {str(e)}",
                                                ephemeral=True)
        print(f"Welcome DM command error: {e}")


@welcome_dm_command.autocomplete('action')
async def welcome_dm_action_autocomplete(interaction: discord.Interaction,
                                         current: str):
    actions = ['enable', 'disable', 'configure', 'status']
    return [
        app_commands.Choice(name=action, value=action) for action in actions
        if current.lower() in action.lower()
    ]


class GiveawayMenuView(discord.ui.View):
    """Main interactive menu for giveaway management"""

    def __init__(self):
        super().__init__(timeout=300)
        self.add_item(GiveawayActionDropdown())

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class GiveawayActionDropdown(discord.ui.Select):
    """Dropdown to select giveaway action"""

    def __init__(self):
        options = [
            discord.SelectOption(
                label="Create Giveaway",
                description="Create a new giveaway with custom settings",
                value="create",
                emoji="📝"),
            discord.SelectOption(
                label="List Active Giveaways",
                description="View all currently running giveaways",
                value="list",
                emoji="📋"),
            discord.SelectOption(label="User",
                                 description="Select a user for a giveaway",
                                 value="choose_winner",
                                 emoji="🎯"),
            discord.SelectOption(label="End Giveaway",
                                 description="End a giveaway early",
                                 value="end",
                                 emoji="🏁")
        ]

        super().__init__(placeholder="🎉 Select a giveaway action...",
                         min_values=1,
                         max_values=1,
                         options=options)

    async def callback(self, interaction: discord.Interaction):
        action = self.values[0]

        if action == "create":
            view = RoleSelectionView(interaction.guild)
            embed = discord.Embed(
                title="📝 Create Giveaway - Step 1/2",
                description="Select the role required to enter this giveaway:",
                color=discord.Color.blue())
            await interaction.response.send_message(embed=embed,
                                                    view=view,
                                                    ephemeral=True)

        elif action == "list":
            await interaction.response.defer()

            if not ACTIVE_GIVEAWAYS:
                embed = discord.Embed(title="📋 Active Giveaways",
                                      description="No active giveaways found.",
                                      color=discord.Color.blue())
                await interaction.followup.send(embed=embed, ephemeral=True)
                return

            giveaway_list = []
            for gid, data in ACTIVE_GIVEAWAYS.items():
                end_time = data['end_time']
                if isinstance(end_time, str):
                    end_time = datetime.fromisoformat(end_time)
                if end_time.tzinfo is None:
                    end_time = end_time.replace(tzinfo=AMSTERDAM_TZ)

                time_left = end_time - datetime.now(AMSTERDAM_TZ)
                if time_left.total_seconds() > 0:
                    hours_left = int(time_left.total_seconds() // 3600)
                    minutes_left = int(
                        (time_left.total_seconds() % 3600) // 60)
                    chosen_count = len(data.get('chosen_winners', []))

                    giveaway_list.append(
                        f"**{gid}**\n" +
                        f"  ⏰ Time left: {hours_left}h {minutes_left}m\n" +
                        f"  🏆 Winners: {data['winner_count']}\n" +
                        f"  🎯 Guaranteed: {chosen_count}/{data['winner_count']}\n"
                    )

            if not giveaway_list:
                embed = discord.Embed(title="📋 Active Giveaways",
                                      description="No active giveaways found.",
                                      color=discord.Color.blue())
            else:
                embed = discord.Embed(title="📋 Active Giveaways",
                                      description="\n".join(giveaway_list),
                                      color=discord.Color.gold())

            await interaction.followup.send(embed=embed, ephemeral=True)

        elif action == "choose_winner":
            if not ACTIVE_GIVEAWAYS:
                embed = discord.Embed(
                    title="🎯 User",
                    description=
                    "❌ No active giveaways found.\n\nCreate a giveaway first before selecting a User.",
                    color=discord.Color.red())
                await interaction.response.send_message(embed=embed,
                                                        ephemeral=True)
                return

            view = ChooseWinnerView(ACTIVE_GIVEAWAYS)
            embed = discord.Embed(
                title="🎯 User Selection",
                description="Select a giveaway and then select a user:",
                color=discord.Color.blue())
            await interaction.response.send_message(embed=embed,
                                                    view=view,
                                                    ephemeral=True)

        elif action == "end":
            if not ACTIVE_GIVEAWAYS:
                embed = discord.Embed(
                    title="🏁 End Giveaway",
                    description=
                    "❌ No active giveaways found.\n\nThere are no giveaways to end.",
                    color=discord.Color.red())
                await interaction.response.send_message(embed=embed,
                                                        ephemeral=True)
                return

            view = EndGiveawayView(ACTIVE_GIVEAWAYS)
            embed = discord.Embed(
                title="🏁 End Giveaway Early",
                description="Select a giveaway to end and select winners:",
                color=discord.Color.orange())
            await interaction.response.send_message(embed=embed,
                                                    view=view,
                                                    ephemeral=True)


class RoleSelectionView(discord.ui.View):
    """View for selecting required role before creating giveaway"""

    def __init__(self, guild):
        super().__init__(timeout=300)
        self.guild = guild
        self.add_item(RoleSelectionDropdown(guild))

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class RoleSelectionDropdown(discord.ui.Select):
    """Dropdown to select required role for giveaway"""

    def __init__(self, guild):
        roles = [
            role for role in guild.roles
            if role.name != "@everyone" and not role.managed
        ]

        options = []
        for role in roles[:25]:
            options.append(
                discord.SelectOption(label=role.name,
                                     value=str(role.id),
                                     description=f"ID: {role.id}"))

        super().__init__(placeholder="🎭 Select required role for entry...",
                         min_values=1,
                         max_values=1,
                         options=options)
        self.guild = guild

    async def callback(self, interaction: discord.Interaction):
        role_id = int(self.values[0])
        role = self.guild.get_role(role_id)

        if not role:
            await interaction.response.send_message(
                "❌ Role not found. Please try again.", ephemeral=True)
            return

        modal = GiveawayCreateModal(role)
        await interaction.response.send_modal(modal)


class GiveawayCreateModal(discord.ui.Modal, title="Create Giveaway"):
    """Modal for collecting giveaway creation parameters"""

    message_input = discord.ui.TextInput(
        label="Giveaway Message",
        placeholder="E.g., Win a $100 Amazon gift card!",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=500)

    winners_input = discord.ui.TextInput(label="Number of Winners",
                                         placeholder="E.g., 1, 2, 3, etc.",
                                         style=discord.TextStyle.short,
                                         required=True,
                                         default="1")

    end_time_input = discord.ui.TextInput(
        label="End of the giveaway:",
        placeholder="Format: YYYY-MM-DD HH:MM (e.g., 2025-11-15 18:30)",
        style=discord.TextStyle.short,
        required=True)

    def __init__(self, role):
        super().__init__()
        self.selected_role = role

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        try:
            message = self.message_input.value.strip()
            winners = int(self.winners_input.value.strip())
            end_time_str = self.end_time_input.value.strip()

            if winners <= 0:
                await interaction.followup.send(
                    "❌ **Invalid winner count!**\n\nNumber of winners must be greater than 0.",
                    ephemeral=True)
                return

            from datetime import datetime
            try:
                naive_end_time = datetime.strptime(end_time_str,
                                                   "%Y-%m-%d %H:%M")
                end_time = AMSTERDAM_TZ.localize(naive_end_time)
            except ValueError:
                await interaction.followup.send(
                    "❌ **Invalid date/time format!**\n\n" +
                    "Please use the format: **YYYY-MM-DD HH:MM**\n" +
                    "Example: `2025-11-15 18:30`",
                    ephemeral=True)
                return

            if end_time <= datetime.now(AMSTERDAM_TZ):
                await interaction.followup.send(
                    "❌ **End time must be in the future!**\n\n" +
                    f"Current time: {datetime.now(AMSTERDAM_TZ).strftime('%Y-%m-%d %H:%M')}",
                    ephemeral=True)
                return

            settings = {
                'message': message,
                'role': self.selected_role,
                'winners': winners,
                'end_time': end_time
            }

            await interaction.followup.send("🎉 Creating your giveaway...",
                                            ephemeral=True)
            await create_giveaway(interaction, settings)

        except ValueError as e:
            await interaction.followup.send(
                f"❌ **Invalid input!**\n\n" +
                f"Make sure winners is a valid number and date/time is in correct format.\n\n"
                + f"Error: {str(e)}",
                ephemeral=True)
        except Exception as e:
            await interaction.followup.send(
                f"❌ **Error creating giveaway:** {str(e)}", ephemeral=True)


class ChooseWinnerView(discord.ui.View):
    """View for User"""

    def __init__(self, active_giveaways):
        super().__init__(timeout=300)
        self.active_giveaways = active_giveaways
        self.selected_giveaway = None

        self.add_item(GiveawaySelectionDropdown(active_giveaways, "choose"))
        user_select = discord.ui.UserSelect(placeholder="👤 Select User")
        user_select.callback = self.user_selected
        user_select.disabled = True
        self.add_item(user_select)

    async def user_selected(self, interaction: discord.Interaction):
        if not self.selected_giveaway:
            await interaction.response.send_message(
                "❌ Please select a giveaway first!", ephemeral=True)
            return

        user = interaction.data['values'][0]
        user_id = int(user)
        member = interaction.guild.get_member(user_id)

        if not member:
            await interaction.response.send_message(
                "❌ User not found in this server!", ephemeral=True)
            return

        giveaway_id = self.selected_giveaway

        if 'chosen_winners' not in ACTIVE_GIVEAWAYS[giveaway_id]:
            ACTIVE_GIVEAWAYS[giveaway_id]['chosen_winners'] = []

        if user_id not in ACTIVE_GIVEAWAYS[giveaway_id]['chosen_winners']:
            current_chosen = len(
                ACTIVE_GIVEAWAYS[giveaway_id]['chosen_winners'])
            max_winners = ACTIVE_GIVEAWAYS[giveaway_id]['winner_count']

            if current_chosen >= max_winners:
                await interaction.response.send_message(
                    f"❌ Cannot add more guaranteed winners.\n" +
                    f"This giveaway already has {current_chosen} guaranteed winner(s) and the max is {max_winners}.",
                    ephemeral=True)
                return

            ACTIVE_GIVEAWAYS[giveaway_id]['chosen_winners'].append(user_id)

            # Update database with new chosen winner
            await bot.save_giveaway_to_db(giveaway_id,
                                          ACTIVE_GIVEAWAYS[giveaway_id])

            embed = discord.Embed(
                title="✅ User Selected",
                description=
                f"{member.mention} has been selected `{giveaway_id}`!\n\n" +
                f"**Selected Users:** {len(ACTIVE_GIVEAWAYS[giveaway_id]['chosen_winners'])}/{max_winners}",
                color=discord.Color.green())
            await interaction.response.send_message(embed=embed,
                                                    ephemeral=True)
        else:
            await interaction.response.send_message(
                f"❌ {member.mention} is already guaranteed as a winner for this giveaway.",
                ephemeral=True)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class EndGiveawayView(discord.ui.View):
    """View for ending a giveaway early"""

    def __init__(self, active_giveaways):
        super().__init__(timeout=300)
        self.active_giveaways = active_giveaways
        self.add_item(GiveawaySelectionDropdown(active_giveaways, "end"))

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class GiveawaySelectionDropdown(discord.ui.Select):
    """Dropdown to select a giveaway"""

    def __init__(self, active_giveaways, action_type):
        self.action_type = action_type

        options = []
        for gid, data in list(active_giveaways.items())[:25]:
            end_time = data['end_time']
            if isinstance(end_time, str):
                end_time = datetime.fromisoformat(end_time)
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=AMSTERDAM_TZ)

            time_left = end_time - datetime.now(AMSTERDAM_TZ)
            hours_left = int(time_left.total_seconds() // 3600)
            minutes_left = int((time_left.total_seconds() % 3600) // 60)

            short_id = gid.replace("giveaway_", "")[:8] + "..."

            options.append(
                discord.SelectOption(
                    label=
                    f"{data['winner_count']} winner(s) | {hours_left}h {minutes_left}m left",
                    description=f"ID: {short_id}",
                    value=gid))

        placeholder = "🎯 Select a giveaway..." if action_type == "choose" else "🏁 Select giveaway to end..."

        super().__init__(placeholder=placeholder,
                         min_values=1,
                         max_values=1,
                         options=options)

    async def callback(self, interaction: discord.Interaction):
        giveaway_id = self.values[0]

        if self.action_type == "choose":
            self.view.selected_giveaway = giveaway_id

            user_select = self.view.children[1]
            user_select.disabled = False

            embed = discord.Embed(
                title="🎯 User",
                description=
                f"**Selected giveaway:** `{giveaway_id}`\n\nNow select a User:",
                color=discord.Color.blue())
            await interaction.response.edit_message(embed=embed,
                                                    view=self.view)

        elif self.action_type == "end":
            await interaction.response.defer()

            embed = discord.Embed(
                title="🏁 Ending Giveaway",
                description=f"Ending giveaway `{giveaway_id}`...",
                color=discord.Color.orange())
            await interaction.followup.send(embed=embed, ephemeral=True)

            await end_giveaway(giveaway_id, interaction)


# Menu-Based Giveaway Command
@bot.tree.command(name="giveaway",
                  description="Manage giveaways with an interactive menu")
async def giveaway_command(interaction: discord.Interaction):
    """Open the giveaway management menu"""

    if not await owner_check(interaction):
        return

    try:
        view = GiveawayMenuView()
        embed = discord.Embed(
            title="🎉 Giveaway Management",
            description=
            "Select an action from the menu below to manage giveaways:",
            color=discord.Color.gold())
        embed.add_field(name="📝 Create Giveaway",
                        value="Set up a new giveaway with custom settings",
                        inline=False)
        embed.add_field(name="📋 List Active Giveaways",
                        value="View all currently running giveaways",
                        inline=False)
        embed.add_field(name="🎯 User", value="Select a User", inline=False)
        embed.add_field(name="🏁 End Giveaway",
                        value="End a giveaway early and select winners",
                        inline=False)

        await interaction.response.send_message(embed=embed,
                                                view=view,
                                                ephemeral=True)

    except Exception as e:
        await interaction.response.send_message(
            f"❌ Error opening giveaway menu: {str(e)}", ephemeral=True)


async def create_giveaway(interaction, settings):
    """Create and post the actual giveaway"""
    try:
        # Generate unique giveaway ID
        giveaway_id = f"giveaway_{int(datetime.now().timestamp())}"

        # Get end time from settings
        end_time = settings['end_time']

        # Create the giveaway embed
        embed = discord.Embed(title="🎉 **GIVEAWAY** 🎉",
                              description=settings['message'],
                              color=discord.Color.gold(),
                              timestamp=end_time)

        embed.add_field(name="⏰ End of the giveaway:",
                        value=f"<t:{int(end_time.timestamp())}:F>",
                        inline=True)

        embed.add_field(
            name="🏆 Winners",
            value=
            f"{settings['winners']} winner{'s' if settings['winners'] != 1 else ''}",
            inline=True)

        embed.add_field(name="🎪 How to Enter",
                        value="React with 🎉 to this message to enter!",
                        inline=False)

        embed.add_field(
            name="📋 Requirements",
            value=
            f"**The required rank to enter this giveaway is: {settings['role'].mention}**",
            inline=False)

        embed.set_footer(text="Ends at")

        # Get the giveaway channel
        giveaway_channel = bot.get_channel(GIVEAWAY_CHANNEL_ID)
        if not giveaway_channel:
            await interaction.followup.send(
                f"❌ Could not find giveaway channel with ID {GIVEAWAY_CHANNEL_ID}",
                ephemeral=True)
            return

        # Create the message content with @everyone at the bottom
        message_content = "@everyone"

        # Send the giveaway message
        message = await giveaway_channel.send(content=message_content,
                                              embed=embed)
        await message.add_reaction("🎉")

        # Store giveaway data
        ACTIVE_GIVEAWAYS[giveaway_id] = {
            'message_id': message.id,
            'channel_id': GIVEAWAY_CHANNEL_ID,
            'guild_id': giveaway_channel.guild.id,
            'creator_id': interaction.user.id,
            'required_role_id': settings['role'].id,
            'winner_count': settings['winners'],
            'end_time': end_time,
            'participants': [],
            'chosen_winners': [],
            'settings': settings
        }

        # Save giveaway to database for persistence across bot restarts
        await bot.save_giveaway_to_db(giveaway_id,
                                      ACTIVE_GIVEAWAYS[giveaway_id])

        # Log to giveaway debug channel
        await bot.log_giveaway_event(
            f"**🎉 NEW GIVEAWAY CREATED**\n"
            f"📝 **Giveaway ID:** `{giveaway_id}`\n"
            f"💬 **Message ID:** {message.id}\n"
            f"📄 **Message:** {settings['message'][:200]}{'...' if len(settings['message']) > 200 else ''}\n"
            f"🏆 **Winners:** {settings['winners']}\n"
            f"⏰ **End Time:** <t:{int(end_time.timestamp())}:F> (<t:{int(end_time.timestamp())}:R>)\n"
            f"🎭 **Required Role:** {settings['role'].mention} (ID: {settings['role'].id})\n"
            f"👤 **Created by:** {interaction.user.mention}\n"
            f"📍 **Channel:** {giveaway_channel.mention}")

        # Clear temp settings
        if hasattr(bot, '_temp_giveaway'):
            delattr(bot, '_temp_giveaway')

        # Schedule the giveaway to end
        asyncio.create_task(schedule_giveaway_end(giveaway_id))

        await interaction.followup.send(
            f"✅ **Giveaway created successfully!**\n" +
            f"🎯 **Giveaway ID:** `{giveaway_id}`\n" +
            f"📍 **Posted in:** {giveaway_channel.mention}\n" +
            f"⏰ **Ends:** <t:{int(end_time.timestamp())}:R>",
            ephemeral=True)

    except Exception as e:
        await interaction.followup.send(f"❌ Error creating giveaway: {str(e)}",
                                        ephemeral=True)


async def schedule_giveaway_end(giveaway_id):
    """Schedule a giveaway to end automatically"""
    try:
        giveaway_data = ACTIVE_GIVEAWAYS.get(giveaway_id)
        if not giveaway_data:
            return

        end_time = giveaway_data['end_time']
        now = datetime.now(AMSTERDAM_TZ)

        # Calculate sleep time
        sleep_seconds = (end_time - now).total_seconds()

        if sleep_seconds > 0:
            await asyncio.sleep(sleep_seconds)

        # End the giveaway if it's still active
        if giveaway_id in ACTIVE_GIVEAWAYS:
            await end_giveaway(giveaway_id)

    except Exception as e:
        print(f"Error scheduling giveaway end: {e}")


async def end_giveaway(giveaway_id, interaction=None):
    """End a giveaway and select winners"""
    try:
        if giveaway_id not in ACTIVE_GIVEAWAYS:
            if interaction:
                await interaction.followup.send("❌ Giveaway not found.",
                                                ephemeral=True)
            return

        giveaway_data = ACTIVE_GIVEAWAYS[giveaway_id]

        # Get the message
        channel = bot.get_channel(giveaway_data['channel_id'])
        if not channel:
            if interaction:
                await interaction.followup.send(
                    "❌ Could not find giveaway channel.", ephemeral=True)
            return

        try:
            message = await channel.fetch_message(giveaway_data['message_id'])
        except discord.NotFound:
            if interaction:
                await interaction.followup.send(
                    "❌ Giveaway message not found.", ephemeral=True)
            return

        # Get all participants who reacted with 🎉
        valid_participants = []
        required_role = channel.guild.get_role(
            giveaway_data['required_role_id'])

        for reaction in message.reactions:
            if str(reaction.emoji) == "🎉":
                async for user in reaction.users():
                    if user.bot:
                        continue

                    member = channel.guild.get_member(user.id)
                    if member and required_role and required_role in member.roles:
                        valid_participants.append(member)

        # Remove duplicates
        valid_participants = list(set(valid_participants))

        # Get chosen winners and random winners
        chosen_winners = giveaway_data.get('chosen_winners', [])
        winner_count = giveaway_data['winner_count']

        final_winners = []

        # Add guaranteed winners first
        for winner_id in chosen_winners:
            winner = channel.guild.get_member(winner_id)
            if winner and winner in valid_participants:
                final_winners.append(winner)
                valid_participants.remove(winner)  # Remove from random pool

        # Fill remaining winner slots with random selection
        remaining_slots = winner_count - len(final_winners)
        if remaining_slots > 0 and valid_participants:
            import random
            additional_winners = random.sample(
                valid_participants,
                min(remaining_slots, len(valid_participants)))
            final_winners.extend(additional_winners)

        # Create winner announcement
        embed = discord.Embed(title="🎉 **GIVEAWAY ENDED** 🎉",
                              color=discord.Color.green(),
                              timestamp=datetime.now(AMSTERDAM_TZ))

        if final_winners:
            winner_mentions = [winner.mention for winner in final_winners]
            embed.add_field(name="🏆 Winners",
                            value="\n".join(winner_mentions),
                            inline=False)

            embed.add_field(
                name="📊 Stats",
                value=
                f"Total Participants: {len(valid_participants) + len(final_winners)}\nWinners Selected: {len(final_winners)}",
                inline=False)
        else:
            embed.add_field(
                name="😔 No Winners",
                value=
                "No valid participants found or no one had the required role.",
                inline=False)

        embed.set_footer(text="Ended at")

        # Send winner announcement
        await channel.send(embed=embed)

        # Log to giveaway debug channel
        if final_winners:
            winner_mentions = [winner.mention for winner in final_winners]
            await bot.log_giveaway_event(
                f"**🏆 GIVEAWAY ENDED - WINNERS SELECTED**\n"
                f"📝 **Giveaway ID:** `{giveaway_id}`\n"
                f"💬 **Message ID:** {message.id}\n"
                f"🎉 **Winners:** {', '.join(winner_mentions)}\n"
                f"👥 **Total Valid Participants:** {len(valid_participants) + len(final_winners)}\n"
                f"🏅 **Winners Selected:** {len(final_winners)}\n"
                f"🎭 **Required Role:** {required_role.name if required_role else 'Unknown'} (ID: {giveaway_data['required_role_id']})\n"
                f"📍 **Channel:** {channel.mention}")
        else:
            await bot.log_giveaway_event(
                f"**😔 GIVEAWAY ENDED - NO WINNERS**\n"
                f"📝 **Giveaway ID:** `{giveaway_id}`\n"
                f"💬 **Message ID:** {message.id}\n"
                f"⚠️ **Reason:** No valid participants found or no one had the required role\n"
                f"🎭 **Required Role:** {required_role.name if required_role else 'Unknown'} (ID: {giveaway_data['required_role_id']})\n"
                f"📍 **Channel:** {channel.mention}")

        # Remove from active giveaways
        del ACTIVE_GIVEAWAYS[giveaway_id]

        # Remove from database
        await bot.remove_giveaway_from_db(giveaway_id)

        if interaction:
            await interaction.followup.send(
                f"✅ Giveaway `{giveaway_id}` ended successfully!\n" +
                f"🏆 Winners: {len(final_winners)}")

    except Exception as e:
        print(f"Error ending giveaway: {e}")
        if interaction:
            await interaction.followup.send(
                f"❌ Error ending giveaway: {str(e)}", ephemeral=True)


# Add the giveaway reaction handler
@bot.event
async def on_reaction_add(reaction, user):
    """Handle giveaway entry via reactions"""
    if user.bot:
        return

    # Check if this is a giveaway reaction
    if str(reaction.emoji) != "🎉":
        return

    # Find if this message is a giveaway
    giveaway_id = None
    for gid, data in ACTIVE_GIVEAWAYS.items():
        if data['message_id'] == reaction.message.id:
            giveaway_id = gid
            break

    if not giveaway_id:
        # This reaction is not for a tracked giveaway, ignore it
        return

    giveaway_data = ACTIVE_GIVEAWAYS[giveaway_id]

    # Check if user has required role
    required_role = reaction.message.guild.get_role(
        giveaway_data['required_role_id'])

    # Fetch member fresh from guild to avoid cached role data
    try:
        member = await reaction.message.guild.fetch_member(user.id)
    except (discord.NotFound, discord.HTTPException):
        member = reaction.message.guild.get_member(user.id)

    # Determine required level from role ID
    required_level = None
    for level, role_id in LEVEL_SYSTEM["level_roles"].items():
        if role_id == giveaway_data['required_role_id']:
            required_level = level
            break

    # Get user's actual level from LEVEL_SYSTEM
    user_id_str = str(user.id)
    user_data = LEVEL_SYSTEM["user_data"].get(user_id_str)
    
    if user_data:
        current_level = user_data.get("current_level", 0)
        message_count = user_data.get("message_count", 0)
    else:
        current_level = 0
        message_count = 0

    # Check if user meets level requirement
    level_check_passed = False
    if required_level is not None:
        level_check_passed = current_level >= required_level
    else:
        # If we can't determine required level, fall back to role check
        level_check_passed = member and required_role and required_role in member.roles

    # Log detailed information to giveaway debug channel
    user_roles_list = []
    if member:
        user_roles_list = [role.name for role in member.roles]

    level_check_result = "PASSED ✅" if level_check_passed else "FAILED ❌"

    # Log to giveaway debug channel
    await bot.log_giveaway_event(
        f"**Giveaway Entry Attempt**\n"
        f"📝 **Giveaway ID:** `{giveaway_id}`\n"
        f"👤 **User:** {user.name} ({user.mention})\n"
        f"🎭 **Required Role:** {required_role.name if required_role else 'NOT FOUND'} (ID: {giveaway_data['required_role_id']})\n"
        f"📊 **Required Level:** {required_level if required_level else 'Unknown'}\n"
        f"📈 **User Level:** {current_level} ({message_count} messages)\n"
        f"🏷️ **User Roles:** {', '.join(user_roles_list) if user_roles_list else 'None'}\n"
        f"✔️ **Level Check:** {level_check_result}")

    if not member or not level_check_passed:
        # Remove their reaction and send DM with detailed level information
        try:
            await reaction.remove(user)

            # Log rejection to giveaway debug channel
            await bot.log_giveaway_event(
                f"🚫 **Entry REJECTED** for {user.mention}\n"
                f"Giveaway ID: `{giveaway_id}`\n"
                f"Reason: Does not meet level requirement (Current: Level {current_level}, Required: Level {required_level if required_level else 'Unknown'})")

            # Determine current level name or "Level 0"
            if current_level > 0:
                current_level_text = f"Level {current_level}"
            else:
                current_level_text = "Level 0"

            # Calculate messages needed to reach required level
            if required_level and required_level in LEVEL_SYSTEM["level_requirements"]:
                required_messages = LEVEL_SYSTEM["level_requirements"][required_level]
                messages_remaining = required_messages - message_count

                # Make sure messages_remaining is not negative
                if messages_remaining < 0:
                    messages_remaining = 0

                level_progress_text = (
                    f"You are currently **{current_level_text}** with **{message_count}** messages. "
                    f"This giveaway requires **Level {required_level}** ({required_messages} messages). "
                    f"You need to send **{messages_remaining}** more chats in <#1384795868670201997> to reach the required level."
                )
            else:
                # Calculate next level progress
                next_level = current_level + 1
                if next_level in LEVEL_SYSTEM["level_requirements"]:
                    required_messages = LEVEL_SYSTEM["level_requirements"][next_level]
                    messages_remaining = required_messages - message_count
                    if messages_remaining < 0:
                        messages_remaining = 0
                    level_progress_text = (
                        f"You are currently **{current_level_text}**. "
                        f"You need to send **{messages_remaining}** more chats in <#1384795868670201997> to get to **Level {next_level}**."
                    )
                else:
                    level_progress_text = f"You are currently **{current_level_text}**."

            await user.send(
                "Unfortunately, your current activity level is not high enough to enter this giveaway. "
                "You can level up by participating in conversations in any of our text channels.\n\n"
                f"{level_progress_text}")
        except (discord.Forbidden, discord.NotFound):
            pass  # Can't DM user or remove reaction
        return

    # User has the required role - add them to participants if not already added
    if user.id not in giveaway_data['participants']:
        giveaway_data['participants'].append(user.id)
        await bot.save_giveaway_to_db(giveaway_id, giveaway_data)

        # Log successful entry to giveaway debug channel
        await bot.log_giveaway_event(
            f"✅ **Entry ACCEPTED** for {user.mention}\n"
            f"Giveaway ID: `{giveaway_id}`\n"
            f"Total Participants: {len(giveaway_data['participants'])}")


# Stats command removed as per user request


# ===== CHECK GIVEAWAY DATA COMMAND =====
@bot.tree.command(name="checkgiveaway",
                  description="Check the stored data for a giveaway")
@app_commands.describe(giveaway_id="The giveaway ID to check")
async def check_giveaway_command(interaction: discord.Interaction,
                                 giveaway_id: str):
    """Check what data is stored for a giveaway"""
    if not await owner_check(interaction):
        return

    try:
        if giveaway_id not in ACTIVE_GIVEAWAYS:
            await interaction.response.send_message(
                f"❌ Giveaway `{giveaway_id}` is not currently being tracked.",
                ephemeral=True)
            return

        giveaway_data = ACTIVE_GIVEAWAYS[giveaway_id]

        # Get the role name
        required_role_id = giveaway_data['required_role_id']
        guild = interaction.guild
        required_role = guild.get_role(required_role_id) if guild else None

        # Get participant details
        participant_count = len(giveaway_data.get('participants', []))
        participant_ids = giveaway_data.get('participants', [])

        embed = discord.Embed(title=f"🔍 Giveaway Data: {giveaway_id}",
                              color=discord.Color.blue())

        embed.add_field(name="Message ID",
                        value=str(giveaway_data['message_id']),
                        inline=False)
        embed.add_field(
            name="Required Role ID",
            value=
            f"{required_role_id} ({required_role.name if required_role else '⚠️ Role not found'})",
            inline=False)
        embed.add_field(name="Winner Count",
                        value=str(giveaway_data['winner_count']),
                        inline=True)
        embed.add_field(name="Participants",
                        value=str(participant_count),
                        inline=True)
        embed.add_field(
            name="End Time",
            value=f"<t:{int(giveaway_data['end_time'].timestamp())}:F>",
            inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)

        # Also log detailed information to giveaway debug channel
        participants_list = []
        if guild and participant_ids:
            for user_id in participant_ids[:10]:  # Show first 10
                member = guild.get_member(user_id)
                if member:
                    participants_list.append(
                        f"- {member.name} ({member.mention})")
                else:
                    participants_list.append(f"- Unknown User (ID: {user_id})")

            if len(participant_ids) > 10:
                participants_list.append(
                    f"... and {len(participant_ids) - 10} more")

        participants_text = "\n".join(
            participants_list) if participants_list else "No participants yet"

        await bot.log_giveaway_event(
            f"**🔍 Giveaway Check Command Used**\n"
            f"📝 **Giveaway ID:** `{giveaway_id}`\n"
            f"💬 **Message ID:** {giveaway_data['message_id']}\n"
            f"🎭 **Required Role:** {required_role.name if required_role else 'NOT FOUND'} (ID: {required_role_id})\n"
            f"🏆 **Winner Count:** {giveaway_data['winner_count']}\n"
            f"👥 **Total Participants:** {participant_count}\n"
            f"⏰ **End Time:** <t:{int(giveaway_data['end_time'].timestamp())}:F>\n"
            f"👤 **Checked by:** {interaction.user.mention}\n\n"
            f"**Participants:**\n{participants_text}")

    except Exception as e:
        await interaction.response.send_message(
            f"❌ Error checking giveaway: {str(e)}", ephemeral=True)
        print(f"Error checking giveaway: {e}")


# ===== RESTORE GIVEAWAY COMMAND =====
@bot.tree.command(name="restoregiveaway",
                  description="Restore a giveaway by message ID")
@app_commands.describe(
    message_id="The message ID of the giveaway",
    giveaway_id="The giveaway ID (e.g., giveaway_1762902912)")
async def restore_giveaway_command(interaction: discord.Interaction,
                                   message_id: str, giveaway_id: str):
    """Restore a giveaway from a message ID"""
    if not await owner_check(interaction):
        return

    try:
        await interaction.response.defer(ephemeral=True)

        # Get the giveaway channel
        giveaway_channel = bot.get_channel(GIVEAWAY_CHANNEL_ID)
        if not giveaway_channel:
            await interaction.followup.send(
                f"❌ Could not find giveaway channel with ID {GIVEAWAY_CHANNEL_ID}",
                ephemeral=True)
            return

        # Fetch the message
        try:
            message = await giveaway_channel.fetch_message(int(message_id))
        except (discord.NotFound, ValueError):
            await interaction.followup.send(
                f"❌ Could not find message with ID {message_id}",
                ephemeral=True)
            return

        # Parse the embed to get giveaway details
        if not message.embeds or len(message.embeds) == 0:
            await interaction.followup.send(
                "❌ Message does not contain an embed. Not a valid giveaway message.",
                ephemeral=True)
            return

        embed = message.embeds[0]

        # Extract information from embed
        message_text = embed.description or ""

        # Find required role from embed fields
        required_role_id = None
        winner_count = 1
        end_timestamp = None

        for field in embed.fields:
            if "Requirements" in field.name and "required rank" in field.value.lower(
            ):
                # Extract role ID from mention (format: <@&role_id>)
                role_match = re.search(r'<@&(\d+)>', field.value)
                if role_match:
                    required_role_id = int(role_match.group(1))
            elif "Winners" in field.name:
                # Extract winner count
                winner_match = re.search(r'(\d+)\s+winner', field.value)
                if winner_match:
                    winner_count = int(winner_match.group(1))

        # Get end time from embed timestamp
        if embed.timestamp:
            end_time = embed.timestamp
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=AMSTERDAM_TZ)
        else:
            await interaction.followup.send(
                "❌ Could not find end time in giveaway embed.", ephemeral=True)
            return

        if not required_role_id:
            await interaction.followup.send(
                "❌ Could not find required role in giveaway embed.",
                ephemeral=True)
            return

        # Create giveaway data structure
        giveaway_data = {
            'message_id': message.id,
            'channel_id': giveaway_channel.id,
            'guild_id': giveaway_channel.guild.id,
            'creator_id': interaction.user.id,
            'required_role_id': required_role_id,
            'winner_count': winner_count,
            'end_time': end_time,
            'participants': [],
            'chosen_winners': [],
            'settings': {
                'message': message_text,
                'winners': winner_count
            }
        }

        # Store in memory and database
        ACTIVE_GIVEAWAYS[giveaway_id] = giveaway_data
        await bot.save_giveaway_to_db(giveaway_id, giveaway_data)

        # Schedule the giveaway to end
        asyncio.create_task(schedule_giveaway_end(giveaway_id))

        # Log the restoration
        await bot.log_to_discord(
            f"**Giveaway Restored** 🔄\n"
            f"ID: `{giveaway_id}`\n"
            f"Message ID: {message_id}\n"
            f"End time: <t:{int(end_time.timestamp())}:F>\n"
            f"Winners: {winner_count}\n"
            f"Restored by: {interaction.user.mention}")

        await interaction.followup.send(
            f"✅ **Giveaway restored successfully!**\n" +
            f"🎯 **Giveaway ID:** `{giveaway_id}`\n" +
            f"📍 **Channel:** {giveaway_channel.mention}\n" +
            f"⏰ **Ends:** <t:{int(end_time.timestamp())}:R>\n" +
            f"🏆 **Winners:** {winner_count}",
            ephemeral=True)

    except Exception as e:
        await interaction.followup.send(
            f"❌ Error restoring giveaway: {str(e)}", ephemeral=True)
        print(f"Error restoring giveaway: {e}")


# ===== DM TRACKING COMMAND =====
@bot.tree.command(
    name="dmstatus",
    description=
    "Check which users have received 3, 7, or 14 day follow-up messages")
@app_commands.describe(
    message_type="Which message type to check: 3day, 7day, 14day, or all")
async def dm_status_command(interaction: discord.Interaction,
                            message_type: str = "all"):
    """Check DM status for timed auto-role follow-up messages"""
    if not await owner_check(interaction):
        return

    try:
        await interaction.response.defer(ephemeral=True)

        if not AUTO_ROLE_CONFIG["dm_schedule"]:
            await interaction.followup.send("📬 No DM schedule data found.",
                                            ephemeral=True)
            return

        # Filter message types
        valid_types = ["all", "3day", "7day", "14day"]
        if message_type.lower() not in valid_types:
            await interaction.followup.send(
                f"❌ Invalid message type. Use: {', '.join(valid_types)}",
                ephemeral=True)
            return

        # Clean up completed users first (those who received 14-day message)
        completed_users = []
        for member_id, dm_data in list(
                AUTO_ROLE_CONFIG["dm_schedule"].items()):
            if dm_data.get("dm_14_sent", False):
                completed_users.append(member_id)

        # Remove completed users from tracking
        for member_id in completed_users:
            del AUTO_ROLE_CONFIG["dm_schedule"][member_id]

        # Save updated config if users were removed
        if completed_users:
            await bot.save_auto_role_config()
            print(
                f"✅ Removed {len(completed_users)} completed users from DM tracking"
            )

        # Create status report
        status_report = "📬 **DM STATUS REPORT**\n━━━━━━━━━━━━━━━━━━━━━━\n\n"

        if completed_users:
            status_report += f"🧹 **Cleanup**: Removed {len(completed_users)} users who completed 14-day sequence\n\n"

        total_scheduled = len(AUTO_ROLE_CONFIG["dm_schedule"])
        sent_3day = sent_7day = sent_14day = 0
        pending_users = []

        for member_id, dm_data in AUTO_ROLE_CONFIG["dm_schedule"].items():
            try:
                # Get member info
                guild = interaction.guild
                member = guild.get_member(int(member_id)) if guild else None
                member_name = member.display_name if member else f"User-{member_id}"

                # Check DM status
                dm_3_sent = dm_data.get("dm_3_sent", False)
                dm_7_sent = dm_data.get("dm_7_sent", False)
                dm_14_sent = dm_data.get("dm_14_sent", False)

                if dm_3_sent: sent_3day += 1
                if dm_7_sent: sent_7day += 1
                if dm_14_sent: sent_14day += 1

                # Filter by message type
                if message_type.lower() == "3day" and dm_3_sent:
                    status_report += f"✅ **3-Day**: {member_name}\n"
                elif message_type.lower() == "7day" and dm_7_sent:
                    status_report += f"✅ **7-Day**: {member_name}\n"
                elif message_type.lower() == "14day" and dm_14_sent:
                    status_report += f"✅ **14-Day**: {member_name}\n"
                elif message_type.lower() == "all":
                    dm_status = []
                    if dm_3_sent: dm_status.append("3✅")
                    if dm_7_sent: dm_status.append("7✅")
                    if dm_14_sent: dm_status.append("14✅")
                    if not dm_status: dm_status.append("⏳")

                    status_report += f"• **{member_name}**: {' '.join(dm_status)}\n"

                # Track pending users (haven't received 14-day message yet)
                if not dm_14_sent:
                    pending_users.append(member_name)

            except Exception as e:
                status_report += f"❌ Error processing member {member_id}: {str(e)}\n"

        # Add summary
        status_report += f"\n**📊 SUMMARY**\n"
        status_report += f"• Total scheduled: **{total_scheduled}**\n"
        status_report += f"• 3-day messages sent: **{sent_3day}**\n"
        status_report += f"• 7-day messages sent: **{sent_7day}**\n"
        status_report += f"• 14-day messages sent: **{sent_14day}**\n"
        status_report += f"• Still pending: **{len(pending_users)}**\n"

        # Split message if too long
        if len(status_report) > 2000:
            # Send summary first
            summary = f"📬 **DM STATUS SUMMARY**\n━━━━━━━━━━━━━━━━━━━━━━\n"
            summary += f"• Total scheduled: **{total_scheduled}**\n"
            summary += f"• 3-day messages sent: **{sent_3day}**\n"
            summary += f"• 7-day messages sent: **{sent_7day}**\n"
            summary += f"• 14-day messages sent: **{sent_14day}**\n"
            summary += f"• Still pending: **{len(pending_users)}**\n\n"
            summary += f"*Use `/dmstatus 3day`, `/dmstatus 7day`, or `/dmstatus 14day` for detailed lists.*"

            await interaction.followup.send(summary, ephemeral=True)
        else:
            await interaction.followup.send(status_report, ephemeral=True)

    except Exception as e:
        await interaction.followup.send(
            f"❌ Error checking DM status: {str(e)}", ephemeral=True)


# ===== PRICE TRACKING COMMANDS =====
# Price tracking is now permanently enabled - no toggle command needed


# ===== ACTIVE TRADES PAGINATION VIEW =====
class ActiveTradesView(discord.ui.View):

    def __init__(self, active_trades_data, total_pages, current_page=1):
        super().__init__(timeout=300)  # 5 minutes timeout
        self.active_trades_data = active_trades_data
        self.total_pages = total_pages
        self.current_page = current_page
        self.trades_per_page = 3

        # Update button states
        self.update_buttons()

    def update_buttons(self):
        # Clear existing buttons
        self.clear_items()

        # Previous page button
        if self.current_page > 1:
            prev_button = discord.ui.Button(
                label="◀ Previous",
                style=discord.ButtonStyle.secondary,
                custom_id="prev_page")
            prev_button.callback = self.previous_page
            self.add_item(prev_button)

        # Page number buttons (show current and adjacent pages)
        start_page = max(1, self.current_page - 1)
        end_page = min(self.total_pages, self.current_page + 1)

        for page_num in range(start_page, end_page + 1):
            style = discord.ButtonStyle.primary if page_num == self.current_page else discord.ButtonStyle.secondary
            page_button = discord.ui.Button(label=str(page_num),
                                            style=style,
                                            custom_id=f"page_{page_num}")
            page_button.callback = lambda interaction, p=page_num: self.go_to_page(
                interaction, p)
            self.add_item(page_button)

        # Next page button
        if self.current_page < self.total_pages:
            next_button = discord.ui.Button(
                label="Next ▶",
                style=discord.ButtonStyle.secondary,
                custom_id="next_page")
            next_button.callback = self.next_page
            self.add_item(next_button)

    async def previous_page(self, interaction: discord.Interaction):
        if self.current_page > 1:
            self.current_page -= 1
            await self.update_message(interaction)

    async def next_page(self, interaction: discord.Interaction):
        if self.current_page < self.total_pages:
            self.current_page += 1
            await self.update_message(interaction)

    async def go_to_page(self, interaction: discord.Interaction,
                         page_num: int):
        self.current_page = page_num
        await self.update_message(interaction)

    async def update_message(self, interaction: discord.Interaction):
        # Refresh data from database to ensure we have the latest trade statuses
        fresh_trades = await bot.get_active_trades_from_db()
        self.active_trades_data = fresh_trades

        # Recalculate pagination in case trades were added/removed
        if self.active_trades_data:
            self.total_pages = math.ceil(
                len(self.active_trades_data) / self.trades_per_page)
        else:
            self.total_pages = 1

        # Ensure current page is valid after data refresh
        if self.current_page > self.total_pages:
            self.current_page = self.total_pages

        embed = await self.create_embed()
        self.update_buttons()
        await interaction.response.edit_message(embed=embed, view=self)

    async def create_embed(self):
        # Handle empty trades case
        if not self.active_trades_data:
            return discord.Embed(
                title="📊 Active Signal Tracking",
                description=
                "❌ **No active trades found**\n\nThere are currently no trading signals being tracked.",
                color=discord.Color.red())

        # Calculate start and end indices for current page
        start_idx = (self.current_page - 1) * self.trades_per_page
        end_idx = min(start_idx + self.trades_per_page,
                      len(self.active_trades_data))

        # Get trades for current page
        trades_list = list(self.active_trades_data.items())
        current_page_trades = trades_list[start_idx:end_idx]

        embed = discord.Embed(
            title="📊 Active Signal Tracking",
            description=
            f"Monitoring **{len(self.active_trades_data)}** trading signals with live price analysis:\n📄 **Page {self.current_page} of {self.total_pages}** (showing {len(current_page_trades)} signals)",
            color=discord.Color.green())

        # Process each trade on current page and get current price status
        for i, (message_id, trade_data) in enumerate(current_page_trades):
            try:
                # Get current live price - try assigned API first, then fallback to others
                assigned_api = trade_data.get("assigned_api", "currencybeacon")
                current_price = await bot.get_live_price(
                    trade_data["pair"], specific_api=assigned_api)

                # If assigned API fails, try fallback rotation
                if current_price is None:
                    current_price = await bot.get_live_price(
                        trade_data["pair"], use_all_apis=False)

                if current_price:
                    # Analyze current position
                    status_info = await analyze_trade_position(
                        trade_data, current_price)
                    price_status = f"**Current: ${current_price:.5f}** {status_info['emoji']}\n"
                    position_text = f"*{status_info['position']}*"
                else:
                    price_status = "**Current: Price unavailable**\n"
                    position_text = "*Unable to determine position*"

                # Build level display
                levels_display = build_levels_display(trade_data,
                                                      current_price)

                # Enhanced TP hits status with visual indicators
                tp_hits = trade_data.get('tp_hits', [])
                status = trade_data.get('status', 'active')
                manual_overrides = trade_data.get('manual_overrides', [])

                # Create visual TP status with emojis
                if tp_hits:
                    tp_status = f"**TP Hits:** {', '.join([f'🟢 {tp.upper()}' for tp in tp_hits])}"
                else:
                    tp_status = "**TP Hits:** None"

                # Enhanced status indicators
                status_indicators = []

                # Breakeven status
                if trade_data.get("breakeven_active"):
                    status_indicators.append(
                        "🔄 **Breakeven SL Active** (TP2 hit)")

                # Trade status indicator
                if 'closed' in status.lower() or 'completed' in status.lower():
                    if 'sl hit' in status.lower():
                        status_indicators.append("🔴 **Trade Closed** (SL Hit)")
                    elif 'tp3 hit' in status.lower():
                        status_indicators.append(
                            "🚀 **Trade Completed** (TP3 Hit)")
                    elif 'breakeven' in status.lower():
                        status_indicators.append(
                            "🟡 **Trade Closed** (Breakeven After TP2)")
                    else:
                        status_indicators.append("⚪ **Trade Closed**")
                elif 'active' in status.lower():
                    status_indicators.append("🟢 **Trade Active**")

                # Manual override indicator
                if manual_overrides:
                    status_indicators.append(
                        f"✋ **Manual Overrides:** {', '.join([override.upper() for override in manual_overrides])}"
                    )

                # Combine status indicators
                combined_status = "\n".join(
                    status_indicators) if status_indicators else ""

                # Time tracking
                time_status = f"\n⏱️ Message: {message_id[:8]}..."

                # Build the field value properly
                field_value = f"{price_status}{position_text}\n\n{levels_display}\n{tp_status}"
                if combined_status:
                    field_value += f"\n{combined_status}"
                field_value += time_status

                embed.add_field(
                    name=f"📈 {trade_data['pair']} - {trade_data['action']}",
                    value=field_value,
                    inline=False)

            except Exception as e:
                # Fallback display if price retrieval fails
                embed.add_field(
                    name=f"⚠️ {trade_data['pair']} - {trade_data['action']}",
                    value=
                    f"**Error getting price data**\nEntry: ${trade_data['entry']}\nStatus: {trade_data.get('status', 'active')}",
                    inline=False)

        # Add pagination instructions in footer
        embed.set_footer(
            text=
            f"Use the buttons below to navigate pages • Page {self.current_page}/{self.total_pages} • Signals auto-removed when SL/TP3 hit"
        )
        return embed

    async def on_timeout(self):
        # Disable all buttons when view times out
        for item in self.children:
            item.disabled = True


# ===== ACTIVE TRADES COMMAND GROUP =====
active_trades_group = app_commands.Group(
    name="activetrades",
    description="[OWNER ONLY] Manage tracked trading signals")


@active_trades_group.command(
    name="view",
    description="View detailed status of all tracked trading signals")
async def active_trades_view(interaction: discord.Interaction):
    """Show active trades with detailed price level analysis"""
    if not await owner_check(interaction):
        return

    # Respond immediately to prevent timeout
    await interaction.response.send_message("🔄 Loading active trades...",
                                            ephemeral=True)

    if not PRICE_TRACKING_CONFIG["enabled"]:
        embed = discord.Embed(
            title="📊 Active Signal Tracking",
            description="❌ Price tracking system is currently disabled",
            color=discord.Color.red())
        await interaction.edit_original_response(content="", embed=embed)
        return

    # Get active trades from database for 24/7 persistence
    active_trades = await bot.get_active_trades_from_db()

    # Check for deleted messages and remove them immediately (don't wait for 5min interval)
    trades_to_remove = []
    if active_trades:
        for message_id, trade_data in list(active_trades.items()):
            try:
                # Check if the original message still exists
                message_deleted = await bot.check_message_deleted(
                    message_id, trade_data.get("channel_id"))
                if message_deleted:
                    print(
                        f"📝 [/activetrades] Original message deleted for {trade_data['pair']} - removing from tracking"
                    )
                    trades_to_remove.append(message_id)
            except Exception as e:
                print(f"Error checking message {message_id}: {e}")
                trades_to_remove.append(message_id)

        # Remove deleted trades from database immediately
        for message_id in trades_to_remove:
            await bot.remove_trade_from_db(message_id)
            if message_id in active_trades:
                del active_trades[message_id]

    # Send debugging to Discord channel
    debug_channel = bot.get_channel(1414220633029611582)
    if debug_channel:
        await debug_channel.send(
            f"🔍 DEBUG (/activetrades view): Found {len(active_trades)} active trades (after deletion check)"
        )
        if trades_to_remove:
            await debug_channel.send(
                f"🗑️ DEBUG (/activetrades view): Removed {len(trades_to_remove)} deleted signals"
            )
        await debug_channel.send(
            f"🔍 DEBUG (/activetrades view): Active trades keys: {list(active_trades.keys())}"
        )

    if not active_trades:
        embed = discord.Embed(
            title="📊 Active Signal Tracking",
            description=
            "✅ No trading signals being monitored\n\n*Signals are automatically removed when they hit SL or TP3*",
            color=discord.Color.blue())
        await interaction.edit_original_response(content="", embed=embed)
        return

    # Pagination setup - 3 trades per page to avoid Discord character limits
    page = 1  # Always start with page 1
    trades_per_page = 3
    total_trades = len(active_trades)
    total_pages = (total_trades + trades_per_page -
                   1) // trades_per_page  # Ceiling division

    # Calculate start and end indices for current page
    start_idx = (page - 1) * trades_per_page
    end_idx = min(start_idx + trades_per_page, total_trades)

    # Get trades for current page
    trades_list = list(active_trades.items())
    current_page_trades = trades_list[start_idx:end_idx]

    # Get time until next refresh
    next_refresh = await bot.get_time_until_next_refresh()

    embed = discord.Embed(
        title="📊 Active Signal Tracking",
        description=
        f"Monitoring **{total_trades}** trading signals with live price analysis:\n⏰ **Next API refresh in:** {next_refresh}\n📄 **Page {page} of {total_pages}** (showing {len(current_page_trades)} signals)",
        color=discord.Color.green())

    # Process each trade on current page and get current price status
    for i, (message_id, trade_data) in enumerate(current_page_trades):
        try:
            # Get current live price - try assigned API first, then fallback to others
            assigned_api = trade_data.get("assigned_api", "currencybeacon")
            current_price = await bot.get_live_price(trade_data["pair"],
                                                     specific_api=assigned_api)

            # If assigned API fails, try fallback rotation
            if current_price is None:
                current_price = await bot.get_live_price(trade_data["pair"],
                                                         use_all_apis=False)

            if current_price:
                # Analyze current position
                status_info = await analyze_trade_position(
                    trade_data, current_price)
                price_status = f"**Current: ${current_price:.5f}** {status_info['emoji']}\n"
                position_text = f"*{status_info['position']}*"
            else:
                price_status = "**Current: Price unavailable**\n"
                position_text = "*Unable to determine position*"

            # Build level display
            levels_display = build_levels_display(trade_data, current_price)

            # TP hits status
            tp_hits = trade_data.get('tp_hits', [])
            tp_status = f"**TP Hits:** {', '.join([tp.upper() for tp in tp_hits]) if tp_hits else 'None'}"

            # Breakeven status
            breakeven_status = ""
            if trade_data.get("breakeven_active"):
                breakeven_status = "\n🔄 **Breakeven SL Active** (TP2 hit)"

            # Time tracking
            time_status = f"\n⏱️ Message: {message_id[:8]}..."

            embed.add_field(
                name=f"📈 {trade_data['pair']} - {trade_data['action']}",
                value=
                f"{price_status}{position_text}\n\n{levels_display}\n{tp_status}{breakeven_status}{time_status}",
                inline=False)

        except Exception as e:
            # Fallback display if price retrieval fails
            embed.add_field(
                name=f"⚠️ {trade_data['pair']} - {trade_data['action']}",
                value=
                f"**Error getting price data**\nEntry: ${trade_data['entry']}\nStatus: {trade_data.get('status', 'active')}",
                inline=False)

    # Create interactive view with buttons if multiple pages
    if total_pages > 1:
        view = ActiveTradesView(active_trades, total_pages, page)
        embed.set_footer(
            text=
            f"Use the buttons below to navigate pages • Page {page}/{total_pages} • Signals auto-removed when SL/TP3 hit"
        )
        await interaction.edit_original_response(content="",
                                                 embed=embed,
                                                 view=view)
    else:
        embed.set_footer(
            text=
            "Signals automatically removed when SL/TP3 hit or original message deleted"
        )
        await interaction.edit_original_response(content="", embed=embed)


# Manual remove command removed - automatic cleanup handles deleted messages

# Register the command group
bot.tree.add_command(active_trades_group)


async def analyze_trade_position(trade_data: dict,
                                 current_price: float) -> dict:
    """Analyze where the current price stands relative to trade levels"""
    action = trade_data["action"]
    entry = trade_data["entry"]
    tp1 = trade_data["tp1"]
    tp2 = trade_data["tp2"]
    tp3 = trade_data["tp3"]
    sl = trade_data["sl"]

    if action == "BUY":
        if current_price <= sl:
            return {"emoji": "🔴", "position": "At/Below SL - Stop Loss Level"}
        elif current_price <= entry:
            return {
                "emoji": "🟡",
                "position": "Below Entry - Potential Loss Zone"
            }
        elif current_price <= tp1:
            return {"emoji": "🟠", "position": "Between Entry and TP1"}
        elif current_price <= tp2:
            return {
                "emoji": "🟢",
                "position": "Between TP1 and TP2 - In Profit"
            }
        elif current_price <= tp3:
            return {
                "emoji": "💚",
                "position": "Between TP2 and TP3 - Strong Profit"
            }
        else:
            return {
                "emoji": "🚀",
                "position": "Above TP3 - Maximum Profit Zone"
            }

    else:  # SELL
        if current_price >= sl:
            return {"emoji": "🔴", "position": "At/Above SL - Stop Loss Level"}
        elif current_price >= entry:
            return {
                "emoji": "🟡",
                "position": "Above Entry - Potential Loss Zone"
            }
        elif current_price >= tp1:
            return {"emoji": "🟠", "position": "Between Entry and TP1"}
        elif current_price >= tp2:
            return {
                "emoji": "🟢",
                "position": "Between TP1 and TP2 - In Profit"
            }
        elif current_price >= tp3:
            return {
                "emoji": "💚",
                "position": "Between TP2 and TP3 - Strong Profit"
            }
        else:
            return {
                "emoji": "🚀",
                "position": "Below TP3 - Maximum Profit Zone"
            }


def build_levels_display(trade_data: dict, current_price: float = None) -> str:
    """Build a visual display of all price levels"""
    action = trade_data["action"]
    entry = trade_data["entry"]
    tp1 = trade_data["tp1"]
    tp2 = trade_data["tp2"]
    tp3 = trade_data["tp3"]
    sl = trade_data["sl"]
    tp_hits = trade_data.get('tp_hits', [])
    status = trade_data.get('status', 'active')

    # Check if SL was hit based on status or manual overrides
    manual_overrides = trade_data.get('manual_overrides', [])
    sl_hit = 'sl hit' in status.lower() or 'sl' in manual_overrides

    # Create level indicators with proper visual feedback
    def get_level_indicator(level_name, price, hit=False, is_sl=False):
        if hit:
            if is_sl:
                hit_marker = "🔴"  # Red circle for SL hit
            else:
                hit_marker = "✅"  # Green checkmark for TP hit
        else:
            hit_marker = "⭕"  # Empty circle for not hit
        return f"{hit_marker} **{level_name}:** ${price:.5f}"

    if action == "BUY":
        # For BUY orders: Display in order from lowest to highest price: SL < Entry < TP1 < TP2 < TP3
        levels = [
            get_level_indicator("SL", sl, sl_hit, is_sl=True),
            get_level_indicator("Entry", entry),
            get_level_indicator("TP1", tp1, "tp1" in tp_hits),
            get_level_indicator("TP2", tp2, "tp2" in tp_hits),
            get_level_indicator("TP3", tp3, "tp3" in tp_hits)
        ]
    else:
        # For SELL orders: Display in order from highest to lowest price: SL > Entry > TP1 > TP2 > TP3
        levels = [
            get_level_indicator("SL", sl, sl_hit, is_sl=True),
            get_level_indicator("Entry", entry),
            get_level_indicator("TP1", tp1, "tp1" in tp_hits),
            get_level_indicator("TP2", tp2, "tp2" in tp_hits),
            get_level_indicator("TP3", tp3, "tp3" in tp_hits)
        ]

    return "\n".join(levels)


@bot.tree.command(
    name="pricetest",
    description="[OWNER ONLY] Test live price retrieval for a trading pair")
@app_commands.describe(pair="Trading pair to test")
@app_commands.autocomplete(pair=pair_autocomplete)
async def test_price_retrieval(interaction: discord.Interaction, pair: str):
    """Test price retrieval for a trading pair"""
    if not await owner_check(interaction):
        return

    if not PRICE_TRACKING_CONFIG["enabled"]:
        await interaction.response.send_message(
            "❌ Price tracking system is disabled", ephemeral=True)
        return

    await interaction.response.defer()

    try:
        # Normalize pair format using centralized cleaning
        pair_clean = bot.clean_pair_name(pair)

        # Get prices from all APIs
        api_results = await bot.get_all_api_prices(pair_clean)

        # Get time until next refresh
        next_refresh = await bot.get_time_until_next_refresh()

        embed = discord.Embed(
            title="💰 Price Test",
            description=f"⏰ **Next API refresh in:** {next_refresh}",
            color=discord.Color.green())

        # Count successful prices
        successful_prices = [
            result for result in api_results.values()
            if result["price"] is not None
        ]

        if successful_prices:
            # Show overall status
            embed.add_field(
                name=f"✅ {pair.upper()}",
                value=
                f"Found **{len(successful_prices)}** valid prices from **{len(api_results)}** APIs",
                inline=False)
        else:
            embed.add_field(name=f"❌ {pair.upper()}",
                            value="Could not retrieve price from any API",
                            inline=False)

        # Show individual API prices
        api_status = []
        for api_name, result in api_results.items():
            # Convert API name to display format
            display_name = api_name.replace('_', ' ').title()
            if api_name == "fmp":
                display_name = "Fmp"
            elif api_name == "fxapi":
                display_name = "Fxapi"

            if result["price"] is not None:
                api_status.append(
                    f"✅ {display_name}: **{result['price']:.5f}**")
            elif result["status"] == "no_key":
                api_status.append(f"❌ {display_name}: No Key")
            elif result["status"] == "no_data":
                api_status.append(f"⚠️ {display_name}: No Data")
            else:
                api_status.append(f"❌ {display_name}: Error")

        embed.add_field(name="🔑 API Prices",
                        value="\n".join(api_status),
                        inline=False)

        await interaction.followup.send(embed=embed)

    except Exception as e:
        embed = discord.Embed(
            title="💰 Price Test",
            description=f"❌ Error testing price retrieval: {str(e)}",
            color=discord.Color.red())
        await interaction.followup.send(embed=embed)


class TradeOverrideView(discord.ui.View):
    """Interactive view for manually overriding trade statuses"""

    def __init__(self, active_trades):
        super().__init__(timeout=300)  # 5 minute timeout
        self.active_trades = active_trades
        self.selected_trades = []

        # Add trade selection dropdown
        self.add_item(TradeSelectionDropdown(active_trades))
        # Add status selection dropdown (initially disabled)
        status_dropdown = StatusSelectionDropdown()
        status_dropdown.disabled = True
        self.add_item(status_dropdown)

    async def on_timeout(self):
        # Disable all items when view times out
        for item in self.children:
            item.disabled = True


class TradeSelectionDropdown(discord.ui.Select):
    """Dropdown to select which trades to override (supports multi-select)"""

    def __init__(self, active_trades):
        # Create options for each active trade
        options = []
        for message_id, trade_data in list(
                active_trades.items())[:25]:  # Discord limit of 25 options
            pair = trade_data["pair"]
            action = trade_data["action"]
            tp_hits = trade_data.get("tp_hits", [])
            tp_status = f" (TP hits: {', '.join([tp.upper() for tp in tp_hits])})" if tp_hits else ""

            # Truncate message_id for display
            short_id = message_id[:8] + "..."

            options.append(
                discord.SelectOption(label=f"{pair} - {action}{tp_status}",
                                     description=f"Message ID: {short_id}",
                                     value=message_id))

        super().__init__(
            placeholder="🎯 Select trade(s) to modify (up to 5)...",
            min_values=1,
            max_values=min(5, len(options)),
            options=options)

    async def callback(self, interaction: discord.Interaction):
        # Store the selected trades (can be multiple)
        self.view.selected_trades = self.values

        # Enable the status dropdown
        status_dropdown = self.view.children[1]
        status_dropdown.disabled = False

        # Update the message with selected trades info
        selected_count = len(self.values)

        if selected_count == 1:
            trade_data = self.view.active_trades[self.values[0]]
            pair = trade_data["pair"]
            action = trade_data["action"]
            description = f"**Selected Trade:** {pair} - {action}\n\nNow select the status to apply:"
        else:
            trade_list = []
            for msg_id in self.values:
                trade_data = self.view.active_trades[msg_id]
                pair = trade_data["pair"]
                action = trade_data["action"]
                trade_list.append(f"• **{pair} - {action}**")

            description = f"**Selected {selected_count} Trades:**\n" + "\n".join(
                trade_list) + "\n\nNow select the status to apply to all:"

        embed = discord.Embed(title="🔧 Trade Override System",
                              description=description,
                              color=discord.Color.orange())

        await interaction.response.edit_message(embed=embed, view=self.view)


class StatusSelectionDropdown(discord.ui.Select):
    """Dropdown to select the status to apply"""

    def __init__(self):
        options = [
            discord.SelectOption(
                label="SL Hit",
                description="Mark trade as stopped out (ends trade)",
                value="sl_hit",
                emoji="🔴"),
            discord.SelectOption(label="TP1 Hit",
                                 description="Mark TP1 as reached",
                                 value="tp1_hit",
                                 emoji="🟢"),
            discord.SelectOption(
                label="TP2 Hit",
                description="Mark TP2 as reached (activates breakeven)",
                value="tp2_hit",
                emoji="🟢"),
            discord.SelectOption(
                label="TP3 Hit",
                description="Mark TP3 as reached (ends trade)",
                value="tp3_hit",
                emoji="🚀"),
            discord.SelectOption(
                label="Breakeven Hit After TP2",
                description=
                "Mark trade as returned to breakeven after TP2 (ends trade)",
                value="breakeven_after_tp2",
                emoji="🟡"),
            discord.SelectOption(
                label="End Tracking",
                description=
                "Stop tracking this trade without sending notifications",
                value="end_tracking",
                emoji="⏹️")
        ]

        super().__init__(placeholder="📊 Select status to apply...",
                         min_values=1,
                         max_values=1,
                         options=options)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        try:
            import asyncio
            selected_trades = self.view.selected_trades
            status = self.values[0]

            # Validate selections
            if not selected_trades:
                embed = discord.Embed(
                    title="❌ No Trades Selected",
                    description="Please select at least one trade to modify.",
                    color=discord.Color.red())
                await interaction.followup.edit_message(interaction.message.id,
                                                        embed=embed,
                                                        view=None)
                return

            # Track results
            successful_trades = []
            failed_trades = []

            # Process each selected trade
            for message_id in selected_trades:
                try:
                    # Fetch from database
                    trade_data = await bot.get_trade_from_db(message_id)
                    if not trade_data:
                        failed_trades.append({
                            "id": message_id[:8],
                            "reason": "Not found in database"
                        })
                        continue

                    pair = trade_data["pair"]
                    action = trade_data["action"]
                    current_tp_hits = trade_data.get("tp_hits", [])

                    # Process status-specific logic
                    if status == "sl_hit":
                        trade_data[
                            "status"] = "closed (sl hit - manual override)"
                        manual_overrides = trade_data.get(
                            "manual_overrides", [])
                        if "sl" not in manual_overrides:
                            manual_overrides.append("sl")
                        trade_data["manual_overrides"] = manual_overrides
                        await bot.remove_trade_from_db(message_id,
                                                       "manual_sl_hit")
                        await bot.send_sl_notification(message_id,
                                                       trade_data,
                                                       offline_hit=False,
                                                       manual_override=True)
                        successful_trades.append(f"{pair} {action} - SL Hit")

                    elif status in ["tp1_hit", "tp2_hit", "tp3_hit"]:
                        tp_level = status.replace("_hit", "")

                        if tp_level in current_tp_hits:
                            failed_trades.append({
                                "id":
                                message_id[:8],
                                "reason":
                                f"{tp_level.upper()} already hit"
                            })
                            continue

                        manual_overrides = trade_data.get(
                            "manual_overrides", [])

                        def add_tp_hit_and_override(tp_name):
                            if tp_name not in trade_data["tp_hits"]:
                                trade_data["tp_hits"].append(tp_name)
                            if tp_name not in manual_overrides:
                                manual_overrides.append(tp_name)

                        add_tp_hit_and_override(tp_level)
                        trade_data["manual_overrides"] = manual_overrides

                        if tp_level == "tp1":
                            trade_data[
                                "status"] = "active (tp1 hit - manual override)"
                            await bot.update_trade_in_db(
                                message_id, trade_data)
                        elif tp_level == "tp2":
                            if "tp1" not in current_tp_hits:
                                add_tp_hit_and_override("tp1")
                                trade_data[
                                    "manual_overrides"] = manual_overrides
                                await bot.send_tp_notification(
                                    message_id,
                                    trade_data,
                                    "tp1",
                                    offline_hit=False,
                                    manual_override=True)
                                await asyncio.sleep(2)
                            trade_data["breakeven_active"] = True
                            trade_data[
                                "status"] = "active (tp2 hit - manual override - breakeven active)"
                            await bot.update_trade_in_db(
                                message_id, trade_data)
                        elif tp_level == "tp3":
                            if "tp1" not in current_tp_hits:
                                add_tp_hit_and_override("tp1")
                                trade_data[
                                    "manual_overrides"] = manual_overrides
                                await bot.send_tp_notification(
                                    message_id,
                                    trade_data,
                                    "tp1",
                                    offline_hit=False,
                                    manual_override=True)
                                await asyncio.sleep(2)
                            if "tp2" not in current_tp_hits:
                                add_tp_hit_and_override("tp2")
                                trade_data["breakeven_active"] = True
                                trade_data[
                                    "manual_overrides"] = manual_overrides
                                await bot.send_tp_notification(
                                    message_id,
                                    trade_data,
                                    "tp2",
                                    offline_hit=False,
                                    manual_override=True)
                                await asyncio.sleep(2)
                            trade_data[
                                "status"] = "completed (tp3 hit - manual override)"
                            await bot.remove_trade_from_db(
                                message_id, "manual_tp3_hit")

                        await bot.send_tp_notification(message_id,
                                                       trade_data,
                                                       tp_level,
                                                       offline_hit=False,
                                                       manual_override=True)
                        successful_trades.append(
                            f"{pair} {action} - {tp_level.upper()} Hit")

                    elif status == "breakeven_after_tp2":
                        trade_data[
                            "status"] = "closed (breakeven after tp2 - manual override)"
                        manual_overrides = trade_data.get(
                            "manual_overrides", [])
                        if "breakeven" not in manual_overrides:
                            manual_overrides.append("breakeven")
                        trade_data["manual_overrides"] = manual_overrides
                        await bot.remove_trade_from_db(message_id,
                                                       "manual_breakeven_hit")
                        await bot.send_breakeven_notification(
                            message_id, trade_data, offline_hit=False)
                        successful_trades.append(
                            f"{pair} {action} - Breakeven After TP2")

                    elif status == "end_tracking":
                        trade_data[
                            "status"] = "closed (ended by manual override)"
                        await bot.remove_trade_from_db(message_id,
                                                       "manual_end_tracking")
                        successful_trades.append(
                            f"{pair} {action} - Tracking Ended")

                except Exception as trade_exc:
                    failed_trades.append({
                        "id":
                        message_id[:8] if message_id else "Unknown",
                        "reason":
                        str(trade_exc)[:50]
                    })

            # Refresh cache
            fresh_trades = await bot.get_active_trades_from_db()
            self.view.active_trades = fresh_trades

            # Build summary embed
            total = len(successful_trades) + len(failed_trades)

            if len(successful_trades) == total:
                title = "✅ All Trades Updated Successfully"
                color = discord.Color.green()
            elif len(successful_trades) > 0:
                title = "⚠️ Partial Success"
                color = discord.Color.orange()
            else:
                title = "❌ All Trades Failed"
                color = discord.Color.red()

            description = f"**Processed {total} trade(s)**\n\n"

            if successful_trades:
                description += "**✅ Successful:**\n"
                for trade_info in successful_trades:
                    description += f"• {trade_info}\n"
                description += "\n"

            if failed_trades:
                description += "**❌ Failed:**\n"
                for fail_info in failed_trades:
                    description += f"• ID {fail_info['id']}: {fail_info['reason']}\n"

            embed = discord.Embed(title=title,
                                  description=description,
                                  color=color)

            # Disable view
            for item in self.view.children:
                item.disabled = True

            await interaction.followup.edit_message(interaction.message.id,
                                                    embed=embed,
                                                    view=self.view)

        except Exception as e:
            embed = discord.Embed(
                title="❌ Error Processing Override",
                description=f"Failed to update trade status: {str(e)}",
                color=discord.Color.red())
            await interaction.followup.edit_message(interaction.message.id,
                                                    embed=embed,
                                                    view=None)
            print(f"Trade override error: {e}")


@bot.tree.command(
    name="tradeoverride",
    description=
    "[OWNER ONLY] Manually set the status of tracked trading signals using interactive menus"
)
async def trade_override_command(interaction: discord.Interaction):
    """Interactive menu system for manually overriding trade statuses"""
    if not await owner_check(interaction):
        return

    await interaction.response.defer(ephemeral=True)

    try:
        # Get active trades from database
        active_trades = await bot.get_active_trades_from_db()

        if not active_trades:
            embed = discord.Embed(
                title="📊 Trade Override System",
                description=
                "❌ **No active trades found**\n\nThere are currently no trading signals being tracked.\nUse `/activetrades view` to confirm.",
                color=discord.Color.red())
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        # Create initial embed
        embed = discord.Embed(
            title="🔧 Trade Override System",
            description=
            f"Found **{len(active_trades)}** active trades.\n\nSelect a trade from the dropdown below:",
            color=discord.Color.blue())

        # Create interactive view
        view = TradeOverrideView(active_trades)

        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

    except Exception as e:
        embed = discord.Embed(
            title="❌ Error Loading Trades",
            description=f"Failed to load active trades: {str(e)}",
            color=discord.Color.red())
        await interaction.followup.send(embed=embed, ephemeral=True)
        print(f"Trade override loading error: {e}")


# SL-TP Scraper command removed as requested by user
# @bot.tree.command(name="sl-tpscraper", description="[OWNER ONLY] Analyze trade signals and performance within a date range")
# @app_commands.describe(
#     start_date="Start date (format: YYYY-MM-DD, e.g., 2025-09-01)",
#     end_date="End date (format: YYYY-MM-DD, e.g., 2025-09-09)"
# )
# async def sl_tp_scraper_command(interaction: discord.Interaction, start_date: str, end_date: str):
# Command body removed


# Web server for health checks
async def web_server():
    """Simple web server for health checks and keeping the service alive"""
    runner = None

    async def health_check(request):
        bot_status = "Connected" if bot.is_ready() else "Connecting"
        guild_count = len(bot.guilds) if bot.is_ready() else 0

        # Check database connection status
        database_status = "Not configured"
        database_details = {}

        if bot.db_pool:
            try:
                async with bot.db_pool.acquire() as conn:
                    # Test database connection
                    version = await conn.fetchval('SELECT version()')
                    database_status = "Connected"
                    database_details = {
                        "postgresql_version":
                        version.split()[1] if version else "Unknown",
                        "pool_size": bot.db_pool.get_size(),
                        "pool_idle": bot.db_pool.get_idle_size()
                    }
            except Exception as e:
                database_status = f"Error: {str(e)[:50]}"

        response_data = {
            "status":
            "running",
            "bot_status":
            bot_status,
            "bot_user":
            str(bot.user) if bot.user else "Not logged in",
            "bot_id":
            bot.user.id if bot.user else None,
            "guild_count":
            guild_count,
            "guild_names":
            [guild.name for guild in bot.guilds] if bot.is_ready() else [],
            "database_status":
            database_status,
            "database_details":
            database_details,
            "uptime":
            str(datetime.now()),
            "version":
            "2.2 - Optimized API & Random Messages",
            "last_heartbeat":
            str(bot.last_heartbeat) if hasattr(bot, 'last_heartbeat')
            and bot.last_heartbeat else "N/A",
            "bot_latency":
            f"{round(bot.latency * 1000)}ms" if bot.is_ready() else "N/A",
            "is_ready":
            bot.is_ready(),
            "is_closed":
            bot.is_closed(),
            "token_length":
            len(DISCORD_TOKEN) if DISCORD_TOKEN else 0,
            "intents":
            str(bot.intents) if hasattr(bot, 'intents') else "N/A"
        }

        return web.json_response(response_data, status=200)

    async def root_handler(request):
        return web.Response(text="Discord Trading Bot is running!", status=200)

    app = web.Application()
    app.router.add_get('/', root_handler)
    app.router.add_get('/health', health_check)
    app.router.add_get('/status', health_check)

    try:
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 5000)
        await site.start()
        print("✅ Web server started successfully on port 5000")
        print("Health check available at: http://0.0.0.0:5000/health")

        try:
            # Keep the server running
            while True:
                await asyncio.sleep(3600)  # Sleep for 1 hour, then continue
        except asyncio.CancelledError:
            print("Web server shutting down...")
        finally:
            # Cleanup web server properly
            await runner.cleanup()
            print("✅ Web server cleaned up properly")

    except Exception as e:
        print(f"❌ Failed to start web server: {e}")
        if runner:
            await runner.cleanup()
        raise


async def main():
    """Main async function to run both web server and Discord bot concurrently"""
    # Check if Discord token is available
    if not DISCORD_TOKEN or len(DISCORD_TOKEN) < 50:
        print("Error: DISCORD_TOKEN not found in environment variables")
        print(
            f"Token parts found: PART1={bool(DISCORD_TOKEN_PART1)}, PART2={bool(DISCORD_TOKEN_PART2)}"
        )
        print(
            f"Assembled token length: {len(DISCORD_TOKEN) if DISCORD_TOKEN else 0}"
        )
        print(
            "Please set DISCORD_TOKEN_PART1 and DISCORD_TOKEN_PART2 environment variables"
        )
        return

    print(f"Bot token length: {len(DISCORD_TOKEN)} characters")
    print("Starting Discord Trading Bot...")

    # Minimal startup delay to avoid overwhelming Discord
    print("🕐 Adding minimal startup delay...")
    await asyncio.sleep(5)  # Short delay

    # Create tasks for concurrent execution
    tasks = []

    # Web server task
    print("Starting web server...")
    web_task = asyncio.create_task(web_server())
    tasks.append(web_task)

    # Discord bot task with minimal retries to prevent IP banning
    async def start_bot_with_retry():
        max_retries = 1  # Only try once to prevent IP banning
        retry_delay = 300  # 5 minutes between attempts

        print("🤖 DISCORD BOT STARTUP SEQUENCE:")
        print(f"   Bot object created: {bot is not None}")
        print(f"   Bot user: {bot.user}")
        print(f"   Intents: {bot.intents}")

        for attempt in range(max_retries):
            try:
                print(
                    f"🚀 Starting Discord bot (attempt {attempt + 1}/{max_retries})..."
                )
                print(f"   Using token length: {len(DISCORD_TOKEN)}")
                print(f"   Bot is closed: {bot.is_closed()}")

                # Test token format before attempting connection
                if not DISCORD_TOKEN or len(DISCORD_TOKEN) < 50:
                    raise ValueError("Invalid Discord token format or length")

                if not DISCORD_TOKEN.count('.') >= 2:
                    raise ValueError(
                        "Discord token format invalid - should contain at least 2 dots"
                    )

                print("   Token format validation passed")
                print("   Attempting Discord connection...")

                await bot.start(DISCORD_TOKEN)
                print("✅ Discord bot started successfully!")
                break  # If successful, break out of retry loop

            except discord.LoginFailure as e:
                print(f"❌ DISCORD LOGIN FAILURE: {e}")
                print("   This indicates invalid bot token")
                print("   Please verify your Discord bot token is correct")
                print(
                    f"   Token being used starts with: {DISCORD_TOKEN[:20]}..."
                )
                break  # Don't retry on login failures

            except discord.HTTPException as e:
                print(f"❌ DISCORD HTTP ERROR: {e}")
                print(f"   Status code: {getattr(e, 'status', 'Unknown')}")
                print(f"   Response: {getattr(e, 'response', 'No response')}")

                if e.status == 429:  # Rate limited
                    # Check if this is a Cloudflare rate limit (Error 1015)
                    if "cloudflare" in str(e).lower() or "1015" in str(e):
                        print("   🚨 CLOUDFLARE IP BAN DETECTED (Error 1015)")
                        print(
                            "   Your Render server IP is banned by Discord's Cloudflare"
                        )
                        print(
                            "   This requires a different approach - IP ban won't resolve with waiting"
                        )

                        # Log the exact issue for user
                        print(
                            "   ❌ CRITICAL: Bot cannot run 24/7 until IP ban is lifted"
                        )
                        print(
                            "   💡 SOLUTION: Need to change server IP or hosting provider"
                        )
                        break  # Don't retry - IP is banned
                    else:
                        print(
                            f"   Normal rate limit. Waiting {retry_delay} seconds before retry..."
                        )
                        await asyncio.sleep(retry_delay)
                elif e.status == 401:  # Unauthorized
                    print("   401 Unauthorized - Invalid bot token")
                    break
                elif e.status == 403:  # Forbidden
                    print(
                        "   403 Forbidden - Bot may be banned or token invalid"
                    )
                    break
                else:
                    print(f"   HTTP error {e.status}")
                    if attempt < max_retries - 1:
                        print(f"   Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                    else:
                        print("   Max retries reached. Bot failed to start.")

            except discord.ConnectionClosed as e:
                print(f"❌ DISCORD CONNECTION CLOSED: {e}")
                print(f"   Code: {getattr(e, 'code', 'Unknown')}")
                print(f"   Reason: {getattr(e, 'reason', 'Unknown')}")
                if attempt < max_retries - 1:
                    print(f"   Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)

            except discord.GatewayNotFound as e:
                print(f"❌ DISCORD GATEWAY NOT FOUND: {e}")
                print("   Discord gateway endpoint not found")
                if attempt < max_retries - 1:
                    print(f"   Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)

            except ValueError as e:
                print(f"❌ TOKEN VALIDATION ERROR: {e}")
                break  # Don't retry on token validation errors

            except Exception as e:
                print(f"❌ UNEXPECTED ERROR STARTING DISCORD BOT: {e}")
                print(f"   Error type: {type(e).__name__}")
                print(f"   Error details: {str(e)}")
                import traceback
                traceback.print_exc()

                if attempt < max_retries - 1:
                    print(f"   Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    print("   Max retries reached. Bot failed to start.")

        print("🔚 Bot startup sequence completed")

    bot_task = asyncio.create_task(start_bot_with_retry())
    tasks.append(bot_task)

    # Wait for all tasks
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except KeyboardInterrupt:
        print("Shutting down...")
        await bot.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot shutdown complete.")
    except Exception as e:
        print(f"Fatal error: {e}")
