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

# Debug token loading with detailed information
print("🔍 DEBUGGING DISCORD TOKEN LOADING:")
print(f"   DISCORD_TOKEN_PART1 exists: {bool(DISCORD_TOKEN_PART1)}")
print(f"   DISCORD_TOKEN_PART1 length: {len(DISCORD_TOKEN_PART1) if DISCORD_TOKEN_PART1 else 0}")
print(f"   DISCORD_TOKEN_PART2 exists: {bool(DISCORD_TOKEN_PART2)}")
print(f"   DISCORD_TOKEN_PART2 length: {len(DISCORD_TOKEN_PART2) if DISCORD_TOKEN_PART2 else 0}")
print(f"   Combined token length: {len(DISCORD_TOKEN) if DISCORD_TOKEN else 0}")

if not DISCORD_TOKEN_PART1:
    print("❌ DISCORD_TOKEN_PART1 is empty or not found")
if not DISCORD_TOKEN_PART2:
    print("❌ DISCORD_TOKEN_PART2 is empty or not found")
if DISCORD_TOKEN and len(DISCORD_TOKEN) > 50:
    print(f"✅ Discord token assembled successfully (length: {len(DISCORD_TOKEN)})")
    print(f"   Token starts with: {DISCORD_TOKEN[:10]}...")
    print(f"   Token ends with: ...{DISCORD_TOKEN[-10:]}")
else:
    print("❌ Failed to assemble Discord token or token too short")

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

# Global storage for active giveaways
ACTIVE_GIVEAWAYS = {}  # giveaway_id: {message_id, participants, settings, etc}

# Global storage for invite tracking
INVITE_TRACKING = {}  # invite_code: {"nickname": str, "total_joins": int, "total_left": int, "current_members": int, "creator_id": int, "guild_id": int}

# Live price tracking system configuration
PRICE_TRACKING_CONFIG = {
    "enabled": True,  # 24/7 monitoring enabled by default
    "excluded_channel_id": "1394958907943817326",
    "owner_user_id": "462707111365836801",
    "signal_keyword": "Trade Signal For:",
    "active_trades": {},  # message_id: {trade_data}
    "api_keys": {
        "fxapi_key": os.getenv("FXAPI_KEY", ""),
        "alpha_vantage_key": os.getenv("ALPHA_VANTAGE_KEY", ""),
        "twelve_data_key": os.getenv("TWELVE_DATA_KEY", ""),
        "fmp_key": os.getenv("FMP_KEY", "")
    },
    "api_endpoints": {
        "fxapi": "https://fxapi.com/api/latest",
        "alpha_vantage": "https://www.alphavantage.co/query",
        "twelve_data": "https://api.twelvedata.com/price",
        "fmp": "https://financialmodelingprep.com/api/v3/quote"
    },
    "last_price_check": {},  # pair: last_check_timestamp
    "check_interval": 225,  # seconds between price checks (3 minutes 45 seconds)
    "api_rotation_index": 0  # for rotating through APIs efficiently
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
                            dm_message = "Hey! It's been 3 days since your **24-hour free access to the Premium Signals channel** ended. We hope you were able to catch good trades with us during that time.\n\nAs you've probably seen, the **free signals channel only gets about 1 signal a day**, while inside **Gold Pioneers**, members receive **8–10 high-quality signals every single day in <#1350929852299214999>**. That means way more chances to profit and grow consistently.\n\nWe'd love to **invite you back to Premium Signals** so you don't miss out on more solid opportunities.\n\n**Feel free to join us again through this link:** https://whop.com/gold-pioneer"
                            await member.send(dm_message)
                            AUTO_ROLE_CONFIG["dm_schedule"][member_id_str]["dm_3_sent"] = True
                            recovered_dms += 1
                            await self.log_to_discord(f"📤 Sent missed 3-day DM to {member.display_name}")
                        except discord.Forbidden:
                            await self.log_to_discord(f"❌ Could not send missed 3-day DM to {member.display_name} (DMs disabled)")
                    
                    # Send missed 7-day DM
                    if not dm_data["dm_7_sent"] and current_time >= dm_7_time:
                        try:
                            dm_message = "It's been a week since your Premium Signals trial ended. Since then, our **Gold Pioneers  have been catching trade setups daily in <#1350929852299214999>**.\n\nIf you found value in just 24 hours, imagine the results you could be seeing by now with full access. It's all about **consistency and staying plugged into the right information**.\n\nWe'd like to **personally invite you to rejoin Premium Signals** and get back into the rhythm.\n\n\n**Feel free to join us again through this link:** https://whop.com/gold-pioneer"
                            await member.send(dm_message)
                            AUTO_ROLE_CONFIG["dm_schedule"][member_id_str]["dm_7_sent"] = True
                            recovered_dms += 1
                            await self.log_to_discord(f"📤 Sent missed 7-day DM to {member.display_name}")
                        except discord.Forbidden:
                            await self.log_to_discord(f"❌ Could not send missed 7-day DM to {member.display_name} (DMs disabled)")
                    
                    # Send missed 14-day DM
                    if not dm_data["dm_14_sent"] and current_time >= dm_14_time:
                        try:
                            dm_message = "Hey! It's been two weeks since your access to Premium Signals ended. We hope you've stayed active. \n\nIf you've been trading solo or passively following the free channel, you might be feeling the difference. in <#1350929852299214999>, it's not just about more signals. It's about the **structure, support, and smarter decision-making**. That edge can make all the difference over time.\n\nWe'd love to **officially invite you back into Premium Signals** and help you start compounding results again.\n\n**Feel free to join us again through this link:** https://whop.com/gold-pioneer"
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
                                
                                # Store both Discord and historical prices
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

    async def get_historical_price(self, pair: str, timestamp: datetime) -> Optional[float]:
        """Get historical price for a trading pair at a specific timestamp"""
        try:
            # For now, use current price as fallback (historical prices require different APIs)
            # This could be enhanced with time-series APIs in the future
            current_price = await self.get_live_price(pair)
            if current_price:
                print(f"⚠️ Using current price for historical lookup: {pair} at {timestamp.strftime('%Y-%m-%d %H:%M')}")
                return current_price
            return None
        except Exception as e:
            print(f"Error getting historical price for {pair}: {e}")
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
                    print(f"Error checking price for trade {message_id}: {e}")
                    trades_to_remove.append(message_id)
            
            # Remove failed trades from database
            for message_id in trades_to_remove:
                await self.remove_trade_from_db(message_id)
                print(f"Removed failed trade {message_id} from tracking")
                    
        except Exception as e:
            print(f"Error in price tracking loop: {e}")

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
                # Weekend join - expires Monday 23:59 (not Tuesday 01:00)
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
                        "**:star2: Welcome to FX Pip Pioneers! :star2:**\n\n"
                        ":white_check_mark: As a welcome gift, we've given you access to our **Premium Signals channel for 24 hours.** "
                        "That means you can start profiting from the **8–10 trade signals** we send per day right now!\n\n"
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
                f"❌ Error assigning auto-role to {member.display_name}: {str(e)}"
            )

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
                        
                        print(f"✅ Signal tracking: Discord entry ${trade_data['discord_entry']}, Live entry ${live_price}")
                        print(f"   Tracking TP/SL based on live price: TP1=${trade_data['tp1']}, SL=${trade_data['sl']}")
                    else:
                        print(f"⚠️ Could not get live price for {trade_data['pair']}, using Discord prices for tracking")
                    
                    # Add channel and message info
                    trade_data["channel_id"] = message.channel.id
                    trade_data["message_id"] = str(message.id)
                    trade_data["timestamp"] = message.created_at.isoformat()
                    
                    # Add to active trades with database persistence
                    await self.save_trade_to_db(str(message.id), trade_data)
                    
                    print(f"✅ Started tracking signal for {trade_data['pair']} ({trade_data['action']}) - Message ID: {message.id}")
                else:
                    print(f"❌ Could not parse signal from message: {message.content[:100]}...")
            except Exception as e:
                print(f"Error processing signal message: {e}")
        
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
            print("⚠️ No database - active trades will be in-memory only")
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
                
                if PRICE_TRACKING_CONFIG["active_trades"]:
                    print(f"🔄 Loaded {len(PRICE_TRACKING_CONFIG['active_trades'])} active trades from database")
                    print("✅ 24/7 trade tracking fully restored from database")
                else:
                    print("📊 No active trades found in database - ready for new signals")
        
        except Exception as e:
            print(f"❌ Error loading active trades from database: {str(e)}")
            print("   Continuing with in-memory storage only")

    async def save_trade_to_db(self, message_id: str, trade_data: dict):
        """Save a new trading signal to database for persistence"""
        if not self.db_pool:
            return
        
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
                
                # Also update in-memory for fast access
                PRICE_TRACKING_CONFIG["active_trades"][message_id] = trade_data
                
        except Exception as e:
            print(f"❌ Error saving trade to database: {str(e)}")

    async def update_trade_in_db(self, message_id: str, trade_data: dict):
        """Update an existing trade in database"""
        if not self.db_pool:
            return
        
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
                
                # Also update in-memory
                PRICE_TRACKING_CONFIG["active_trades"][message_id] = trade_data
                
        except Exception as e:
            print(f"❌ Error updating trade in database: {str(e)}")

    async def remove_trade_from_db(self, message_id: str):
        """Remove completed trade from database"""
        if not self.db_pool:
            return
        
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute('DELETE FROM active_trades WHERE message_id = $1', message_id)
                
                # Also remove from in-memory
                if message_id in PRICE_TRACKING_CONFIG["active_trades"]:
                    del PRICE_TRACKING_CONFIG["active_trades"][message_id]
                
        except Exception as e:
            print(f"❌ Error removing trade from database: {str(e)}")

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
            print(f"❌ Error getting active trades: {str(e)}")
            return PRICE_TRACKING_CONFIG["active_trades"]

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
        """Get price using smart API rotation to minimize free tier usage"""
        # Define API priority order (most reliable first)
        api_order = ["twelve_data", "fxapi", "alpha_vantage", "fmp"]
        
        # Rotate through APIs to distribute load
        start_index = PRICE_TRACKING_CONFIG["api_rotation_index"] % len(api_order)
        
        # Try 2 APIs max per check (primary + backup)
        for i in range(2):
            api_index = (start_index + i) % len(api_order)
            api_name = api_order[api_index]
            
            price = await self.get_price_from_single_api(api_name, pair_clean)
            if price is not None:
                # Update rotation for next check
                PRICE_TRACKING_CONFIG["api_rotation_index"] = (start_index + 1) % len(api_order)
                await self.log_to_discord(f"✅ **SUCCESS:** Price from {api_name} for {pair_clean}: ${price:.5f}")
                return price
        
        await self.log_to_discord(f"⚠️ **Primary APIs failed** for {pair_clean}, trying all APIs as fallback")
        return await self.get_verified_price_all_apis(pair_clean)
    
    def get_api_symbol(self, api_name: str, pair_clean: str) -> str:
        """Map user-friendly symbols to API-specific symbols"""
        # Let's try multiple variations for each API to find what works
        symbol_mappings = {
            "fxapi": {
                "US100": ["US100", "USTEC", "NDX", "NASDAQ"],   # Try multiple variations
                "GER40": ["GER40", "DAX", "DE40"],               # Try multiple variations  
                "GER30": ["GER30", "DAX", "DE30"],               # Try multiple variations
                "NAS100": ["US100", "USTEC", "NDX", "NASDAQ"],  # Alternative name for US100
                "US500": ["US500", "SPX", "SPY"],               # S&P 500
                "UK100": ["UK100", "UKX", "FTSE"],              # FTSE 100
                "JPN225": ["JPN225", "N225", "NKY"],             # Nikkei 225
                "AUS200": ["AUS200", "ASX", "XJO"]               # ASX 200
            },
            "twelve_data": {
                "US100": ["NDX", "IXIC", "USTEC"],              # Twelve Data variations
                "GER40": ["DAX", "GDAXI", "DE40"],              # German DAX index
                "GER30": ["DAX", "GDAXI", "DE30"],              # Alternative name
                "NAS100": ["NDX", "IXIC", "USTEC"],             # Alternative name for Nasdaq 100
                "US500": ["SPX", "GSPC"],                       # S&P 500
                "UK100": ["UKX", "FTSE"],                       # FTSE 100
                "JPN225": ["N225", "NKY"],                      # Nikkei 225
                "AUS200": ["XJO", "AXJO"]                       # ASX 200
            },
            "alpha_vantage": {
                # Alpha Vantage doesn't support indices through currency exchange rate
                # These symbols won't work with the current implementation
                # We'll skip Alpha Vantage for indices
            },
            "fmp": {
                "US100": ["^NDX", "^IXIC", "NDX"],             # FMP variations
                "GER40": ["^GDAXI", "^DAX", "GDAXI"],          # German DAX index
                "GER30": ["^GDAXI", "^DAX", "GDAXI"],          # Alternative name
                "NAS100": ["^NDX", "^IXIC", "NDX"],            # Alternative name for Nasdaq 100
                "US500": ["^SPX", "^GSPC"],                    # S&P 500
                "UK100": ["^UKX", "^FTSE"],                    # FTSE 100
                "JPN225": ["^N225", "^NKY"],                   # Nikkei 225
                "AUS200": ["^AXJO", "^XJO"]                    # ASX 200
            }
        }
        
        # Get API-specific mapping - return first symbol to try
        if api_name in symbol_mappings and pair_clean in symbol_mappings[api_name]:
            symbol_list = symbol_mappings[api_name][pair_clean]
            if isinstance(symbol_list, list) and symbol_list:
                mapped_symbol = symbol_list[0]  # Use first symbol for now
                print(f"🔄 Mapping {pair_clean} → {mapped_symbol} for {api_name} (from options: {symbol_list})")
                return mapped_symbol
            elif isinstance(symbol_list, str):
                print(f"🔄 Mapping {pair_clean} → {symbol_list} for {api_name}")
                return symbol_list
        
        # Return original symbol if no mapping found
        return pair_clean

    async def get_price_from_single_api(self, api_name: str, pair_clean: str) -> Optional[float]:
        """Get price from a specific API"""
        try:
            # Map symbol to API-specific format
            api_symbol = self.get_api_symbol(api_name, pair_clean)
            
            # Skip Alpha Vantage for indices (it doesn't support them via currency exchange)
            if api_name == "alpha_vantage" and pair_clean in ["US100", "GER40", "GER30", "NAS100", "US500", "UK100", "JPN225", "AUS200"]:
                print(f"⏭️ Skipping Alpha Vantage for index {pair_clean} (not supported)")
                return None
            if api_name == "fxapi" and PRICE_TRACKING_CONFIG["api_keys"]["fxapi_key"]:
                url = f"{PRICE_TRACKING_CONFIG['api_endpoints']['fxapi']}"
                params = {
                    "access_key": PRICE_TRACKING_CONFIG["api_keys"]["fxapi_key"],
                    "symbols": api_symbol
                }
                
                async with aiohttp.ClientSession() as session:
                    debug_msg = f"🔍 **FXApi Debug for {pair_clean}**\nRequest: {url}\nParams: {params}"
                    await self.log_to_discord(debug_msg)
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        response_text = await response.text()
                        response_debug = f"🔍 **FXApi Response**\nStatus: {response.status}\nData: {response_text[:500]}..."
                        await self.log_to_discord(response_debug)
                        if response.status == 200:
                            data = await response.json()
                            if "rates" in data and api_symbol in data["rates"]:
                                price = float(data["rates"][api_symbol])
                                print(f"✅ FXApi found price for {api_symbol}: {price}")
                                return price
                            else:
                                print(f"❌ FXApi: Symbol {api_symbol} not found in rates. Available symbols: {list(data.get('rates', {}).keys())[:10]}...")
                        elif response.status == 429:
                            await self.log_api_limit_warning("FXApi", "Rate limit exceeded - switching to backup API")
                        elif response.status == 403:
                            await self.log_api_limit_warning("FXApi", "Access denied - API key may be invalid")
            
            elif api_name == "twelve_data" and PRICE_TRACKING_CONFIG["api_keys"]["twelve_data_key"]:
                url = f"{PRICE_TRACKING_CONFIG['api_endpoints']['twelve_data']}"
                params = {
                    "symbol": api_symbol,
                    "apikey": PRICE_TRACKING_CONFIG["api_keys"]["twelve_data_key"]
                }
                
                async with aiohttp.ClientSession() as session:
                    debug_msg = f"🔍 **Twelve Data Debug for {pair_clean}**\nRequest: {url}\nParams: {params}"
                    await self.log_to_discord(debug_msg)
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        response_text = await response.text()
                        response_debug = f"🔍 **Twelve Data Response**\nStatus: {response.status}\nData: {response_text[:500]}..."
                        await self.log_to_discord(response_debug)
                        if response.status == 200:
                            data = await response.json()
                            if "price" in data:
                                price = float(data["price"])
                                print(f"✅ Twelve Data found price for {api_symbol}: {price}")
                                return price
                            elif "message" in data and "limit" in data["message"].lower():
                                print(f"❌ Twelve Data: Usage limit reached. Data: {data}")
                                await self.log_api_limit_warning("Twelve Data", f"Usage limit: {data['message']}")
                            else:
                                print(f"❌ Twelve Data: No 'price' field found. Data: {data}")
                        elif response.status == 429:
                            await self.log_api_limit_warning("Twelve Data", "Rate limit exceeded - switching to backup")
            
            elif api_name == "alpha_vantage" and PRICE_TRACKING_CONFIG["api_keys"]["alpha_vantage_key"]:
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
                                    return float(rate_data["5. Exchange Rate"])
                            elif "Note" in data and "call frequency" in data["Note"]:
                                await self.log_api_limit_warning("Alpha Vantage", "Daily limit reached - switching to backup")
                        elif response.status == 429:
                            await self.log_api_limit_warning("Alpha Vantage", "Rate limit exceeded")
            
            elif api_name == "fmp" and PRICE_TRACKING_CONFIG["api_keys"]["fmp_key"]:
                url = f"{PRICE_TRACKING_CONFIG['api_endpoints']['fmp']}/{api_symbol}"
                params = {
                    "apikey": PRICE_TRACKING_CONFIG["api_keys"]["fmp_key"]
                }
                
                async with aiohttp.ClientSession() as session:
                    debug_msg = f"🔍 **FMP Debug for {pair_clean}**\nRequest: {url}\nParams: {params}"
                    await self.log_to_discord(debug_msg)
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        response_text = await response.text()
                        response_debug = f"🔍 **FMP Response**\nStatus: {response.status}\nData: {response_text[:500]}..."
                        await self.log_to_discord(response_debug)
                        if response.status == 200:
                            data = await response.json()
                            if isinstance(data, list) and len(data) > 0 and "price" in data[0]:
                                price = float(data[0]["price"])
                                print(f"✅ FMP found price for {api_symbol}: {price}")
                                return price
                            elif isinstance(data, dict) and "Error Message" in data:
                                print(f"❌ FMP: Error message received. Data: {data}")
                                if "limit" in data["Error Message"].lower():
                                    await self.log_api_limit_warning("Financial Modeling Prep", f"Usage limit: {data['Error Message']}")
                            else:
                                print(f"❌ FMP: Unexpected data format. Data: {data}")
                        elif response.status == 429:
                            await self.log_api_limit_warning("Financial Modeling Prep", "Rate limit exceeded")
        
        except Exception as e:
            print(f"❌ {api_name} failed for {pair_clean} (mapped to {api_symbol}): {e}")
        
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
                    "access_key": PRICE_TRACKING_CONFIG["api_keys"]["fxapi_key"],
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
            print(f"FXApi failed for {pair_clean}: {e}")
        
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
            print(f"Twelve Data API failed for {pair_clean}: {e}")
        
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
            print(f"Alpha Vantage API failed for {pair_clean}: {e}")
        
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
                print(f"❌ Signal parsing failed - missing fields: {missing_fields}")
            
        except Exception as e:
            print(f"Error parsing signal: {e}")
        
        return None

    async def check_price_levels(self, message_id: str, trade_data: Dict) -> bool:
        """Check if current price has hit any TP/SL levels"""
        try:
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

    async def handle_tp_hit(self, message_id: str, trade_data: Dict, tp_level: str):
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
            await self.send_tp_notification(message_id, trade_data, tp_level)
            
        except Exception as e:
            print(f"Error handling TP hit: {e}")

    async def handle_sl_hit(self, message_id: str, trade_data: Dict):
        """Handle when SL is hit"""
        try:
            trade_data["status"] = "closed (sl hit)"
            
            # Remove from active trades (database and memory)
            await self.remove_trade_from_db(message_id)
            
            # Send notification
            await self.send_sl_notification(message_id, trade_data)
            
        except Exception as e:
            print(f"Error handling SL hit: {e}")

    async def handle_breakeven_hit(self, message_id: str, trade_data: Dict):
        """Handle when price returns to breakeven after TP2"""
        try:
            trade_data["status"] = "closed (breakeven after tp2)"
            
            # Remove from active trades (database and memory)
            await self.remove_trade_from_db(message_id)
            
            # Send breakeven notification
            await self.send_breakeven_notification(message_id, trade_data)
            
        except Exception as e:
            print(f"Error handling breakeven hit: {e}")

    async def send_tp_notification(self, message_id: str, trade_data: Dict, tp_level: str):
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
            
            await message.reply(notification)
            
        except Exception as e:
            print(f"Error sending TP notification: {e}")

    async def send_sl_notification(self, message_id: str, trade_data: Dict):
        """Send SL hit notification with random message selection"""
        import random
        
        try:
            # Random messages for SL hits
            sl_messages = [
                "@everyone This one hit SL. It happens. Let's stay focused and get the next one 🔄🧠",
                "@everyone SL has been hit. Risk was managed, we move on 💪📉",
                "@everyone This setup didn't go as planned and hit SL. On to the next 📊",
                "@everyone SL hit. It's all part of the process. Stay disciplined 💼📚",
                "@everyone SL hit. Losses are part of trading. We bounce back 📈⏭️",
                "@everyone SL hit. Trust the process and prepare for the next opportunity 🔄🧠",
                "@everyone SL was hit on this one. We took the loss, now let's stay sharp 🔁💪",
                "@everyone SL hit. It's part of the game. Let's stay focused on quality 📉🎯",
                "@everyone This trade hit SL. Discipline keeps us in the game. We´ll get the loss back next trade💼🧘‍♂️"
            ]
            
            message = await self.get_channel(trade_data.get("channel_id")).fetch_message(int(message_id))
            notification = random.choice(sl_messages)
            
            await message.reply(notification)
            
        except Exception as e:
            print(f"Error sending SL notification: {e}")

    async def send_breakeven_notification(self, message_id: str, trade_data: Dict):
        """Send breakeven hit notification"""
        try:
            message = await self.get_channel(trade_data.get("channel_id")).fetch_message(int(message_id))
            notification = f"This pair reversed to breakeven after we hit TP2. As we stated, we had already moved our SL to breakeven, so we were out safe.\n@everyone"
            
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
            "Hey! It's been 3 days since your **24-hour free access to the Premium Signals channel** ended. We hope you were able to catch good trades with us during that time.\n\nAs you've probably seen, the **free signals channel only gets about 1 signal a day**, while inside **Gold Pioneers**, members receive **8–10 high-quality signals every single day in <#1350929852299214999>**. That means way more chances to profit and grow consistently.\n\nWe'd love to **invite you back to Premium Signals** so you don't miss out on more solid opportunities.\n\n**Feel free to join us again through this link:** https://whop.com/gold-pioneer",
            7:
            "It's been a week since your Premium Signals trial ended. Since then, our **Gold Pioneers  have been catching trade setups daily in <#1350929852299214999>**.\n\nIf you found value in just 24 hours, imagine the results you could be seeing by now with full access. It's all about **consistency and staying plugged into the right information**.\n\nWe'd like to **personally invite you to rejoin Premium Signals** and get back into the rhythm.\n\n\n**Feel free to join us again through this link:** https://whop.com/gold-pioneer",
            14:
            "Hey! It's been two weeks since your access to Premium Signals ended. We hope you've stayed active. \n\nIf you've been trading solo or passively following the free channel, you might be feeling the difference. in <#1350929852299214999>, it's not just about more signals. It's about the **structure, support, and smarter decision-making**. That edge can make all the difference over time.\n\nWe'd love to **officially invite you back into Premium Signals** and help you start compounding results again.\n\n**Feel free to join us again through this link:** https://whop.com/gold-pioneer"
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


    def calculate_live_tracking_levels(self, live_price: float, pair: str, action: str):
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
            print(f"🔒 Setting up command permissions for owner: {owner_id}")
            
            # Get all commands
            commands_to_setup = []
            
            # Get all individual commands
            for cmd in self.tree.get_commands():
                if hasattr(cmd, 'name') and hasattr(cmd, 'default_permissions'):
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
                                {
                                    discord.Object(id=owner_id): True
                                },
                                sync=False  # Don't sync immediately
                            )
                            permissions_set += 1
                        except Exception as cmd_err:
                            print(f"   ⚠️ Failed to set permission for {command.name}: {cmd_err}")
                    
                    # Also set permissions for giveaway group
                    try:
                        await giveaway_group.set_permissions(
                            guild,
                            {
                                discord.Object(id=owner_id): True
                            },
                            sync=False
                        )
                        permissions_set += len(giveaway_group.commands)
                    except Exception as group_err:
                        print(f"   ⚠️ Failed to set giveaway group permissions: {group_err}")
                    
                    # Sync permissions for this guild
                    await self.tree.sync(guild=guild)
                    print(f"   ✅ Set permissions for commands in {guild.name}")
                    
                except Exception as guild_err:
                    print(f"   ❌ Failed to set permissions in {guild.name}: {guild_err}")
            
            print(f"✅ Owner permissions setup complete! {permissions_set} command permissions set")
            
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
            ephemeral=True
        )
        return False
    return True


@bot.tree.command(
    name="timedautorole",
    description="Configure timed auto-role for new members"
)
@app_commands.describe(
    action=
    "Enable/disable, check status, list active members, add user manually, or remove user",
    role="Role to assign to new members (required when enabling)",
    user=
    "User to add/remove manually (required for adduser/removeuser actions)",
    timing=
    "Timing type for manual add: 24hours, weekend, or custom (required for adduser action)",
    custom_hours="Custom hours for role duration (used with timing=custom)",
    custom_minutes="Custom minutes for role duration (used with timing=custom)"
)
async def timed_auto_role_command(interaction: discord.Interaction,
                                  action: str,
                                  role: discord.Role | None = None,
                                  user: discord.Member | None = None,
                                  timing: str | None = None,
                                  custom_hours: int | None = None,
                                  custom_minutes: int | None = None):
    """Configure the timed auto-role system with fixed 24-hour duration"""

    # Check if user is bot owner
    if not await owner_check(interaction):
        return

    try:
        if action.lower() == "enable":
            if not role:
                await interaction.response.send_message(
                    "❌ You must specify a role when enabling auto-role.",
                    ephemeral=True)
                return

            # Check if bot has permission to manage the role
            if interaction.guild and interaction.guild.me and role >= interaction.guild.me.top_role:
                await interaction.response.send_message(
                    f"❌ I cannot manage the role '{role.name}' because it's higher than my highest role.",
                    ephemeral=True)
                return

            # Update configuration (duration is fixed at 24 hours)
            AUTO_ROLE_CONFIG["enabled"] = True
            AUTO_ROLE_CONFIG["role_id"] = role.id
            AUTO_ROLE_CONFIG["duration_hours"] = 24  # Fixed duration

            # Save configuration
            await bot.save_auto_role_config()

            await interaction.response.send_message(
                f"✅ **Auto-role system enabled!**\n"
                f"• **Role:** {role.mention}\n"
                f"• **Duration:** 24 hours (fixed)\n"
                f"• **Weekend handling:** Enabled (24h countdown starts Monday, ends Tuesday)\n\n"
                f"New members will automatically receive this role for 24 hours.",
                ephemeral=True)

        elif action.lower() == "disable":
            AUTO_ROLE_CONFIG["enabled"] = False
            await bot.save_auto_role_config()

            await interaction.response.send_message(
                "✅ Auto-role system disabled. No new roles will be assigned to new members.",
                ephemeral=True)

        elif action.lower() == "status":
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
                status_message += f"• **Duration:** 24 hours (fixed)\n"
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

            # Build the list of active members with precise time remaining
            member_list = []

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
                        member_list.append(
                            f"• {member.display_name} - {time_display}")

                except Exception as e:
                    print(f"Error processing member {member_id}: {str(e)}")
                    continue

            if not member_list:
                await interaction.response.send_message(
                    "📝 No valid members found with temporary roles.",
                    ephemeral=True)
                return

            # Create the response message
            role = interaction.guild.get_role(
                AUTO_ROLE_CONFIG["role_id"]
            ) if interaction.guild and AUTO_ROLE_CONFIG["role_id"] else None
            role_name = role.name if role else "Unknown Role"

            list_message = f"📋 **Active Temporary Role Members**\n"
            list_message += f"**Role:** {role_name}\n"
            list_message += f"**Duration:** 24 hours (fixed)\n\n"
            list_message += "\n".join(
                member_list[:20]
            )  # Limit to 20 members to avoid message length issues

            if len(member_list) > 20:
                list_message += f"\n\n*...and {len(member_list) - 20} more members*"

            await interaction.response.send_message(list_message,
                                                    ephemeral=True)

        elif action.lower() == "adduser":
            if not AUTO_ROLE_CONFIG["enabled"]:
                await interaction.response.send_message(
                    "❌ Auto-role system is disabled. Enable it first before adding users manually.",
                    ephemeral=True)
                return

            if not user:
                await interaction.response.send_message(
                    "❌ You must specify a user when using the adduser action.",
                    ephemeral=True)
                return

            if not timing or timing.lower() not in [
                    "24hours", "weekend", "custom"
            ]:
                await interaction.response.send_message(
                    "❌ You must specify timing: '24hours', 'weekend', or 'custom'.",
                    ephemeral=True)
                return

            if timing.lower() == "custom":
                if custom_hours is None and custom_minutes is None:
                    await interaction.response.send_message(
                        "❌ You must specify custom_hours and/or custom_minutes when using custom timing.",
                        ephemeral=True)
                    return
                if custom_hours is not None and (
                        custom_hours < 0 or custom_hours > 168):  # Max 1 week
                    await interaction.response.send_message(
                        "❌ Custom hours must be between 0 and 168 (1 week maximum).",
                        ephemeral=True)
                    return
                if custom_minutes is not None and (custom_minutes < 0
                                                   or custom_minutes > 59):
                    await interaction.response.send_message(
                        "❌ Custom minutes must be between 0 and 59.",
                        ephemeral=True)
                    return

            # Get the configured role
            target_role = interaction.guild.get_role(
                AUTO_ROLE_CONFIG["role_id"]) if interaction.guild else None
            if not target_role:
                await interaction.response.send_message(
                    "❌ Auto-role is not properly configured. No valid role found.",
                    ephemeral=True)
                return

            # Manual adduser bypasses anti-abuse system (admin exception)
            user_id_str = str(user.id)

            # Check if user already has the role or is already tracked
            if user_id_str in AUTO_ROLE_CONFIG["active_members"]:
                await interaction.response.send_message(
                    f"❌ {user.display_name} already has an active temporary role.",
                    ephemeral=True)
                return

            if target_role in user.roles:
                await interaction.response.send_message(
                    f"❌ {user.display_name} already has the {target_role.name} role.",
                    ephemeral=True)
                return

            try:
                # Add the role to the user
                await user.add_roles(
                    target_role,
                    reason="Manual addition via /timedautorole adduser")

                now = datetime.now(AMSTERDAM_TZ)

                if timing.lower() == "weekend":
                    # Weekend timing - expires Monday 23:59
                    expiry_time = bot.get_monday_expiry_time(now)

                    AUTO_ROLE_CONFIG["active_members"][user_id_str] = {
                        "role_added_time": now.isoformat(),
                        "role_id": target_role.id,
                        "guild_id": interaction.guild.id,
                        "weekend_delayed": True,
                        "expiry_time": expiry_time.isoformat()
                    }

                    # Record in role history for anti-abuse
                    AUTO_ROLE_CONFIG["role_history"][user_id_str] = {
                        "first_granted": now.isoformat(),
                        "times_granted": 1,
                        "last_expired": None,
                        "guild_id": interaction.guild.id
                    }

                    timing_info = f"Weekend timing (expires Monday 23:59)"

                elif timing.lower() == "custom":
                    # Custom timing
                    hours = custom_hours or 0
                    minutes = custom_minutes or 0
                    total_minutes = (hours * 60) + minutes

                    if total_minutes == 0:
                        await interaction.response.send_message(
                            "❌ Custom duration cannot be 0. Please specify at least 1 minute.",
                            ephemeral=True)
                        return

                    expiry_time = now + timedelta(hours=hours, minutes=minutes)

                    AUTO_ROLE_CONFIG["active_members"][user_id_str] = {
                        "role_added_time": now.isoformat(),
                        "role_id": target_role.id,
                        "guild_id": interaction.guild.id,
                        "weekend_delayed":
                        True,  # Use weekend logic for custom timing
                        "expiry_time": expiry_time.isoformat(),
                        "custom_duration": True
                    }

                    # Record in role history for anti-abuse
                    AUTO_ROLE_CONFIG["role_history"][user_id_str] = {
                        "first_granted": now.isoformat(),
                        "times_granted": 1,
                        "last_expired": None,
                        "guild_id": interaction.guild.id
                    }

                    duration_text = []
                    if hours > 0:
                        duration_text.append(f"{hours}h")
                    if minutes > 0:
                        duration_text.append(f"{minutes}m")

                    timing_info = f"Custom: {' '.join(duration_text)} (expires {expiry_time.strftime('%A %H:%M')})"

                else:
                    # 24-hour timing
                    AUTO_ROLE_CONFIG["active_members"][user_id_str] = {
                        "role_added_time": now.isoformat(),
                        "role_id": target_role.id,
                        "guild_id": interaction.guild.id,
                        "weekend_delayed": False
                    }

                    # Record in role history for anti-abuse
                    AUTO_ROLE_CONFIG["role_history"][user_id_str] = {
                        "first_granted": now.isoformat(),
                        "times_granted": 1,
                        "last_expired": None,
                        "guild_id": interaction.guild.id
                    }

                    timing_info = f"24 hours (expires {(now + timedelta(hours=24)).strftime('%A %H:%M')})"

                # Save configuration
                await bot.save_auto_role_config()

                await interaction.response.send_message(
                    f"✅ **Successfully added {user.display_name} to temporary role**\n"
                    f"• **Role:** {target_role.name}\n"
                    f"• **Duration:** {timing_info}\n"
                    f"• **Added by:** {interaction.user.display_name}",
                    ephemeral=True)

            except discord.Forbidden:
                await interaction.response.send_message(
                    f"❌ I don't have permission to add the {target_role.name} role to {user.display_name}.",
                    ephemeral=True)
            except Exception as e:
                await interaction.response.send_message(
                    f"❌ Error adding role to {user.display_name}: {str(e)}",
                    ephemeral=True)

        elif action.lower() == "removeuser":
            if not user:
                await interaction.response.send_message(
                    "❌ You must specify a user when using the removeuser action.",
                    ephemeral=True)
                return

            # Check if user is tracked in the system
            if str(user.id) not in AUTO_ROLE_CONFIG["active_members"]:
                await interaction.response.send_message(
                    f"❌ {user.display_name} is not currently tracked in the auto-role system.",
                    ephemeral=True)
                return

            try:
                # Get the role info before removing
                user_data = AUTO_ROLE_CONFIG["active_members"][str(user.id)]
                role_id = user_data.get("role_id")
                target_role = interaction.guild.get_role(
                    role_id) if interaction.guild and role_id else None

                # Remove from tracking
                del AUTO_ROLE_CONFIG["active_members"][str(user.id)]

                # Remove the role if they still have it
                if target_role and target_role in user.roles:
                    await user.remove_roles(
                        target_role,
                        reason="Manual removal via /timedautorole removeuser")
                    role_removed_msg = f"• **Role removed:** {target_role.name}"
                else:
                    role_removed_msg = "• **Role status:** Already removed or not found"

                # Save configuration
                await bot.save_auto_role_config()

                await interaction.response.send_message(
                    f"✅ **Successfully removed {user.display_name} from auto-role system**\n"
                    f"{role_removed_msg}\n"
                    f"• **Removed by:** {interaction.user.display_name}",
                    ephemeral=True)

            except discord.Forbidden:
                # Still remove from tracking even if we can't remove the role
                del AUTO_ROLE_CONFIG["active_members"][str(user.id)]
                await bot.save_auto_role_config()

                await interaction.response.send_message(
                    f"⚠️ **Removed {user.display_name} from tracking** but couldn't remove role due to permissions.\n"
                    f"• **Removed by:** {interaction.user.display_name}",
                    ephemeral=True)
            except Exception as e:
                await interaction.response.send_message(
                    f"❌ Error removing {user.display_name}: {str(e)}",
                    ephemeral=True)

        else:
            await interaction.response.send_message(
                "❌ Invalid action. Use 'enable', 'disable', 'status', 'list', 'adduser', or 'removeuser'.",
                ephemeral=True)

    except Exception as e:
        await interaction.response.send_message(
            f"❌ Error configuring auto-role: {str(e)}", ephemeral=True)


@timed_auto_role_command.autocomplete('action')
async def action_autocomplete(interaction: discord.Interaction, current: str):
    actions = ['enable', 'disable', 'status', 'list', 'adduser', 'removeuser']
    return [
        app_commands.Choice(name=action, value=action) for action in actions
        if current.lower() in action.lower()
    ]


@timed_auto_role_command.autocomplete('timing')
async def timing_autocomplete(interaction: discord.Interaction, current: str):
    timings = ['24hours', 'weekend', 'custom']
    return [
        app_commands.Choice(name=timing, value=timing) for timing in timings
        if current.lower() in timing.lower()
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
@bot.tree.command(name="level", description="Check level information for yourself or another user, or view leaderboard")
@app_commands.describe(
    user="User to check level for (leave empty to check your own level)",
    show_leaderboard="Show server leaderboard instead of individual level"
)
async def level_command(interaction: discord.Interaction, user: discord.Member = None, show_leaderboard: bool = False):
    """Check level information or display leaderboard"""
    
    if not await owner_check(interaction):
        return
    
    # If leaderboard is requested, show top users
    if show_leaderboard:
        await interaction.response.defer(ephemeral=True)
        
        if not LEVEL_SYSTEM["user_data"]:
            await interaction.followup.send("📊 No level data available yet. Users need to send messages to start leveling up!", ephemeral=True)
            return
        
        # Get guild members and filter level data to current guild
        guild_users = []
        for user_id, data in LEVEL_SYSTEM["user_data"].items():
            if data.get("guild_id") == interaction.guild.id:
                guild_users.append((user_id, data))
        
        # Sort by level (descending), then by message count (descending)
        guild_users.sort(key=lambda x: (x[1]["current_level"], x[1]["message_count"]), reverse=True)
        
        # Create leaderboard embed
        embed = discord.Embed(
            title=f"🏆 {interaction.guild.name} Level Leaderboard",
            description="Top active community members ranked by level and messages",
            color=discord.Color.gold()
        )
        
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
                print(f"Error processing leaderboard entry for user {user_id}: {e}")
                continue
        
        if leaderboard_text:
            embed.add_field(
                name="📈 Top Community Members",
                value=leaderboard_text,
                inline=False
            )
            
            # Add server stats
            total_users = len(guild_users)
            total_messages = sum(data["message_count"] for _, data in guild_users)
            avg_level = sum(data["current_level"] for _, data in guild_users) / total_users if total_users > 0 else 0
            
            embed.add_field(
                name="📊 Server Statistics",
                value=f"• **Total Active Users**: {total_users:,}\n" +
                      f"• **Total Messages**: {total_messages:,}\n" +
                      f"• **Average Level**: {avg_level:.1f}",
                inline=False
            )
        else:
            embed.add_field(
                name="📊 Leaderboard",
                value="No active users found in this server.",
                inline=False
            )
        
        embed.set_footer(text="Use /level without leaderboard option to check your individual progress")
        await interaction.followup.send(embed=embed, ephemeral=True)
        return
    
    # Individual level check (original functionality)
    target_user = user or interaction.user
    user_id = str(target_user.id)
    
    if user_id not in LEVEL_SYSTEM["user_data"]:
        await interaction.response.send_message(
            f"📊 **{target_user.display_name}** has not sent any messages yet.\n" +
            "Start chatting to begin leveling up!",
            ephemeral=True
        )
        return
    
    user_data = LEVEL_SYSTEM["user_data"][user_id]
    current_level = user_data["current_level"]
    message_count = user_data["message_count"]
    
    # Calculate progress to next level
    next_level = current_level + 1
    if next_level <= 8:  # Max level is 8
        messages_needed = LEVEL_SYSTEM["level_requirements"][next_level]
        progress = message_count
        remaining = messages_needed - message_count
        progress_percentage = (progress / messages_needed) * 100
    else:
        # Max level reached
        messages_needed = None
        remaining = 0
        progress_percentage = 100
    
    # Create level embed
    embed = discord.Embed(
        title=f"📊 Level Information for {target_user.display_name}",
        color=discord.Color.gold()
    )
    
    if current_level > 0:
        embed.add_field(
            name="🏆 Current Level",
            value=f"**Level {current_level}**",
            inline=True
        )
    else:
        embed.add_field(
            name="🏆 Current Level", 
            value="**Not yet achieved**",
            inline=True
        )
    
    embed.add_field(
        name="💬 Total Messages",
        value=f"**{message_count:,}**",
        inline=True
    )
    
    if next_level <= 8:
        embed.add_field(
            name="🎯 Next Level Progress",
            value=f"**{progress}/{messages_needed}** messages\n" +
                  f"**{remaining:,}** messages to Level {next_level}\n" +
                  f"**{progress_percentage:.1f}%** complete",
            inline=False
        )
    else:
        embed.add_field(
            name="🎉 Achievement",
            value="**MAX LEVEL REACHED!**\nCongratulations on reaching Level 8!",
            inline=False
        )
    
    # Add level requirements info
    requirements_text = ""
    for level, required in LEVEL_SYSTEM["level_requirements"].items():
        if level <= current_level:
            requirements_text += f"✅ Level {level}: {required:,} messages\n"
        elif level == current_level + 1:
            requirements_text += f"🎯 Level {level}: {required:,} messages\n"
        else:
            requirements_text += f"🔒 Level {level}: {required:,} messages\n"
    
    embed.add_field(
        name="📋 Level Requirements",
        value=requirements_text,
        inline=False
    )
    
    embed.set_footer(text="Keep chatting in any text channel to level up!")
    
    await interaction.response.send_message(embed=embed, ephemeral=True)


# Unified Giveaway Command
@bot.tree.command(name="giveaway", description="Complete giveaway management")
@app_commands.describe(
    action="What do you want to do with giveaways?",
    message="[CREATE] Custom giveaway message explaining what it's for",
    required_role="[CREATE] Role required to enter the giveaway", 
    winners="[CREATE] Number of users that can win the giveaway",
    weeks="[CREATE] Number of weeks for giveaway duration",
    days="[CREATE] Number of days for giveaway duration",
    hours="[CREATE] Number of hours for giveaway duration", 
    minutes="[CREATE] Number of minutes for giveaway duration",
    giveaway_id="[SELECT/END] ID of the giveaway (use action=list to see active ones)",
    user="[SELECT]"
)
async def giveaway_command(interaction: discord.Interaction,
                          action: str,
                          message: str = None,
                          required_role: discord.Role = None,
                          winners: int = 1,
                          weeks: int = 0,
                          days: int = 0, 
                          hours: int = 0,
                          minutes: int = 0,
                          giveaway_id: str = None,
                          user: discord.Member = None):
    """Unified giveaway management command"""
    
    # Owner check for security
    if not await owner_check(interaction):
        return
    
    try:
        if action == "create":
            # Validate required parameters for create
            if not message:
                await interaction.response.send_message(
                    "❌ **Message is required** for creating giveaways.\n" +
                    "Example: `message: Win a $100 Amazon gift card!`",
                    ephemeral=True
                )
                return
                
            if not required_role:
                await interaction.response.send_message(
                    "❌ **Required role is required** for creating giveaways.\n" +
                    "Example: `required_role: @Active Members`",
                    ephemeral=True
                )
                return
            
            if winners <= 0:
                await interaction.response.send_message(
                    "❌ Winner count must be greater than 0.",
                    ephemeral=True
                )
                return
            
            total_minutes = weeks * 7 * 24 * 60 + days * 24 * 60 + hours * 60 + minutes
            if total_minutes <= 0:
                await interaction.response.send_message(
                    "❌ You must set a duration greater than 0.\n" +
                    "Use `weeks`, `days`, `hours`, and/or `minutes` parameters.",
                    ephemeral=True
                )
                return
            
            # Create giveaway settings
            settings = {
                'message': message,
                'role': required_role,
                'winners': winners,
                'duration': {
                    'weeks': weeks,
                    'days': days,
                    'hours': hours,
                    'minutes': minutes,
                    'total_minutes': total_minutes
                }
            }
            
            await interaction.response.send_message("🎉 Creating your giveaway...", ephemeral=True)
            await create_giveaway(interaction, settings)
            
        elif action == "list":
            # List all active giveaways
            if not ACTIVE_GIVEAWAYS:
                await interaction.response.send_message(
                    "📋 **No active giveaways found.**",
                    ephemeral=True
                )
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
                    minutes_left = int((time_left.total_seconds() % 3600) // 60)
                    chosen_count = len(data.get('chosen_winners', []))
                    
                    giveaway_list.append(
                        f"**{gid}**\n" +
                        f"  ⏰ Time left: {hours_left}h {minutes_left}m\n" +
                        f"  🏆 Winners: {data['winner_count']}\n" +
                        f"  🎯 Guaranteed: {chosen_count}/{data['winner_count']}\n"
                    )
            
            if not giveaway_list:
                await interaction.response.send_message(
                    "📋 **No active giveaways found.**",
                    ephemeral=True
                )
                return
            
            await interaction.response.send_message(
                "📋 **Active Giveaways:**\n\n" + "\n".join(giveaway_list),
                ephemeral=True
            )
            
        elif action == "choose_winner":
            # Guarantee a specific user as winner
            if not giveaway_id:
                await interaction.response.send_message(
                    "❌ **Giveaway ID is required** for choosing winners.\n" +
                    "Use `action: list` first to see active giveaway IDs.",
                    ephemeral=True
                )
                return
                
            if not user:
                await interaction.response.send_message(
                    "❌ **User is required** for choosing winners.\n" +
                    "Example: `user: @JohnDoe`",
                    ephemeral=True
                )
                return
            
            if giveaway_id not in ACTIVE_GIVEAWAYS:
                await interaction.response.send_message(
                    f"❌ Giveaway `{giveaway_id}` not found.\n" +
                    f"Use `action: list` to see active giveaways.",
                    ephemeral=True
                )
                return
            
            # Add chosen winner
            if 'chosen_winners' not in ACTIVE_GIVEAWAYS[giveaway_id]:
                ACTIVE_GIVEAWAYS[giveaway_id]['chosen_winners'] = []
            
            if user.id not in ACTIVE_GIVEAWAYS[giveaway_id]['chosen_winners']:
                # Check if we're not exceeding winner limit
                current_chosen = len(ACTIVE_GIVEAWAYS[giveaway_id]['chosen_winners'])
                max_winners = ACTIVE_GIVEAWAYS[giveaway_id]['winner_count']
                
                if current_chosen >= max_winners:
                    await interaction.response.send_message(
                        f"❌ Cannot add more guaranteed winners.\n" +
                        f"This giveaway already has {current_chosen} guaranteed winner(s) and the max is {max_winners}.",
                        ephemeral=True
                    )
                    return
                
                ACTIVE_GIVEAWAYS[giveaway_id]['chosen_winners'].append(user.id)
                await interaction.response.send_message(
                    f"✅ **{user.mention} has been guaranteed as a winner** for giveaway `{giveaway_id}`!\n" +
                    f"Guaranteed winners: {len(ACTIVE_GIVEAWAYS[giveaway_id]['chosen_winners'])}/{max_winners}",
                    ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    f"❌ {user.mention} is already guaranteed as a winner for this giveaway.",
                    ephemeral=True
                )
                
        elif action == "end":
            # End a giveaway early
            if not giveaway_id:
                await interaction.response.send_message(
                    "❌ **Giveaway ID is required** for ending giveaways.\n" +
                    "Use `action: list` first to see active giveaway IDs.",
                    ephemeral=True
                )
                return
            
            if giveaway_id not in ACTIVE_GIVEAWAYS:
                await interaction.response.send_message(
                    f"❌ Giveaway `{giveaway_id}` not found.\n" +
                    f"Use `action: list` to see active giveaways.",
                    ephemeral=True
                )
                return
            
            await interaction.response.send_message(f"🏁 Ending giveaway `{giveaway_id}`...", ephemeral=True)
            await end_giveaway(giveaway_id, interaction)
            
        else:
            await interaction.response.send_message(
                "❌ **Invalid action!**\n\n" +
                "**Available actions:**\n" +
                "• `create` - Create a new giveaway\n" +
                "• `list` - List all active giveaways\n" +
                "• `choose_winner` - Guarantee a specific user as winner\n" +
                "• `end` - End a giveaway early and select winners",
                ephemeral=True
            )
            
    except Exception as e:
        await interaction.response.send_message(
            f"❌ Error in giveaway command: {str(e)}",
            ephemeral=True
        )

# Autocomplete for giveaway actions
@giveaway_command.autocomplete('action')
async def giveaway_action_autocomplete(interaction: discord.Interaction, current: str):
    actions = [
        app_commands.Choice(name="📝 Create", value="create"),
        app_commands.Choice(name="📋 List", value="list"), 
        app_commands.Choice(name="🎯 Select", value="choose_winner"),
        app_commands.Choice(name="🏁 End", value="end")
    ]
    return [choice for choice in actions if current.lower() in choice.name.lower()]


async def create_giveaway(interaction, settings):
    """Create and post the actual giveaway"""
    try:
        # Generate unique giveaway ID
        giveaway_id = f"giveaway_{int(datetime.now().timestamp())}"
        
        # Calculate end time
        end_time = datetime.now(AMSTERDAM_TZ) + timedelta(minutes=settings['duration']['total_minutes'])
        
        # Create duration text
        duration = settings['duration']
        duration_parts = []
        if duration['weeks'] > 0:
            duration_parts.append(f"{duration['weeks']} week{'s' if duration['weeks'] != 1 else ''}")
        if duration['days'] > 0:
            duration_parts.append(f"{duration['days']} day{'s' if duration['days'] != 1 else ''}")
        if duration['hours'] > 0:
            duration_parts.append(f"{duration['hours']} hour{'s' if duration['hours'] != 1 else ''}")
        if duration['minutes'] > 0:
            duration_parts.append(f"{duration['minutes']} minute{'s' if duration['minutes'] != 1 else ''}")
        
        duration_text = ", ".join(duration_parts)
        
        # Create the giveaway embed
        embed = discord.Embed(
            title="🎉 **GIVEAWAY** 🎉",
            description=settings['message'],
            color=discord.Color.gold(),
            timestamp=end_time
        )
        
        embed.add_field(
            name="⏰ Duration",
            value=duration_text,
            inline=True
        )
        
        embed.add_field(
            name="🏆 Winners",
            value=f"{settings['winners']} winner{'s' if settings['winners'] != 1 else ''}",
            inline=True
        )
        

        
        embed.add_field(
            name="🎪 How to Enter",
            value="React with 🎉 to this message to enter!",
            inline=False
        )
        
        embed.add_field(
            name="📋 Requirements",
            value=f"**The required rank to enter this giveaway is: {settings['role'].mention}**",
            inline=False
        )
        
        embed.set_footer(text="Ends at")
        
        # Get the giveaway channel
        giveaway_channel = bot.get_channel(GIVEAWAY_CHANNEL_ID)
        if not giveaway_channel:
            await interaction.followup.send(
                f"❌ Could not find giveaway channel with ID {GIVEAWAY_CHANNEL_ID}",
                ephemeral=True
            )
            return
        
        # Create the message content with @everyone at the bottom
        message_content = "@everyone"
        
        # Send the giveaway message
        message = await giveaway_channel.send(content=message_content, embed=embed)
        await message.add_reaction("🎉")
        
        # Store giveaway data
        ACTIVE_GIVEAWAYS[giveaway_id] = {
            'message_id': message.id,
            'channel_id': GIVEAWAY_CHANNEL_ID,
            'creator_id': interaction.user.id,
            'required_role_id': settings['role'].id,
            'winner_count': settings['winners'],
            'end_time': end_time,
            'participants': [],
            'chosen_winners': [],
            'settings': settings
        }
        
        # Log to bot log channel with giveaway ID
        await bot.log_to_discord(
            f"**Giveaway Created** 🎉\n"
            f"ID: `{giveaway_id}`\n"
            f"Message: {settings['message'][:100]}{'...' if len(settings['message']) > 100 else ''}\n"
            f"Winners: {settings['winners']}\n"
            f"Duration: {duration_text}\n"
            f"Required Role: {settings['role'].mention}\n"
            f"Creator: {interaction.user.mention}"
        )
        
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
            ephemeral=True
        )
        
    except Exception as e:
        await interaction.followup.send(
            f"❌ Error creating giveaway: {str(e)}",
            ephemeral=True
        )


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
                await interaction.followup.send("❌ Giveaway not found.", ephemeral=True)
            return
        
        giveaway_data = ACTIVE_GIVEAWAYS[giveaway_id]
        
        # Get the message
        channel = bot.get_channel(giveaway_data['channel_id'])
        if not channel:
            if interaction:
                await interaction.followup.send("❌ Could not find giveaway channel.", ephemeral=True)
            return
        
        try:
            message = await channel.fetch_message(giveaway_data['message_id'])
        except discord.NotFound:
            if interaction:
                await interaction.followup.send("❌ Giveaway message not found.", ephemeral=True)
            return
        
        # Get all participants who reacted with 🎉
        valid_participants = []
        required_role = channel.guild.get_role(giveaway_data['required_role_id'])
        
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
                min(remaining_slots, len(valid_participants))
            )
            final_winners.extend(additional_winners)
        
        # Create winner announcement
        embed = discord.Embed(
            title="🎉 **GIVEAWAY ENDED** 🎉",
            color=discord.Color.green(),
            timestamp=datetime.now(AMSTERDAM_TZ)
        )
        
        if final_winners:
            winner_mentions = [winner.mention for winner in final_winners]
            embed.add_field(
                name="🏆 Winners",
                value="\n".join(winner_mentions),
                inline=False
            )
            
            embed.add_field(
                name="📊 Stats",
                value=f"Total Participants: {len(valid_participants) + len(final_winners)}\nWinners Selected: {len(final_winners)}",
                inline=False
            )
        else:
            embed.add_field(
                name="😔 No Winners",
                value="No valid participants found or no one had the required role.",
                inline=False
            )
        

        
        embed.set_footer(text="Ended at")
        
        # Send winner announcement
        await channel.send(embed=embed)
        
        # Log to bot log channel with giveaway ID
        if final_winners:
            winner_names = [winner.display_name for winner in final_winners]
            await bot.log_to_discord(
                f"**Giveaway Ended** 🏆\n"
                f"ID: `{giveaway_id}`\n"
                f"Winners: {', '.join(winner_names)}\n"
                f"Total Participants: {len(valid_participants) + len(final_winners)}\n"
                f"Channel: {channel.mention}"
            )
        else:
            await bot.log_to_discord(
                f"**Giveaway Ended** 😔\n"
                f"ID: `{giveaway_id}`\n"
                f"No Winners - No valid participants found\n"
                f"Channel: {channel.mention}"
            )
        
        # Remove from active giveaways
        del ACTIVE_GIVEAWAYS[giveaway_id]
        
        if interaction:
            await interaction.followup.send(
                f"✅ Giveaway `{giveaway_id}` ended successfully!\n" +
                f"🏆 Winners: {len(final_winners)}"
            )
        
    except Exception as e:
        print(f"Error ending giveaway: {e}")
        if interaction:
            await interaction.followup.send(f"❌ Error ending giveaway: {str(e)}", ephemeral=True)


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
        return
    
    giveaway_data = ACTIVE_GIVEAWAYS[giveaway_id]
    
    # Check if user has required role
    required_role = reaction.message.guild.get_role(giveaway_data['required_role_id'])
    member = reaction.message.guild.get_member(user.id)
    
    if not member or not required_role or required_role not in member.roles:
        # Remove their reaction and send DM
        try:
            await reaction.remove(user)
            await user.send(
                "**Unfortunately, your current activity level is not high enough to enter this giveaway. " +
                "You can level up by participating in conversations in any of our text channels.**"
            )
        except (discord.Forbidden, discord.NotFound):
            pass  # Can't DM user or remove reaction
        return


# Stats command removed as per user request

# ===== DM TRACKING COMMAND =====
@bot.tree.command(
    name="dmstatus",
    description="Check which users have received 3, 7, or 14 day follow-up messages"
)
@app_commands.describe(
    message_type="Which message type to check: 3day, 7day, 14day, or all"
)
async def dm_status_command(interaction: discord.Interaction, message_type: str = "all"):
    """Check DM status for timed auto-role follow-up messages"""
    if not await owner_check(interaction):
        return

    try:
        await interaction.response.defer(ephemeral=True)
        
        if not AUTO_ROLE_CONFIG["dm_schedule"]:
            await interaction.followup.send("📬 No DM schedule data found.", ephemeral=True)
            return

        # Filter message types
        valid_types = ["all", "3day", "7day", "14day"]
        if message_type.lower() not in valid_types:
            await interaction.followup.send(f"❌ Invalid message type. Use: {', '.join(valid_types)}", ephemeral=True)
            return

        # Clean up completed users first (those who received 14-day message)
        completed_users = []
        for member_id, dm_data in list(AUTO_ROLE_CONFIG["dm_schedule"].items()):
            if dm_data.get("dm_14_sent", False):
                completed_users.append(member_id)
                
        # Remove completed users from tracking
        for member_id in completed_users:
            del AUTO_ROLE_CONFIG["dm_schedule"][member_id]
            
        # Save updated config if users were removed
        if completed_users:
            await bot.save_auto_role_config()
            print(f"✅ Removed {len(completed_users)} completed users from DM tracking")

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
        await interaction.followup.send(f"❌ Error checking DM status: {str(e)}", ephemeral=True)


# ===== INVITE TRACKING COMMANDS =====
@bot.tree.command(
    name="invitetracking",
    description="Manage server invite tracking and statistics"
)
@app_commands.describe(
    action="Action: list, nickname, stats, or reset",
    invite_code="Invite code (for nickname action)",
    nickname="Nickname for the invite (for nickname action)"
)
async def invite_tracking_command(interaction: discord.Interaction, action: str, invite_code: str = None, nickname: str = None):
    """Manage invite tracking system"""
    if not await owner_check(interaction):
        return

    try:
        await interaction.response.defer(ephemeral=True)
        
        if action.lower() == "list":
            # List all tracked invites
            if not INVITE_TRACKING:
                await interaction.followup.send("📋 No invites are currently being tracked.", ephemeral=True)
                return
            
            report = "📋 **INVITE TRACKING REPORT**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            
            for code, data in INVITE_TRACKING.items():
                nickname_display = data.get("nickname", f"Invite-{code[:8]}")
                report += f"**{nickname_display}** (`{code}`)\n"
                report += f"• Total Joins: **{data.get('total_joins', 0)}**\n"
                report += f"• Total Left: **{data.get('total_left', 0)}**\n"
                report += f"• Current Members: **{data.get('current_members', 0)}**\n"
                report += f"• Creator: <@{data.get('creator_id', 0)}>\n\n"
            
            # Split message if too long
            if len(report) > 2000:
                chunks = [report[i:i+2000] for i in range(0, len(report), 2000)]
                for i, chunk in enumerate(chunks):
                    if i == 0:
                        await interaction.followup.send(chunk, ephemeral=True)
                    else:
                        await interaction.followup.send(chunk, ephemeral=True)
            else:
                await interaction.followup.send(report, ephemeral=True)
                
        elif action.lower() == "nickname":
            # Set nickname for an invite
            if not invite_code or not nickname:
                await interaction.followup.send("❌ Both invite_code and nickname are required for this action.", ephemeral=True)
                return
            
            # Check if invite exists in tracking
            if invite_code not in INVITE_TRACKING:
                # Initialize tracking for this invite
                try:
                    # Try to fetch the invite to get creator info
                    invite = await interaction.guild.fetch_invite(invite_code)
                    INVITE_TRACKING[invite_code] = {
                        "nickname": nickname,
                        "total_joins": 0,
                        "total_left": 0,
                        "current_members": 0,
                        "creator_id": invite.inviter.id if invite.inviter else 0,
                        "guild_id": interaction.guild.id,
                        "created_at": datetime.now(AMSTERDAM_TZ).isoformat(),
                        "last_updated": datetime.now(AMSTERDAM_TZ).isoformat()
                    }
                except discord.NotFound:
                    await interaction.followup.send(f"❌ Invite code `{invite_code}` not found or expired.", ephemeral=True)
                    return
                except Exception as e:
                    await interaction.followup.send(f"❌ Error fetching invite: {str(e)}", ephemeral=True)
                    return
            else:
                # Update existing nickname
                INVITE_TRACKING[invite_code]["nickname"] = nickname
            
            # Save to database
            await bot.save_invite_tracking()
            
            await interaction.followup.send(f"✅ Invite `{invite_code}` nickname set to: **{nickname}**", ephemeral=True)
            
        elif action.lower() == "stats":
            # Show overall statistics
            if not INVITE_TRACKING:
                await interaction.followup.send("📊 No invite statistics available.", ephemeral=True)
                return
            
            total_joins = sum(data.get("total_joins", 0) for data in INVITE_TRACKING.values())
            total_left = sum(data.get("total_left", 0) for data in INVITE_TRACKING.values())
            current_members = sum(data.get("current_members", 0) for data in INVITE_TRACKING.values())
            total_invites = len(INVITE_TRACKING)
            
            retention_rate = ((current_members / total_joins) * 100) if total_joins > 0 else 0
            
            stats_report = f"📊 **INVITE STATISTICS OVERVIEW**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            stats_report += f"**Overall Performance:**\n"
            stats_report += f"• Total Tracked Invites: **{total_invites}**\n"
            stats_report += f"• Total Members Joined: **{total_joins}**\n"
            stats_report += f"• Total Members Left: **{total_left}**\n"
            stats_report += f"• Current Active Members: **{current_members}**\n"
            stats_report += f"• Retention Rate: **{retention_rate:.1f}%**\n\n"
            
            # Top performers
            if INVITE_TRACKING:
                sorted_invites = sorted(INVITE_TRACKING.items(), key=lambda x: x[1].get("total_joins", 0), reverse=True)
                stats_report += f"**Top Performing Invites:**\n"
                for i, (code, data) in enumerate(sorted_invites[:5]):
                    nickname_display = data.get("nickname", f"Invite-{code[:8]}")
                    stats_report += f"{i+1}. **{nickname_display}**: {data.get('total_joins', 0)} joins\n"
            
            await interaction.followup.send(stats_report, ephemeral=True)
            
        elif action.lower() == "reset" and invite_code != "confirm":
            # Reset all invite tracking (with confirmation)
            await interaction.followup.send(
                f"⚠️ **WARNING**: This will reset ALL invite tracking data!\n"
                f"Currently tracking {len(INVITE_TRACKING)} invites.\n"
                f"Use `/invitetracking reset confirm` to proceed.",
                ephemeral=True
            )
            
        elif action.lower() == "reset" and invite_code == "confirm":
            # Actually reset the data
            INVITE_TRACKING.clear()
            if bot.db_pool:
                async with bot.db_pool.acquire() as conn:
                    await conn.execute('DELETE FROM invite_tracking')
                    await conn.execute('DELETE FROM member_joins')
            
            await interaction.followup.send("✅ All invite tracking data has been reset.", ephemeral=True)
            
        else:
            await interaction.followup.send(
                f"❌ Invalid action. Use: `list`, `nickname`, `stats`, or `reset`",
                ephemeral=True
            )

    except Exception as e:
        await interaction.followup.send(f"❌ Error with invite tracking: {str(e)}", ephemeral=True)


# ===== PRICE TRACKING COMMANDS =====

@bot.tree.command(name="pricetracking", description="[OWNER ONLY] Toggle live price tracking system on/off")
@app_commands.describe(enabled="Enable or disable the price tracking system")
async def toggle_price_tracking(interaction: discord.Interaction, enabled: bool):
    """Toggle price tracking system"""
    if not await owner_check(interaction):
        return
    
    PRICE_TRACKING_CONFIG["enabled"] = enabled
    status = "enabled" if enabled else "disabled"
    
    embed = discord.Embed(
        title="🔄 Price Tracking System",
        description=f"Live price tracking has been **{status}**",
        color=discord.Color.green() if enabled else discord.Color.red()
    )
    
    if enabled:
        embed.add_field(
            name="📊 Configuration",
            value=f"• **Monitoring**: Signals containing '{PRICE_TRACKING_CONFIG['signal_keyword']}'\n"
                  f"• **From**: Owner and bot messages only\n"
                  f"• **Excluding**: Channel ID {PRICE_TRACKING_CONFIG['excluded_channel_id']}\n"
                  f"• **Check Interval**: {PRICE_TRACKING_CONFIG['check_interval']} seconds (3m 45s)",
            inline=False
        )
    
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="activetrades", description="[OWNER ONLY] View detailed status of all tracked trading signals")
async def active_trades(interaction: discord.Interaction):
    """Show active trades with detailed price level analysis"""
    if not await owner_check(interaction):
        return
    
    if not PRICE_TRACKING_CONFIG["enabled"]:
        embed = discord.Embed(
            title="📊 Active Signal Tracking",
            description="❌ Price tracking system is currently disabled",
            color=discord.Color.red()
        )
        await interaction.response.send_message(embed=embed)
        return
    
    # Get active trades from database for 24/7 persistence
    active_trades = await bot.get_active_trades_from_db()
    
    if not active_trades:
        embed = discord.Embed(
            title="📊 Active Signal Tracking",
            description="✅ No trading signals being monitored\n\n*Signals are automatically removed when they hit SL or TP3*",
            color=discord.Color.blue()
        )
        await interaction.response.send_message(embed=embed)
        return
    
    await interaction.response.defer()
    
    embed = discord.Embed(
        title="📊 Active Signal Tracking",
        description=f"Monitoring **{len(active_trades)}** trading signals with live price analysis:",
        color=discord.Color.green()
    )
    
    # Process each trade and get current price status
    for i, (message_id, trade_data) in enumerate(list(active_trades.items())[:8]):  # Limit to 8 for readability
        try:
            # Get current live price
            current_price = await bot.get_live_price(trade_data["pair"])
            
            if current_price:
                # Analyze current position
                status_info = await analyze_trade_position(trade_data, current_price)
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
                value=f"{price_status}{position_text}\n\n{levels_display}\n{tp_status}{breakeven_status}{time_status}",
                inline=False
            )
            
        except Exception as e:
            # Fallback display if price retrieval fails
            embed.add_field(
                name=f"⚠️ {trade_data['pair']} - {trade_data['action']}",
                value=f"**Error getting price data**\nEntry: ${trade_data['entry']}\nStatus: {trade_data.get('status', 'active')}",
                inline=False
            )
    
    if len(active_trades) > 8:
        embed.set_footer(text=f"Showing 8 of {len(active_trades)} active signals • Signals auto-removed after SL/TP3 hit")
    else:
        embed.set_footer(text="Signals automatically removed when SL or TP3 is hit (after notification sent)")
    
    await interaction.followup.send(embed=embed)

async def analyze_trade_position(trade_data: dict, current_price: float) -> dict:
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
            return {"emoji": "🟡", "position": "Below Entry - Potential Loss Zone"}
        elif current_price <= tp1:
            return {"emoji": "🟠", "position": "Between Entry and TP1"}
        elif current_price <= tp2:
            return {"emoji": "🟢", "position": "Between TP1 and TP2 - In Profit"}
        elif current_price <= tp3:
            return {"emoji": "💚", "position": "Between TP2 and TP3 - Strong Profit"}
        else:
            return {"emoji": "🚀", "position": "Above TP3 - Maximum Profit Zone"}
    
    else:  # SELL
        if current_price >= sl:
            return {"emoji": "🔴", "position": "At/Above SL - Stop Loss Level"}
        elif current_price >= entry:
            return {"emoji": "🟡", "position": "Above Entry - Potential Loss Zone"}
        elif current_price >= tp1:
            return {"emoji": "🟠", "position": "Between Entry and TP1"}
        elif current_price >= tp2:
            return {"emoji": "🟢", "position": "Between TP1 and TP2 - In Profit"}
        elif current_price >= tp3:
            return {"emoji": "💚", "position": "Between TP2 and TP3 - Strong Profit"}
        else:
            return {"emoji": "🚀", "position": "Below TP3 - Maximum Profit Zone"}

def build_levels_display(trade_data: dict, current_price: float = None) -> str:
    """Build a visual display of all price levels"""
    action = trade_data["action"]
    entry = trade_data["entry"]
    tp1 = trade_data["tp1"]
    tp2 = trade_data["tp2"]
    tp3 = trade_data["tp3"]
    sl = trade_data["sl"]
    tp_hits = trade_data.get('tp_hits', [])
    
    # Create level indicators
    def get_level_indicator(level_name, price, hit=False):
        hit_marker = "✅" if hit else "⭕"
        return f"{hit_marker} **{level_name}:** ${price:.5f}"
    
    if action == "BUY":
        # For BUY orders: SL < Entry < TP1 < TP2 < TP3
        levels = [
            get_level_indicator("SL", sl),
            get_level_indicator("Entry", entry),
            get_level_indicator("TP1", tp1, "tp1" in tp_hits),
            get_level_indicator("TP2", tp2, "tp2" in tp_hits),
            get_level_indicator("TP3", tp3, "tp3" in tp_hits)
        ]
    else:
        # For SELL orders: TP3 < TP2 < TP1 < Entry < SL
        levels = [
            get_level_indicator("SL", sl),
            get_level_indicator("Entry", entry),
            get_level_indicator("TP1", tp1, "tp1" in tp_hits),
            get_level_indicator("TP2", tp2, "tp2" in tp_hits),
            get_level_indicator("TP3", tp3, "tp3" in tp_hits)
        ]
    
    return "\n".join(levels)

@bot.tree.command(name="pricetest", description="[OWNER ONLY] Test live price retrieval for a trading pair")
@app_commands.describe(pair="Trading pair to test")
@app_commands.autocomplete(pair=pair_autocomplete)
async def test_price_retrieval(interaction: discord.Interaction, pair: str):
    """Test price retrieval for a trading pair"""
    if not await owner_check(interaction):
        return
    
    if not PRICE_TRACKING_CONFIG["enabled"]:
        await interaction.response.send_message("❌ Price tracking system is disabled", ephemeral=True)
        return
    
    await interaction.response.defer()
    
    try:
        price = await bot.get_live_price(pair)
        
        embed = discord.Embed(
            title="💰 Price Test",
            color=discord.Color.green() if price else discord.Color.red()
        )
        
        if price:
            embed.add_field(
                name=f"✅ {pair.upper()}",
                value=f"Current Price: **{price}**",
                inline=False
            )
        else:
            embed.add_field(
                name=f"❌ {pair.upper()}",
                value="Could not retrieve price from any API",
                inline=False
            )
            
        # Show API status
        api_status = []
        for api_name, api_key in PRICE_TRACKING_CONFIG["api_keys"].items():
            status = "✅" if api_key else "❌"
            api_status.append(f"{status} {api_name.replace('_', ' ').title()}")
        
        embed.add_field(
            name="🔑 API Status",
            value="\n".join(api_status),
            inline=False
        )
        
        await interaction.followup.send(embed=embed)
        
    except Exception as e:
        embed = discord.Embed(
            title="💰 Price Test",
            description=f"❌ Error testing price retrieval: {str(e)}",
            color=discord.Color.red()
        )
        await interaction.followup.send(embed=embed)


# ===== ANTI-ABUSE MANAGEMENT COMMAND =====
@bot.tree.command(name="antiabuse", description="[OWNER ONLY] Manage anti-abuse system for auto-roles")
@app_commands.describe(
    action="Action: view, unblock, block, or stats",
    user_id="Discord User ID (for block/unblock actions)",
    reason="Reason for manual block (for block action)"
)
async def anti_abuse_command(interaction: discord.Interaction, action: str, user_id: str = None, reason: str = None):
    """Manage anti-abuse system"""
    if not await owner_check(interaction):
        return
    
    try:
        await interaction.response.defer(ephemeral=True)
        
        if action.lower() == "view":
            # Show blocked users and statistics
            if not AUTO_ROLE_CONFIG["role_history"]:
                await interaction.followup.send("📋 No users in anti-abuse history.", ephemeral=True)
                return
            
            blocked_users = []
            total_users = len(AUTO_ROLE_CONFIG["role_history"])
            
            for uid, data in AUTO_ROLE_CONFIG["role_history"].items():
                if isinstance(data, dict) and "blocked_reason" in data:
                    try:
                        member = interaction.guild.get_member(int(uid))
                        member_name = member.display_name if member else f"User-{uid}"
                        blocked_reason = data["blocked_reason"]
                        blocked_at = data.get("blocked_at", "Unknown")
                        blocked_users.append(f"• **{member_name}** (`{uid}`)\n  └ Reason: {blocked_reason}\n  └ Blocked: {blocked_at[:10]}")
                    except Exception:
                        continue
            
            report = f"🛡️ **Anti-Abuse System Report**\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            report += f"• **Total users in history**: {total_users}\n"
            report += f"• **Currently blocked**: {len(blocked_users)}\n\n"
            
            if blocked_users:
                report += "**🚫 Blocked Users:**\n" + "\n".join(blocked_users[:10])
                if len(blocked_users) > 10:
                    report += f"\n\n*...and {len(blocked_users) - 10} more*"
            else:
                report += "✅ No users currently blocked"
            
            await interaction.followup.send(report, ephemeral=True)
            
        elif action.lower() == "unblock":
            if not user_id:
                await interaction.followup.send("❌ User ID required for unblock action", ephemeral=True)
                return
            
            if user_id in AUTO_ROLE_CONFIG["role_history"]:
                del AUTO_ROLE_CONFIG["role_history"][user_id]
                await bot.save_auto_role_config()
                await interaction.followup.send(f"✅ Unblocked user {user_id} from anti-abuse system", ephemeral=True)
                await bot.log_to_discord(f"🔓 Owner manually unblocked user {user_id} from anti-abuse system")
            else:
                await interaction.followup.send(f"❌ User {user_id} not found in anti-abuse system", ephemeral=True)
                
        elif action.lower() == "block":
            if not user_id or not reason:
                await interaction.followup.send("❌ User ID and reason required for block action", ephemeral=True)
                return
            
            AUTO_ROLE_CONFIG["role_history"][user_id] = {
                "blocked_reason": f"manual_block: {reason}",
                "blocked_by": interaction.user.id,
                "blocked_at": datetime.now(AMSTERDAM_TZ).isoformat()
            }
            await bot.save_auto_role_config()
            await interaction.followup.send(f"✅ Manually blocked user {user_id}: {reason}", ephemeral=True)
            await bot.log_to_discord(f"🔒 Owner manually blocked user {user_id}: {reason}")
            
        elif action.lower() == "stats":
            # Show anti-abuse statistics
            total_history = len(AUTO_ROLE_CONFIG["role_history"])
            blocked_new_accounts = sum(1 for data in AUTO_ROLE_CONFIG["role_history"].values() 
                                     if isinstance(data, dict) and data.get("blocked_reason") == "account_too_new")
            blocked_rapid_joins = sum(1 for data in AUTO_ROLE_CONFIG["role_history"].values() 
                                    if isinstance(data, dict) and data.get("blocked_reason") == "rapid_join_pattern")
            manual_blocks = sum(1 for data in AUTO_ROLE_CONFIG["role_history"].values() 
                              if isinstance(data, dict) and "manual_block" in data.get("blocked_reason", ""))
            
            stats_report = f"📊 **Anti-Abuse Statistics**\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            stats_report += f"• **Total users in history**: {total_history}\n"
            stats_report += f"• **Blocked (new accounts)**: {blocked_new_accounts}\n"
            stats_report += f"• **Blocked (rapid joins)**: {blocked_rapid_joins}\n"
            stats_report += f"• **Manual blocks**: {manual_blocks}\n"
            stats_report += f"• **Normal completions**: {total_history - blocked_new_accounts - blocked_rapid_joins - manual_blocks}\n\n"
            stats_report += f"**🛡️ System Status**: Manual blocking only\n"
            stats_report += f"**📅 Min Account Age**: DISABLED (influencer collabs enabled)\n"
            stats_report += f"**⏱️ Max Joins/Hour**: UNLIMITED (influencer collabs enabled)"
            
            await interaction.followup.send(stats_report, ephemeral=True)
            
        else:
            await interaction.followup.send("❌ Invalid action. Use: view, unblock, block, or stats", ephemeral=True)
    
    except Exception as e:
        await interaction.followup.send(f"❌ Error managing anti-abuse system: {str(e)}", ephemeral=True)


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
            "status": "running",
            "bot_status": bot_status,
            "bot_user": str(bot.user) if bot.user else "Not logged in",
            "bot_id": bot.user.id if bot.user else None,
            "guild_count": guild_count,
            "guild_names": [guild.name for guild in bot.guilds] if bot.is_ready() else [],
            "database_status": database_status,
            "database_details": database_details,
            "uptime": str(datetime.now()),
            "version": "2.2 - Optimized API & Random Messages",
            "last_heartbeat": str(bot.last_heartbeat) if hasattr(bot, 'last_heartbeat') and bot.last_heartbeat else "N/A",
            "bot_latency": f"{round(bot.latency * 1000)}ms" if bot.is_ready() else "N/A",
            "is_ready": bot.is_ready(),
            "is_closed": bot.is_closed(),
            "token_length": len(DISCORD_TOKEN) if DISCORD_TOKEN else 0,
            "intents": str(bot.intents) if hasattr(bot, 'intents') else "N/A"
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
        print(f"Token parts found: PART1={bool(DISCORD_TOKEN_PART1)}, PART2={bool(DISCORD_TOKEN_PART2)}")
        print(f"Assembled token length: {len(DISCORD_TOKEN) if DISCORD_TOKEN else 0}")
        print("Please set DISCORD_TOKEN_PART1 and DISCORD_TOKEN_PART2 environment variables")
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
                print(f"🚀 Starting Discord bot (attempt {attempt + 1}/{max_retries})...")
                print(f"   Using token length: {len(DISCORD_TOKEN)}")
                print(f"   Bot is closed: {bot.is_closed()}")
                
                # Test token format before attempting connection
                if not DISCORD_TOKEN or len(DISCORD_TOKEN) < 50:
                    raise ValueError("Invalid Discord token format or length")
                
                if not DISCORD_TOKEN.count('.') >= 2:
                    raise ValueError("Discord token format invalid - should contain at least 2 dots")
                
                print("   Token format validation passed")
                print("   Attempting Discord connection...")
                
                await bot.start(DISCORD_TOKEN)
                print("✅ Discord bot started successfully!")
                break  # If successful, break out of retry loop
                
            except discord.LoginFailure as e:
                print(f"❌ DISCORD LOGIN FAILURE: {e}")
                print("   This indicates invalid bot token")
                print("   Please verify your Discord bot token is correct")
                print(f"   Token being used starts with: {DISCORD_TOKEN[:20]}...")
                break  # Don't retry on login failures
                
            except discord.HTTPException as e:
                print(f"❌ DISCORD HTTP ERROR: {e}")
                print(f"   Status code: {getattr(e, 'status', 'Unknown')}")
                print(f"   Response: {getattr(e, 'response', 'No response')}")
                
                if e.status == 429:  # Rate limited
                    # Check if this is a Cloudflare rate limit (Error 1015)
                    if "cloudflare" in str(e).lower() or "1015" in str(e):
                        print("   🚨 CLOUDFLARE IP BAN DETECTED (Error 1015)")
                        print("   Your Render server IP is banned by Discord's Cloudflare")
                        print("   This requires a different approach - IP ban won't resolve with waiting")
                        
                        # Log the exact issue for user
                        print("   ❌ CRITICAL: Bot cannot run 24/7 until IP ban is lifted")
                        print("   💡 SOLUTION: Need to change server IP or hosting provider")
                        break  # Don't retry - IP is banned
                    else:
                        print(f"   Normal rate limit. Waiting {retry_delay} seconds before retry...")
                        await asyncio.sleep(retry_delay)
                elif e.status == 401:  # Unauthorized
                    print("   401 Unauthorized - Invalid bot token")
                    break
                elif e.status == 403:  # Forbidden
                    print("   403 Forbidden - Bot may be banned or token invalid")
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
