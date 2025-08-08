import discord
from discord.ext import commands, tasks
from discord import app_commands
import os
from dotenv import load_dotenv
import asyncio
from aiohttp import web
import json
from datetime import datetime, timedelta, timezone
import asyncpg
import logging

# Try to import pytz for proper timezone handling, fallback to basic timezone if not available
try:
    import pytz
    PYTZ_AVAILABLE = True
    print("✅ Pytz loaded - Full timezone support enabled")
except ImportError:
    PYTZ_AVAILABLE = False
    print("⚠️ Pytz not available - Using basic timezone handling")

# Telegram integration
try:
    from pyrogram.client import Client
    from pyrogram import filters
    from pyrogram.types import Message
    TELEGRAM_AVAILABLE = True
    print("Pyrogram loaded - Telegram integration enabled")
except ImportError:
    TELEGRAM_AVAILABLE = False
    print(
        "Pyrogram not available - Install with: pip install pyrogram tgcrypto")

# Load environment variables
load_dotenv()

# Reconstruct tokens from split parts for enhanced security
DISCORD_TOKEN_PART1 = os.getenv("DISCORD_TOKEN_PART1", "")
DISCORD_TOKEN_PART2 = os.getenv("DISCORD_TOKEN_PART2", "")
DISCORD_TOKEN = DISCORD_TOKEN_PART1 + DISCORD_TOKEN_PART2

DISCORD_CLIENT_ID_PART1 = os.getenv("DISCORD_CLIENT_ID_PART1", "")
DISCORD_CLIENT_ID_PART2 = os.getenv("DISCORD_CLIENT_ID_PART2", "")
DISCORD_CLIENT_ID = DISCORD_CLIENT_ID_PART1 + DISCORD_CLIENT_ID_PART2

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
    "weekend_pending": {
    },  # member_id: {"join_time": datetime, "guild_id": guild_id} for weekend joiners
    "role_history": {
    },  # member_id: {"first_granted": datetime, "times_granted": int, "last_expired": datetime, "guild_id": guild_id}
    "dm_schedule": {
    }  # member_id: {"role_expired": datetime, "guild_id": guild_id, "dm_3_sent": bool, "dm_7_sent": bool, "dm_14_sent": bool}
}

# Log channel ID for Discord logging
LOG_CHANNEL_ID = 1350888185487429642

# Gold Pioneer role ID for checking membership before sending follow-up DMs
GOLD_PIONEER_ROLE_ID = 1384489575187091466

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

    async def log_to_discord(self, message):
        """Send log message to Discord channel"""
        if self.log_channel:
            try:
                await self.log_channel.send(f"📋 **Bot Log:** {message}")
            except Exception as e:
                print(f"Failed to send log to Discord: {e}")
        # Always print to console as backup
        print(message)

    async def init_database(self):
        """Initialize database connection and create tables"""
        try:
            # Try multiple possible database environment variables (Render uses different names)
            database_url = os.getenv('DATABASE_URL') or os.getenv('POSTGRES_URL') or os.getenv('POSTGRESQL_URL')
            
            if not database_url:
                print("❌ No database URL found - continuing without persistent memory")
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
                server_settings={'application_name': 'discord-trading-bot'}
            )
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

            print("✅ Database tables initialized")
            
            # Load existing config from database
            await self.load_config_from_db()
            
        except Exception as e:
            print(f"❌ Database initialization failed: {e}")
            print("   Continuing with in-memory storage only (data will be lost on restart)")
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
                config_row = await conn.fetchrow('SELECT * FROM auto_role_config ORDER BY id DESC LIMIT 1')
                if config_row:
                    AUTO_ROLE_CONFIG["enabled"] = config_row['enabled']
                    AUTO_ROLE_CONFIG["role_id"] = config_row['role_id'] 
                    AUTO_ROLE_CONFIG["duration_hours"] = config_row['duration_hours']
                    if config_row['custom_message']:
                        AUTO_ROLE_CONFIG["custom_message"] = config_row['custom_message']

                # Load active members
                active_rows = await conn.fetch('SELECT * FROM active_members')
                for row in active_rows:
                    AUTO_ROLE_CONFIG["active_members"][str(row['member_id'])] = {
                        "role_added_time": row['role_added_time'].isoformat(),
                        "role_id": row['role_id'],
                        "guild_id": row['guild_id'],
                        "weekend_delayed": row['weekend_delayed'],
                        "expiry_time": row['expiry_time'].isoformat() if row['expiry_time'] else None,
                        "custom_duration": row['custom_duration']
                    }
                
                # Load weekend pending
                weekend_rows = await conn.fetch('SELECT * FROM weekend_pending')
                for row in weekend_rows:
                    AUTO_ROLE_CONFIG["weekend_pending"][str(row['member_id'])] = {
                        "join_time": row['join_time'].isoformat(),
                        "guild_id": row['guild_id']
                    }
                
                # Load role history
                history_rows = await conn.fetch('SELECT * FROM role_history')
                for row in history_rows:
                    AUTO_ROLE_CONFIG["role_history"][str(row['member_id'])] = {
                        "first_granted": row['first_granted'].isoformat(),
                        "times_granted": row['times_granted'],
                        "last_expired": row['last_expired'].isoformat() if row['last_expired'] else None,
                        "guild_id": row['guild_id']
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

        # Force sync after bot is ready for better reliability
        self.first_sync_done = False
        
        # Initialize database
        await self.init_database()

    async def on_ready(self):
        print(f'{self.user} has landed!')
        if self.user:
            print(f'Bot ID: {self.user.id}')

        # Force command sync on ready for better reliability
        if not getattr(self, 'first_sync_done', False):
            try:
                synced = await self.tree.sync()
                print(f"🔄 Force synced {len(synced)} command(s) on ready")
                self.first_sync_done = True
            except Exception as e:
                print(f"⚠️ Force sync on ready failed: {e}")

        # Start the role removal task
        if not self.role_removal_task.is_running():
            self.role_removal_task.start()

        # Start the Monday activation notification task
        if not self.weekend_activation_task.is_running():
            self.weekend_activation_task.start()

        # Start the follow-up DM task
        if not self.followup_dm_task.is_running():
            self.followup_dm_task.start()

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

        await self.log_to_discord("⚠️ Telegram integration not configured")

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

            # If we found the invite and it was created by a bot, ignore this member
            if used_invite and used_invite.inviter and used_invite.inviter.bot:
                await self.log_to_discord(
                    f"🤖 Ignoring {member.display_name} - joined via bot invite from {used_invite.inviter.display_name}"
                )
                return

            role = member.guild.get_role(AUTO_ROLE_CONFIG["role_id"])
            if not role:
                await self.log_to_discord(f"❌ Auto-role not found in guild {member.guild.name}")
                return

            # Check if user has already received the role before (anti-abuse system)
            member_id_str = str(member.id)
            if member_id_str in AUTO_ROLE_CONFIG["role_history"]:
                await self.log_to_discord(f"🚫 {member.display_name} has already received auto-role before - access denied (anti-abuse)")
                return

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
                        "**access to the Premium Signals channel for 24 hours.** However, the trading markets are closed right now "
                        "because it's the weekend. We're writing to let you know that your 24 hours will start counting down from "
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

    async def save_auto_role_config(self):
        """Save auto-role configuration to database"""
        if not self.db_pool:
            return  # No database available
            
        try:
            async with self.db_pool.acquire() as conn:
                # Save main config - use upsert with a fixed ID
                await conn.execute('''
                    INSERT INTO auto_role_config (id, enabled, role_id, duration_hours, custom_message)
                    VALUES (1, $1, $2, $3, $4)
                    ON CONFLICT (id) DO UPDATE SET
                        enabled = $1,
                        role_id = $2, 
                        duration_hours = $3,
                        custom_message = $4
                ''', AUTO_ROLE_CONFIG["enabled"], AUTO_ROLE_CONFIG["role_id"], 
                AUTO_ROLE_CONFIG["duration_hours"], AUTO_ROLE_CONFIG["custom_message"])
                
                # Save active members
                await conn.execute('DELETE FROM active_members')
                for member_id, data in AUTO_ROLE_CONFIG["active_members"].items():
                    expiry_time = None
                    if data.get("expiry_time"):
                        expiry_time = datetime.fromisoformat(data["expiry_time"].replace('Z', '+00:00'))
                    
                    await conn.execute('''
                        INSERT INTO active_members 
                        (member_id, role_added_time, role_id, guild_id, weekend_delayed, expiry_time, custom_duration)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ''', int(member_id), datetime.fromisoformat(data["role_added_time"].replace('Z', '+00:00')),
                    data["role_id"], data["guild_id"], data["weekend_delayed"], 
                    expiry_time, data.get("custom_duration", False))
                
                # Save weekend pending
                await conn.execute('DELETE FROM weekend_pending')
                for member_id, data in AUTO_ROLE_CONFIG["weekend_pending"].items():
                    await conn.execute('''
                        INSERT INTO weekend_pending (member_id, join_time, guild_id)
                        VALUES ($1, $2, $3)
                    ''', int(member_id), datetime.fromisoformat(data["join_time"].replace('Z', '+00:00')), 
                    data["guild_id"])
                
                # Save role history using UPSERT to avoid conflicts
                for member_id, data in AUTO_ROLE_CONFIG["role_history"].items():
                    last_expired = None
                    if data.get("last_expired"):
                        last_expired = datetime.fromisoformat(data["last_expired"].replace('Z', '+00:00'))
                        
                    await conn.execute('''
                        INSERT INTO role_history (member_id, first_granted, times_granted, last_expired, guild_id)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (member_id) DO UPDATE SET
                            first_granted = $2,
                            times_granted = $3,
                            last_expired = $4,
                            guild_id = $5
                    ''', int(member_id), datetime.fromisoformat(data["first_granted"].replace('Z', '+00:00')),
                    data["times_granted"], last_expired, data["guild_id"])
                
                # Save DM schedule using UPSERT
                for member_id, data in AUTO_ROLE_CONFIG["dm_schedule"].items():
                    await conn.execute('''
                        INSERT INTO dm_schedule (member_id, role_expired, guild_id, dm_3_sent, dm_7_sent, dm_14_sent)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (member_id) DO UPDATE SET
                            role_expired = $2,
                            guild_id = $3,
                            dm_3_sent = $4,
                            dm_7_sent = $5,
                            dm_14_sent = $6
                    ''', int(member_id), datetime.fromisoformat(data["role_expired"].replace('Z', '+00:00')),
                    data["guild_id"], data["dm_3_sent"], data["dm_7_sent"], data["dm_14_sent"])
                    
        except Exception as e:
            print(f"❌ Error saving to database: {str(e)}")

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
                    await self.send_monday_activation_dm(member_id)
                except Exception as e:
                    await self.log_to_discord(
                        f"❌ Error processing Monday activation for member {member_id}: {str(e)}"
                    )

    async def followup_dm_task(self):
        """Background task to send follow-up DMs after 3, 7, and 14 days"""
        if not AUTO_ROLE_CONFIG["dm_schedule"]:
            return

        current_time = datetime.now(AMSTERDAM_TZ)
        messages_to_send = []

        # Define the follow-up messages
        dm_messages = {
            3: "Hey! It's been 3 days since your **24-hour free access to the Premium Signals channel** ended. We hope you were able to catch good trades with us during that time.\n\nAs you've probably seen, the **free signals channel only gets about 1 signal a day**, while inside **Gold Pioneers**, members receive **8–10 high-quality signals every single day in <#1350929852299214999>**. That means way more chances to profit and grow consistently.\n\nWe'd love to **invite you back to Premium Signals** so you don't miss out on more solid opportunities.\n\n**Feel free to join us again through this link:** https://whop.com/gold-pioneer",
            7: "It's been a week since your Premium Signals trial ended. Since then, our **Gold Pioneers have been catching trade setups daily**.\n\nIf you found value in just 24 hours, imagine the results you could be seeing by now with full access. It's all about **consistency and staying plugged into the right information**.\n\nWe'd like to **personally invite you to rejoin Premium Signals** and get back into the rhythm.\n\n**Feel free to join us again through this link:** https://whop.com/gold-pioneer",
            14: "Hey! It's been two weeks since your access to Premium Signals ended. We hope you've stayed active.\n\nIf you've been trading solo or passively following the free channel, you might be feeling the difference. in <#1350929852299214999>, it's not just about more signals. It's about the **structure, support, and smarter decision-making**. That edge can make all the difference over time.\n\nWe'd love to **officially invite you back into Premium Signals** and help you start compounding results again.\n\n**Feel free to join us again through this link:** https://whop.com/gold-pioneer"
        }

        for member_id, schedule_data in AUTO_ROLE_CONFIG["dm_schedule"].items():
            try:
                expiry_time = datetime.fromisoformat(schedule_data["role_expired"])
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
                                'member_id': member_id,
                                'guild_id': schedule_data['guild_id'],
                                'message': message,
                                'days': days,
                                'sent_key': sent_key
                            })

            except Exception as e:
                await self.log_to_discord(f"❌ Error processing DM schedule for member {member_id}: {str(e)}")

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
                    await self.log_to_discord(f"⏭️ Skipping {msg_data['days']}-day DM for {member.display_name} - already has Gold Pioneer role")
                    # Mark as sent even though we skipped it
                    AUTO_ROLE_CONFIG["dm_schedule"][msg_data['member_id']][msg_data['sent_key']] = True
                    continue

                # Send the follow-up DM
                await member.send(msg_data['message'])
                await self.log_to_discord(f"📬 Sent {msg_data['days']}-day follow-up DM to {member.display_name}")
                
                # Mark as sent
                AUTO_ROLE_CONFIG["dm_schedule"][msg_data['member_id']][msg_data['sent_key']] = True

            except discord.Forbidden:
                await self.log_to_discord(f"⚠️ Could not send {msg_data['days']}-day follow-up DM to member {msg_data['member_id']} (DMs disabled)")
                # Mark as sent to avoid retrying
                AUTO_ROLE_CONFIG["dm_schedule"][msg_data['member_id']][msg_data['sent_key']] = True
            except Exception as e:
                await self.log_to_discord(f"❌ Error sending {msg_data['days']}-day follow-up DM to member {msg_data['member_id']}: {str(e)}")

        # Save config if any changes were made
        if messages_to_send:
            await self.save_auto_role_config()

    @tasks.loop(hours=1)  # Check every hour for follow-up DMs
    async def followup_dm_task(self):
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
                AUTO_ROLE_CONFIG["role_history"][member_id]["last_expired"] = current_time.isoformat()

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
    tp2_pips = 50 * pip_value
    tp3_pips = 100 * pip_value
    sl_pips = 70 * pip_value

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


@bot.tree.command(
    name="timedautorole",
    description="Configure timed auto-role for new members (24h fixed duration)"
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

    # Check permissions
    if not interaction.guild or not interaction.user.guild_permissions.manage_roles:
        await interaction.response.send_message(
            "❌ You need 'Manage Roles' permission to use this command.",
            ephemeral=True)
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
                active_count = len(AUTO_ROLE_CONFIG["active_members"])
                weekend_pending_count = len(
                    AUTO_ROLE_CONFIG.get("weekend_pending", {}))

                status_message = f"✅ **Auto-role system is ENABLED**\n"
                if role:
                    status_message += f"• **Role:** {role.mention}\n"
                else:
                    status_message += f"• **Role:** Not found (ID: {AUTO_ROLE_CONFIG['role_id']})\n"
                status_message += f"• **Duration:** 24 hours (fixed)\n"
                status_message += f"• **Active members:** {active_count}\n"
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

            for member_id, data in AUTO_ROLE_CONFIG["active_members"].items():
                try:
                    # Get member info
                    guild = interaction.guild
                    if not guild:
                        continue

                    member = guild.get_member(int(member_id))
                    if not member:
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


@bot.tree.command(name="entry", description="Create a trading signal entry")
@app_commands.describe(
    entry_type="Type of entry (Long, Short, Long Swing, Short Swing)",
    pair="Trading pair",
    price="Entry price",
    channels="Select channel destination")
async def entry_command(interaction: discord.Interaction, entry_type: str,
                        pair: str, price: float, channels: str):
    """Create and send a trading signal to specified channels"""

    try:
        # Calculate TP and SL levels
        levels = calculate_levels(price, pair, entry_type)

        # Create the signal message
        signal_message = f"""**Trade Signal For: {pair}**
Entry Type: {entry_type}
Entry Price: {levels['entry']}

**Take Profit Levels:**
TP1: {levels['tp1']}
TP2: {levels['tp2']}
TP3: {levels['tp3']}

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
            
            if target_channel and isinstance(target_channel, discord.TextChannel):
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
async def channels_autocomplete(interaction: discord.Interaction, current: str):
    channel_choices = [
        'Free channel',
        'Premium channel', 
        'Both',
        'Testing'
    ]
    return [
        app_commands.Choice(name=choice, value=choice) for choice in channel_choices
        if current.lower() in choice.lower()
    ]


@bot.tree.command(name="dbstatus", description="Check database connection and status")
async def database_status_command(interaction: discord.Interaction):
    """Check database connection status and show database information"""
    
    await interaction.response.defer(ephemeral=True)
    
    if not bot.db_pool:
        embed = discord.Embed(
            title="📊 Database Status",
            description="❌ **Database not configured**\n\nThe bot is running without database persistence.\nMemory-based storage is being used instead.",
            color=discord.Color.orange()
        )
        embed.add_field(
            name="💡 To Enable Database",
            value="Add a PostgreSQL service to your Render deployment and set the DATABASE_URL environment variable.",
            inline=False
        )
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
                color=discord.Color.green()
            )
            
            embed.add_field(
                name="🗄️ PostgreSQL Info",
                value=f"Version: {version.split()[1]}\nServer Time: {current_time.strftime('%Y-%m-%d %H:%M:%S UTC')}",
                inline=True
            )
            
            embed.add_field(
                name="📊 Connection Pool",
                value=f"Pool Size: {pool_size}\nIdle Connections: {pool_idle}\nActive: {pool_size - pool_idle}",
                inline=True
            )
            
            embed.add_field(
                name="📋 Tables",
                value=f"Total Tables: {table_count}",
                inline=True
            )
            
            # Check specific bot tables
            bot_tables = ['role_history', 'active_members', 'weekend_pending', 'dm_schedule', 'auto_role_config']
            existing_tables = []
            
            for table in bot_tables:
                exists = await conn.fetchval("""
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
                    value=f"Created: {len(existing_tables)}/{len(bot_tables)}\n" + "\n".join(f"✅ {table}" for table in existing_tables),
                    inline=False
                )
            
            embed.set_footer(text="Database is functioning properly for persistent memory storage")
            
    except Exception as e:
        embed = discord.Embed(
            title="📊 Database Status",
            description="❌ **Database Connection Error**",
            color=discord.Color.red()
        )
        embed.add_field(
            name="Error Details",
            value=f"```{str(e)[:500]}```",
            inline=False
        )
        embed.add_field(
            name="💡 Troubleshooting",
            value="1. Check DATABASE_URL environment variable\n2. Verify PostgreSQL service is running\n3. Check network connectivity",
            inline=False
        )
    
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="stats", description="Send trading statistics summary")
@app_commands.describe(
    date_range="Date range for the statistics",
    total_signals="Total number of signals sent",
    tp1_hits="Number of TP1 hits",
    tp2_hits="Number of TP2 hits",
    tp3_hits="Number of TP3 hits",
    sl_hits="Number of SL hits",
    channels=
    "Select channels to send the stats to (comma-separated channel mentions or names)",
    currently_open="Number of currently open trades",
    total_closed="Total closed trades (auto-calculated if not provided)")
async def stats_command(interaction: discord.Interaction,
                        date_range: str,
                        total_signals: int,
                        tp1_hits: int,
                        tp2_hits: int,
                        tp3_hits: int,
                        sl_hits: int,
                        channels: str,
                        currently_open: str = "0",
                        total_closed: int = None):
    """Send formatted trading statistics to specified channels"""

    try:
        # Calculate total closed if not provided
        if total_closed is None:
            total_closed = tp1_hits + sl_hits

        # Calculate percentages
        def calc_percentage(hits, total):
            if total == 0:
                return "0%"
            return f"{(hits/total)*100:.0f}%"

        tp1_percent = calc_percentage(
            tp1_hits, total_closed) if total_closed > 0 else "0%"
        tp2_percent = calc_percentage(
            tp2_hits, total_closed) if total_closed > 0 else "0%"
        tp3_percent = calc_percentage(
            tp3_hits, total_closed) if total_closed > 0 else "0%"
        sl_percent = calc_percentage(
            sl_hits, total_closed) if total_closed > 0 else "0%"

        # Create the stats message
        stats_message = f"""**:bar_chart: TRADING SIGNAL STATISTICS**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
**:date: Period:** {date_range}

**:chart_with_upwards_trend: SIGNAL OVERVIEW**
• Total Signals Sent: **{total_signals}**
• Total Closed Positions: **{total_closed}**
• Currently Open: **{currently_open}**

**:dart: TAKE PROFIT PERFORMANCE**
• TP1 Hits: **{tp1_hits}**
• TP2 Hits: **{tp2_hits}**
• TP3 Hits: **{tp3_hits}**

**:octagonal_sign: STOP LOSS**
• SL Hits: **{sl_hits}** ({sl_percent})

**:bar_chart: PERFORMANCE SUMMARY**
• **Win Rate:** {tp1_percent}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""

        # Parse and send to multiple channels
        channel_list = [ch.strip() for ch in channels.split(',')]
        sent_channels = []

        for channel_identifier in channel_list:
            target_channel = None

            # Try to parse as channel mention
            if channel_identifier.startswith(
                    '<#') and channel_identifier.endswith('>'):
                channel_id = int(channel_identifier[2:-1])
                target_channel = bot.get_channel(channel_id)
            # Try to parse as channel ID
            elif channel_identifier.isdigit():
                target_channel = bot.get_channel(int(channel_identifier))
            # Try to find by name
            else:
                target_channel = discord.utils.get(
                    interaction.guild.channels,
                    name=channel_identifier) if interaction.guild else None

            if target_channel and isinstance(target_channel,
                                             discord.TextChannel):
                try:
                    await target_channel.send(stats_message)
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
                f"✅ Stats sent to: {', '.join(sent_channels)}", ephemeral=True)
        else:
            await interaction.response.send_message(
                "❌ No valid channels found or no messages sent.",
                ephemeral=True)

    except Exception as e:
        await interaction.response.send_message(
            f"❌ Error sending stats: {str(e)}", ephemeral=True)


# Web server for health checks
async def web_server():
    """Simple web server for health checks and keeping the service alive"""

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
                        "postgresql_version": version.split()[1] if version else "Unknown",
                        "pool_size": bot.db_pool.get_size(),
                        "pool_idle": bot.db_pool.get_idle_size()
                    }
            except Exception as e:
                database_status = f"Error: {str(e)[:50]}"
        
        response_data = {
            "status": "running",
            "bot_status": bot_status,
            "guild_count": guild_count,
            "database_status": database_status,
            "database_details": database_details,
            "uptime": str(datetime.now()),
            "version": "2.0"
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

        # Keep the server running
        while True:
            await asyncio.sleep(3600)  # Sleep for 1 hour, then continue

    except Exception as e:
        print(f"❌ Failed to start web server: {e}")
        raise


async def main():
    """Main async function to run both web server and Discord bot concurrently"""
    # Check if Discord token is available
    if not DISCORD_TOKEN:
        print("Error: DISCORD_TOKEN not found in environment variables")
        print(
            "Please set DISCORD_TOKEN_PART1 and DISCORD_TOKEN_PART2 environment variables"
        )
        return

    print(f"Bot token length: {len(DISCORD_TOKEN)} characters")
    print("Starting Discord Trading Bot...")

    # Create tasks for concurrent execution
    tasks = []

    # Web server task
    print("Starting web server...")
    web_task = asyncio.create_task(web_server())
    tasks.append(web_task)

    # Discord bot task with proper error handling
    async def start_bot_with_retry():
        max_retries = 3
        retry_delay = 30  # seconds

        for attempt in range(max_retries):
            try:
                print(
                    f"Starting Discord bot (attempt {attempt + 1}/{max_retries})..."
                )
                await bot.start(DISCORD_TOKEN)
                break  # If successful, break out of retry loop
            except discord.LoginFailure as e:
                print(f"Discord login failed: {e}")
                print("Please check your Discord bot token")
                break  # Don't retry on login failures
            except discord.HTTPException as e:
                if e.status == 429:  # Rate limited
                    print(
                        f"Rate limited by Discord. Waiting {retry_delay} seconds before retry..."
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    print(f"Discord HTTP error: {e}")
                    if attempt < max_retries - 1:
                        print(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                    else:
                        print("Max retries reached. Bot failed to start.")
            except Exception as e:
                print(f"Unexpected error starting Discord bot: {e}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    print("Max retries reached. Bot failed to start.")

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
