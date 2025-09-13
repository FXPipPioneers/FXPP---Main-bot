#!/usr/bin/env python3
"""
Database Connection Test Script
Run this to verify your PostgreSQL database is working on Render
"""

import os
import asyncio
import asyncpg
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

async def test_database_connection():
    """Test database connection and basic operations"""
    
    print("🔍 Testing Database Connection...")
    print("=" * 50)
    
    # Get database URL from environment
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        print("❌ ERROR: DATABASE_URL environment variable not found")
        print("   Make sure DATABASE_URL is set in your Render environment variables")
        return False
    
    print(f"✅ DATABASE_URL found: {database_url[:20]}...{database_url[-10:]}")
    
    try:
        # Test connection
        print("\n📡 Connecting to database...")
        conn = await asyncpg.connect(database_url)
        print("✅ Database connection successful!")
        
        # Test basic query
        print("\n🔍 Testing basic query...")
        result = await conn.fetchval('SELECT version()')
        print(f"✅ PostgreSQL version: {result[:50]}...")
        
        # Test table creation
        print("\n📋 Testing table creation...")
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS bot_test (
                id SERIAL PRIMARY KEY,
                test_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        print("✅ Test table created successfully")
        
        # Test data insertion
        print("\n📝 Testing data insertion...")
        await conn.execute(
            "INSERT INTO bot_test (test_message) VALUES ($1)",
            "Database test successful!"
        )
        print("✅ Data inserted successfully")
        
        # Test data retrieval
        print("\n📖 Testing data retrieval...")
        rows = await conn.fetch("SELECT * FROM bot_test ORDER BY id DESC LIMIT 1")
        if rows:
            row = rows[0]
            print(f"✅ Retrieved data: ID={row['id']}, Message='{row['test_message']}'")
        
        # Clean up test table
        print("\n🧹 Cleaning up test data...")
        await conn.execute("DROP TABLE IF EXISTS bot_test")
        print("✅ Test table cleaned up")
        
        # Close connection
        await conn.close()
        print("\n✅ Database connection closed properly")
        
        print("\n" + "=" * 50)
        print("🎉 ALL DATABASE TESTS PASSED!")
        print("Your PostgreSQL database is working perfectly on Render!")
        print("=" * 50)
        
        return True
        
    except Exception as e:
        print(f"\n❌ DATABASE ERROR: {str(e)}")
        print("\nTroubleshooting:")
        print("1. Check that DATABASE_URL is correctly set in Render environment variables")
        print("2. Verify your Render PostgreSQL service is running")
        print("3. Check Render logs for database connection issues")
        print("4. Ensure your app has network access to the database")
        return False

async def test_bot_database_functions():
    """Test bot-specific database operations"""
    
    print("\n🤖 Testing Bot Database Functions...")
    print("=" * 50)
    
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ DATABASE_URL not found, skipping bot tests")
        return False
    
    try:
        conn = await asyncpg.connect(database_url)
        
        # Test tables that the bot might create
        print("🔍 Checking for bot-related tables...")
        
        # Check if any tables exist
        tables = await conn.fetch("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public'
        """)
        
        if tables:
            print("✅ Found existing tables:")
            for table in tables:
                print(f"   - {table['tablename']}")
        else:
            print("ℹ️  No tables found (this is normal for a new database)")
        
        # Test that we can create tables with the structure the bot needs
        print("\n📋 Testing bot table creation capabilities...")
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS trading_signals_test (
                id SERIAL PRIMARY KEY,
                pair VARCHAR(20),
                entry_type VARCHAR(20),
                entry_price DECIMAL(10,5),
                tp1 DECIMAL(10,5),
                tp2 DECIMAL(10,5),
                tp3 DECIMAL(10,5),
                sl DECIMAL(10,5),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        print("✅ Trading signals table structure test passed")
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS member_roles_test (
                id SERIAL PRIMARY KEY,
                member_id BIGINT,
                guild_id BIGINT,
                role_id BIGINT,
                granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP
            )
        ''')
        print("✅ Member roles table structure test passed")
        
        # Clean up test tables
        await conn.execute("DROP TABLE IF EXISTS trading_signals_test")
        await conn.execute("DROP TABLE IF EXISTS member_roles_test")
        print("✅ Test tables cleaned up")
        
        await conn.close()
        
        print("\n🎉 BOT DATABASE FUNCTIONS TEST PASSED!")
        print("Your bot can successfully create and manage database tables!")
        
        return True
        
    except Exception as e:
        print(f"❌ BOT DATABASE ERROR: {str(e)}")
        return False

async def test_invite_abuse_system():
    """Test invite anti-abuse system database functionality"""
    
    print("\n🛡️ Testing Invite Anti-Abuse System...")
    print("=" * 50)
    
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ DATABASE_URL not found, skipping anti-abuse tests")
        return False
    
    try:
        conn = await asyncpg.connect(database_url)
        
        print("🔍 Testing invite_events table creation...")
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
        print("✅ invite_events table creation successful")
        
        print("🔍 Testing inviter_abuse_stats table creation...")
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
        print("✅ inviter_abuse_stats table creation successful")
        
        # Test eligibility check queries
        print("🔍 Testing anti-abuse system queries...")
        
        # Test ban check query
        ban_record = await conn.fetchrow('''
            SELECT banned_from_autorole FROM inviter_abuse_stats 
            WHERE guild_id = $1 AND inviter_id = $2
        ''', 123456789, 987654321)
        print(f"✅ Ban check query works: {ban_record}")
        
        # Test suspicious account logging
        from datetime import datetime, timezone, timedelta
        test_time = datetime.now(timezone.utc)
        
        await conn.execute('''
            INSERT INTO invite_events (guild_id, member_id, inviter_id, invite_code, joined_at, account_created_at, suspicious, autorole_allowed, reason)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (guild_id, member_id) DO NOTHING
        ''', 123456789, 111222333, 987654321, "testcode123", test_time, test_time - timedelta(minutes=30), True, False, "Test suspicious account")
        print("✅ Suspicious account logging works")
        
        # Test abuse stats update
        await conn.execute('''
            INSERT INTO inviter_abuse_stats (guild_id, inviter_id, suspicious_count, banned_from_autorole, first_suspicious_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (guild_id, inviter_id) DO UPDATE SET
            suspicious_count = inviter_abuse_stats.suspicious_count + 1,
            banned_from_autorole = CASE WHEN inviter_abuse_stats.suspicious_count + 1 >= 2 THEN TRUE ELSE inviter_abuse_stats.banned_from_autorole END,
            banned_at = CASE WHEN inviter_abuse_stats.suspicious_count + 1 >= 2 AND inviter_abuse_stats.banned_at IS NULL THEN $6 ELSE inviter_abuse_stats.banned_at END
        ''', 123456789, 987654321, 1, False, test_time, test_time)
        print("✅ Abuse statistics tracking works")
        
        # Verify tables exist
        tables = await conn.fetch("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name IN ('invite_events', 'inviter_abuse_stats')
        """)
        print(f"✅ Anti-abuse tables verified: {[row['table_name'] for row in tables]}")
        
        # Clean up test data
        await conn.execute("DELETE FROM invite_events WHERE member_id = $1", 111222333)
        await conn.execute("DELETE FROM inviter_abuse_stats WHERE inviter_id = $1", 987654321)
        print("✅ Test data cleaned up")
        
        await conn.close()
        
        print("\n🎉 INVITE ANTI-ABUSE SYSTEM TEST PASSED!")
        print("All database tables and queries work correctly!")
        
        return True
        
    except Exception as e:
        print(f"❌ ANTI-ABUSE SYSTEM ERROR: {str(e)}")
        return False

if __name__ == "__main__":
    print("Discord Trading Bot - Database Verification Tool")
    print("=" * 60)
    
    async def run_all_tests():
        # Run basic database tests
        basic_test_passed = await test_database_connection()
        
        if basic_test_passed:
            # Run bot-specific tests
            bot_test_passed = await test_bot_database_functions()
            
            # Run invite anti-abuse system tests
            abuse_test_passed = await test_invite_abuse_system()
            
            if bot_test_passed and abuse_test_passed:
                print("\n🏆 COMPLETE SUCCESS!")
                print("Your database is fully functional and ready for the Discord bot!")
                print("All invite anti-abuse system tables and queries work perfectly!")
            else:
                print("\n⚠️  Some tests failed - check the output above")
        else:
            print("\n❌ Database connection failed - check your configuration")
    
    # Run the tests
    asyncio.run(run_all_tests())