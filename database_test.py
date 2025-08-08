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

if __name__ == "__main__":
    print("Discord Trading Bot - Database Verification Tool")
    print("=" * 60)
    
    async def run_all_tests():
        # Run basic database tests
        basic_test_passed = await test_database_connection()
        
        if basic_test_passed:
            # Run bot-specific tests
            bot_test_passed = await test_bot_database_functions()
            
            if bot_test_passed:
                print("\n🏆 COMPLETE SUCCESS!")
                print("Your database is fully functional and ready for the Discord bot!")
            else:
                print("\n⚠️  Basic database works, but bot functions need attention")
        else:
            print("\n❌ Database connection failed - check your configuration")
    
    # Run the tests
    asyncio.run(run_all_tests())