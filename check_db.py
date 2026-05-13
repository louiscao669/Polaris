import pymysql
import datetime

# --- CONFIGURATION ---
DB_CONFIG = {
    "host": "polaris-db.clmsauq4mqfc.us-east-2.rds.amazonaws.com",
    "port": 3306,           # Change to 3036 if that's your custom port
    "user": "PolarisAdmin",
    "password": "Polarishorse",
    "database": "polarisDB"
}

def run_debug_insert():
    print(f"🚀 Connecting to {DB_CONFIG['host']}...")
    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            # 1. Create a debug table if it doesn't exist
            print("🛠️  Ensuring debug table exists...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bus_debug (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    test_message VARCHAR(255),
                    created_at DATETIME
                )
            """)

            # 2. Insert a test record
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            msg = f"Testing Event Bus Connection at {now}"
            print(f"📤 Inserting: '{msg}'")
            
            sql = "INSERT INTO bus_debug (test_message, created_at) VALUES (%s, %s)"
            cursor.execute(sql, (msg, now))
            
            # CRITICAL: MySQL requires a commit to save changes!
            conn.commit() 
            print("✅ INSERT successful and committed.")

            # 3. Verify it's actually there
            print("🔎 Reading back last 1 record...")
            cursor.execute("SELECT * FROM bus_debug ORDER BY id DESC LIMIT 1")
            result = cursor.fetchone()
            print(f"🎉 SUCCESS! Found in DB: {result}")

        conn.close()

    except Exception as e:
        print(f"❌ FAILED: {e}")

if __name__ == "__main__":
    run_debug_insert()
