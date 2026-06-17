import os
import sys
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.pandas_hive_utils import metastore_db_config, db_connection

load_dotenv()

def diagnose():
    print("🔍 DIAGNOSING POSTGRES HIVE METASTORE CONNECTION...")
    try:
        config = metastore_db_config()
        print(f"Connection Config:")
        print(f"  - Host: {config['host']}")
        print(f"  - Port: {config['port']}")
        print(f"  - Database: {config['database']}")
        print(f"  - User: {config['user']}")
        
        conn = db_connection(config)
        cursor = conn.cursor()
        
        # Query schemas
        print("\n1. Schemas in database:")
        cursor.execute("SELECT schema_name FROM information_schema.schemata")
        schemas = [r[0] for r in cursor.fetchall()]
        print(f"  - Schemas: {schemas}")
        
        # Query tables in public schema
        print("\n2. Tables in public schema (lowercase):")
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'db%' OR table_name LIKE 'DB%'")
        tables = [r[0] for r in cursor.fetchall()]
        print(f"  - Database tables: {tables}")
        
        # Print all tables in public schema
        print("\n3. All tables in public schema:")
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name")
        all_tables = [r[0] for r in cursor.fetchall()]
        print(f"  - Count: {len(all_tables)}")
        print(f"  - Tables: {all_tables}")
        
        # Check specific tables
        for check_tab in ['dbs', 'DBS', 'tbls', 'TBLS']:
            cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{check_tab}')")
            exists = cursor.fetchone()[0]
            print(f"  - Table '{check_tab}' exists: {exists}")
            
        cursor.close()
        conn.close()
        print("\n✔ Diagnosis finished successfully!")
    except Exception as e:
        print(f"\n❌ Error during diagnosis: {e}")

if __name__ == "__main__":
    diagnose()
