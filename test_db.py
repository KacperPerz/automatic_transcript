import sqlite3
import os

def test_processed_db():
    db_path = "processed.db"
    
    if not os.path.exists(db_path):
        print(f"Database file {db_path} not found!")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Get list of tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"Tables in database: {[table[0] for table in tables]}")
        print()
        
        # Query each table
        for table in tables:
            table_name = table[0]
            print(f"=== Table: {table_name} ===")
            
            # Get table schema
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            print(f"Columns: {[col[1] for col in columns]}")
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]
            print(f"Row count: {count}")
            
            # Show first 5 rows
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 5;")
            rows = cursor.fetchall()
            print("Sample data:")
            for row in rows:
                print(f"  {row}")
            print()
    
    except Exception as e:
        print(f"Error querying database: {e}")
    
    finally:
        conn.close()

def delete_all_rows(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM processed")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    # test_processed_db()
    delete_all_rows("processed.db")