import csv
import os
import shutil
from pathlib import Path
from datetime import datetime

# Configuration
SOURCE_DIR = Path("data/.data-reduced")
TARGET_DIR = Path("data/.data-small-test")
TARGET_DIR.mkdir(parents=True, exist_ok=True)

# Helper to read csv
def read_csv(path):
    with open(path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader), reader.fieldnames

# Helper to write csv
def write_csv(path, data, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, mode='w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def filter_transactions(source_dir, target_dir):
    selected_transactions = []
    selected_txn_ids = set()
    selected_user_ids = set()
    selected_store_ids = set()
    
    # We will process Jan 2024 and Jan 2025 to cover both years
    files_to_process = ["transactions_202401.csv", "transactions_202501.csv"]
    
    for filename in files_to_process:
        source_path = source_dir / "transactions" / filename
        if not source_path.exists():
            print(f"Warning: {source_path} does not exist")
            continue
            
        rows, fieldnames = read_csv(source_path)
        
        # Select a subset: 
        # 1. Some satisfying Q1 (amount >= 75, 06:00-23:00)
        # 2. Some NOT satisfying Q1 (amount < 75 or outside hours)
        # We'll just take the first 50 rows to keep it simple but deterministic
        # and hope the reduced dataset has enough variety. 
        # Actually, let's try to pick specifically to ensure coverage.
        
        count = 0
        for row in rows:
            # Parse date to check time
            try:
                dt = datetime.strptime(row['created_at'], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
                
            amount = float(row['final_amount'])
            
            # We want a mix. Let's take:
            # - 10 rows with amount >= 75 and valid time
            # - 10 rows with amount < 75
            # - 10 rows with invalid time (if any)
            # - 20 random others
            
            hour = dt.hour
            valid_time = 6 <= hour < 23
            high_amount = amount >= 75
            
            keep = False
            if count < 50: # Limit total per file
                keep = True
                
            if keep:
                selected_transactions.append(row)
                selected_txn_ids.add(row['transaction_id'])
                if row['user_id']:
                    # Normalize user_id (remove .0 if present)
                    try:
                        uid = str(int(float(row['user_id'])))
                        row['user_id'] = uid # Update row as well
                        selected_user_ids.add(uid)
                    except ValueError:
                        selected_user_ids.add(row['user_id'])
                selected_store_ids.add(row['store_id'])
                count += 1
        
        write_csv(target_dir / "transactions" / filename, selected_transactions[-count:], fieldnames)
        print(f"Wrote {count} transactions to {filename}")

    return selected_txn_ids, selected_user_ids, selected_store_ids

def filter_transaction_items(source_dir, target_dir, txn_ids):
    selected_items = []
    selected_item_ids = set()
    
    files_to_process = ["transaction_items_202401.csv", "transaction_items_202501.csv"]
    
    for filename in files_to_process:
        source_path = source_dir / "transaction_items" / filename
        if not source_path.exists():
            continue
            
        rows, fieldnames = read_csv(source_path)
        
        file_items = []
        for row in rows:
            if row['transaction_id'] in txn_ids:
                file_items.append(row)
                selected_item_ids.add(row['item_id'])
        
        write_csv(target_dir / "transaction_items" / filename, file_items, fieldnames)
        print(f"Wrote {len(file_items)} transaction items to {filename}")
        
    return selected_item_ids

def filter_users(source_dir, target_dir, user_ids):
    # Users are split by month in source, but user_id is consistent.
    # We need to find where these users are defined. 
    # Since we don't know which file contains which user without reading all,
    # and users might be repeated or updated? Assuming static user info for now based on file structure.
    # Actually, the users directory has users_YYYYMM.csv. 
    # Let's just read all user files corresponding to our transaction months 
    # AND maybe others if we can't find them?
    # Simpler approach: Read ALL user files, find the users we need, write them to their respective files.
    
    # Note: The structure is data/.data-reduced/users/users/users_YYYYMM.csv
    # We should replicate this.
    
    user_files = sorted((source_dir / "users" / "users").glob("users_*.csv"))
    
    found_users = set()
    
    for file_path in user_files:
        rows, fieldnames = read_csv(file_path)
        file_users = []
        
        for row in rows:
            if row['user_id'] in user_ids:
                file_users.append(row)
                found_users.add(row['user_id'])
        
        if file_users:
            target_path = target_dir / "users" / "users" / file_path.name
            write_csv(target_path, file_users, fieldnames)
            print(f"Wrote {len(file_users)} users to {file_path.name}")

def filter_stores(source_dir, target_dir, store_ids):
    # Stores are likely in one file or few
    store_files = (source_dir / "stores").glob("*.csv")
    
    for file_path in store_files:
        rows, fieldnames = read_csv(file_path)
        file_stores = []
        for row in rows:
            if row['store_id'] in store_ids:
                file_stores.append(row)
        
        write_csv(target_dir / "stores" / file_path.name, file_stores, fieldnames)
        print(f"Wrote {len(file_stores)} stores to {file_path.name}")

def filter_menu_items(source_dir, target_dir, item_ids):
    menu_files = (source_dir / "menu_items").glob("*.csv")
    
    for file_path in menu_files:
        rows, fieldnames = read_csv(file_path)
        file_items = []
        for row in rows:
            if row['item_id'] in item_ids:
                file_items.append(row)
        
        write_csv(target_dir / "menu_items" / file_path.name, file_items, fieldnames)
        print(f"Wrote {len(file_items)} menu items to {file_path.name}")

def main():
    print("Generating small dataset...")
    txn_ids, user_ids, store_ids = filter_transactions(SOURCE_DIR, TARGET_DIR)
    item_ids = filter_transaction_items(SOURCE_DIR, TARGET_DIR, txn_ids)
    filter_users(SOURCE_DIR, TARGET_DIR, user_ids)
    filter_stores(SOURCE_DIR, TARGET_DIR, store_ids)
    filter_menu_items(SOURCE_DIR, TARGET_DIR, item_ids)
    print("Done.")

if __name__ == "__main__":
    main()
