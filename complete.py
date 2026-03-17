"""
FINAL INTEGRATED SOLUTION - complete.py
--------------------------------------------------
Student Name: Tang Yiwei
Admin Number: G2500285L

CHOSEN ALGORITHMS (Based on Time Complexity Analysis):
1. Aggregation: Pandas GroupBy (O(N)) - Selected for superior vectorization on raw logs.
2. Mapping: Native Hash Map (O(1)) - Selected for fastest metadata association.
3. Batching: Native Hash Bucketing (O(N)) - Selected for efficient linear partitioning.
"""
import pandas as pd
import os
import glob
import shutil
from datetime import datetime

from loadData import (
    load_bank_codes, 
    load_merchant_info, 
    load_all_redemptions_from_folder
)

def clean_currency(value):
    if isinstance(value, str):
        return float(value.replace('$', '').replace(',', ''))
    return float(value)

def main():
    print("=== Batch Reimbursement System: Processing New Data ===\n")

    basedir = os.path.abspath(os.path.dirname(__file__))
    
    # --- Step 1: Data Loading ---
    redemption_files = glob.glob(os.path.join(basedir, "Redeem*.csv"))
    
    if not redemption_files:
        print(">>> No new redemption logs found. System up to date.")
        return

    print(f"[1/4] Found {len(redemption_files)} new log files. Loading...")
    
    bank_swift_map = load_bank_codes("BankCode.csv")
    df_merchants = load_merchant_info("Merchant.csv", bank_swift_map)
    df_redemptions = load_all_redemptions_from_folder(basedir)
    
    if df_merchants is None or df_redemptions is None or df_redemptions.empty:
        print("Error: Could not parse data.")
        return

    # Data Sanitization
    df_redemptions['Amount_Redeemed'] = df_redemptions['Amount_Redeemed'].apply(clean_currency)
    df_redemptions['Transaction_DT'] = pd.to_datetime(df_redemptions['Transaction_Date_Time'])
    df_redemptions['Date_Only'] = df_redemptions['Transaction_DT'].dt.date

    # --- Step 2: Aggregation (Multi-Dimension) ---
    print("[2/4] Aggregating unique transactions...")
    df_unique_tx = df_redemptions.drop_duplicates(subset=['Transaction_ID'])
    df_filtered = df_unique_tx[
        (df_unique_tx['Payment_Status'] == 'Completed') & 
        (df_unique_tx['Merchant_ID'].isin(df_merchants['Merchant_ID']))
    ]
    
    if df_filtered.empty:
        print(">>> No 'Completed' transactions found to process.")
    else:
        # Group by Merchant and Original Transaction Date
        summary_df = df_filtered.groupby(['Merchant_ID', 'Date_Only'])['Amount_Redeemed'].sum().reset_index()

        # --- Step 3: Mapping & Finalizing Dates ---
        print("[3/4] Generating reimbursement records...")
        merchant_lookup = df_merchants.set_index('Merchant_ID').to_dict('index')
        
        # KEY CHANGE: Capture the REAL-WORLD DATE of the program execution
        execution_date_str = datetime.now().strftime("%Y-%m-%d")
        
        final_records = []
        tx_counter = 123456 
        
        for row in summary_df.itertuples(index=False):
            if row.Merchant_ID in merchant_lookup:
                info = merchant_lookup[row.Merchant_ID]
                
                final_records.append({
                    "Merchant_ID": row.Merchant_ID,
                    "Merchant_Name": info['Merchant_Name'],
                    "Reimburse_ID": f"TX{tx_counter}",
                    "Reimburse_Date": execution_date_str,             # Current Execution Date
                    "Transaction_Date": row.Date_Only.strftime("%Y-%m-%d"), # Original Day of Transaction
                    "Amount_Reimbursed": f"${row.Amount_Redeemed:,.2f}",
                    "Bank_Account": info['Account_Number'],
                    "Remarks": f"CDC Settlement {row.Date_Only}",
                    "SWIFT_Code": info.get('SWIFT_Code', 'UNKNOWN')
                })
                tx_counter += 1

        # --- Step 4: Export & Partitioning ---
        print("[4/4] Partitioning files by SWIFT and incrementing batches...")
        
        output_dir = "reimbursement_files"
        if not os.path.exists(output_dir): os.makedirs(output_dir)
        
        today_file_stamp = datetime.now().strftime("%Y%m%d")
        
        # Organize into Bank Buckets
        bank_buckets = {}
        for entry in final_records:
            swift = entry.pop('SWIFT_Code')
            bank_buckets.setdefault(swift, []).append(entry)

        # Dynamic Batch ID Detection
        for swift, records in bank_buckets.items():
            batch_id = 1
            while True:
                filename = f"{swift}_{today_file_stamp}_{batch_id:02d}.csv"
                file_full_path = os.path.join(output_dir, filename)
                if not os.path.exists(file_full_path):
                    break
                batch_id += 1
            
            pd.DataFrame(records).to_csv(file_full_path, index=False)
            print(f"   -> Success: Generated {filename}")

    # --- Archiving ---
    archive_dir = os.path.join(basedir, "processed_logs")
    if not os.path.exists(archive_dir): os.makedirs(archive_dir)

    print(f"\n[Maintenance] Archiving {len(redemption_files)} processed files...")
    for f_path in redemption_files:
        f_name = os.path.basename(f_path)
        shutil.move(f_path, os.path.join(archive_dir, f_name))
        print(f"   -> Moved: {f_name}")

    print("\n[Final] Batch complete. Settlement Date set to today's date.")

if __name__ == "__main__":
    main()