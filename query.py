"""
AUDIT QUERY SYSTEM - query.py
--------------------------------------------------
Student Name: Tang Yiwei
Admin Number: G2500285L

Task (ii): Retrieve raw redemptions and tally with reimbursement files.
Optimized with O(1) Dictionary lookup for merchant metadata.
"""
import pandas as pd
import os
from loadData import load_bank_codes, load_merchant_info

def clean_currency(value):
    if isinstance(value, str):
        return float(value.replace('$', '').replace(',', ''))
    return float(value)

def run_query():
    print("=== CDC Voucher Audit Query System (Multi-Batch Enabled) ===")
    
    # Step 1: User Inputs
    m_id = input("Enter Merchant ID (e.g., M001): ").strip().upper()
    t_date = input("Enter Transaction Date (YYYYMMDD): ").strip()
    # Format t_date for CSV matching (YYYY-MM-DD)
    t_date_fmt = f"{t_date[:4]}-{t_date[4:6]}-{t_date[6:8]}"

    # Step 2: Retrieve Raw Redemptions from PROCESSED_LOGS
    print(f"\n[1/3] Searching archived logs for unique transactions on {t_date}...")
    
    archive_dir = "processed_logs"
    found_raw_files = []
    
    if os.path.exists(archive_dir):
        prefix = f"Redeem{t_date}"
        for filename in os.listdir(archive_dir):
            if filename.startswith(prefix) and filename.endswith(".csv"):
                found_raw_files.append(filename)
                
    if not found_raw_files:
        print(f"Result: No processed logs found for {t_date}. (Run complete.py first)")
        return

    all_raw_dfs = [pd.read_csv(os.path.join(archive_dir, f)) for f in found_raw_files]
    df_all_raw = pd.concat(all_raw_dfs, ignore_index=True)
    
    # Audit Logic: De-duplicate by Transaction_ID to get the true required sum
    df_all_raw['Amount_Redeemed'] = df_all_raw['Amount_Redeemed'].apply(clean_currency)
    m_records = df_all_raw[
        (df_all_raw['Merchant_ID'] == m_id) & 
        (df_all_raw['Payment_Status'] == 'Completed')
    ].drop_duplicates(subset=['Transaction_ID'])
    
    raw_expected_total = m_records['Amount_Redeemed'].sum()

    if raw_expected_total == 0:
        print(f"Result: No 'Completed' transactions found for {m_id} on {t_date}.")
        return

    # Step 3: Tally with ALL relevant Reimbursement Batches
    print(f"[2/3] Scanning all settlement batches for Merchant {m_id}...")
    
    bank_map = load_bank_codes("BankCode.csv")
    df_m = load_merchant_info("Merchant.csv", bank_map)
    merchant_lookup = df_m.set_index('Merchant_ID').to_dict('index')
    
    if m_id not in merchant_lookup:
        print(f"Error: Merchant ID {m_id} not found in database.")
        return
        
    swift_code = merchant_lookup[m_id]['SWIFT_Code']
    reimb_accumulated_total = 0.0
    all_reimb_ids = []
    settlement_dates = set()
    
    reimb_dir = "reimbursement_files"
    if os.path.exists(reimb_dir):
        # Scan EVERY file belonging to this bank
        for filename in os.listdir(reimb_dir):
            if filename.startswith(swift_code) and filename.endswith(".csv"):
                df_reimb = pd.read_csv(os.path.join(reimb_dir, filename))
                
                # Find rows matching both Merchant AND the specific Transaction Date
                # A merchant might have multiple entries across different batch files
                matches = df_reimb[
                    (df_reimb['Merchant_ID'] == m_id) & 
                    (df_reimb['Transaction_Date'] == t_date_fmt)
                ]
                
                if not matches.empty:
                    for _, row in matches.iterrows():
                        reimb_accumulated_total += clean_currency(row['Amount_Reimbursed'])
                        all_reimb_ids.append(row['Reimburse_ID'])
                        settlement_dates.add(row['Reimburse_Date'])

    # Step 4: Show Comprehensive Audit Report
    print("\n" + "="*50)
    print(f"                AUDIT REPORT: {m_id}")
    print("="*50)
    print(f"Merchant Name:     {merchant_lookup[m_id]['Merchant_Name']}")
    print(f"Bank / SWIFT:      {merchant_lookup[m_id]['Bank_Name']} ({swift_code})")
    print(f"Transaction Date:  {t_date_fmt}")
    print("-" * 50)
    print(f"EXPECTED (Logs):   ${raw_expected_total:,.2f}")
    print(f"ACTUAL (Settled):  ${reimb_accumulated_total:,.2f}")
    print(f"Settlement Dates:  {', '.join(list(settlement_dates)) if settlement_dates else 'N/A'}")
    print(f"Reimburse IDs:     {', '.join(all_reimb_ids) if all_reimb_ids else 'None'}")
    print("-" * 50)
    
    # Tally Check
    diff = abs(raw_expected_total - reimb_accumulated_total)
    status = "✅ MATCHED" if diff < 0.01 else "❌ DISCREPANCY"
    print(f"AUDIT STATUS:      {status}")
    if diff >= 0.01:
        print(f"Variance:          ${diff:,.2f}")
    print("="*50)

    # Step 5: Save choice
    if input("\nDownload detailed audit report? (y/n): ").lower() == 'y':
        report_file = f"Audit_{m_id}_{t_date}.csv"
        pd.DataFrame([{
            "Merchant_ID": m_id,
            "TX_Date": t_date_fmt,
            "Expected_Amt": raw_expected_total,
            "Settled_Amt": reimb_accumulated_total,
            "Reimburse_IDs": "|".join(all_reimb_ids),
            "Status": status
        }]).to_csv(report_file, index=False)
        print(f"Report saved to {report_file}")

if __name__ == "__main__":
    run_query()