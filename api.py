from flask import Flask, request, render_template, jsonify, redirect
from flask_cors import CORS
import random
import string
import csv
import os
import json
import glob
from datetime import datetime

# Import your core logic classes from data_structure.py
from data_structure import CDCSystem 
pending_redemptions = {}

app = Flask(__name__)
CORS(app)

# Landing page
@app.route("/", methods=["GET"])
def landing():
    """
    First page: CDC Voucher Platform with links to Household and Merchant modules.
    """
    return render_template("landing.html")


# Initialize the global CDC system instance
cdc_system = CDCSystem()
pending_redemptions = {}

def restore_household_state(h_id):
    """
    Restores a household's state from persistent storage (CSV) to memory.
    Ensures that claimed vouchers and used statuses remain accurate across server restarts.
    """
    basedir = os.path.abspath(os.path.dirname(__file__)) 
    claims_csv = os.path.join(basedir, "Claims.csv")

    hh = cdc_system.households.get(h_id)
    if not hh: 
        return

    # --- Part A: Restore Voucher Claim Status ---
    # Re-issue vouchers into memory based on the historical claims registry.
    if os.path.isfile(claims_csv):
        with open(claims_csv, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            # Standard denomination configurations for various schemes
            configs = {
                "2025_may": {2: 50, 5: 20, 10: 30}, 
                "2026_jan": {2: 30, 5: 12, 10: 18}
            }
            for row in reader:
                if row["Household_ID"] == h_id:
                    t_key = row["Tranche_Key"]
                    if t_key in configs:
                        cdc_system.claim_vouchers(h_id, t_key, configs[t_key])

    # --- Part B: Restore Redemption Status (Multi-Path Optimization) ---
    # Define all possible locations where redemption logs might exist.
    # This includes the root, the staging area, and archived logs moved by the settlement engine.
    search_paths = [
        os.path.join(basedir, "Redeem*.csv"),                   # Root directory
        os.path.join(basedir, "redemptions", "Redeem*.csv"),    # Pending settlement folder
        os.path.join(basedir, "processed_logs", "Redeem*.csv")  # Archived/Processed folder
    ]

    # Consolidate all identified file paths into a single list
    all_redeem_files = []
    for path in search_paths:
        all_redeem_files.extend(glob.glob(path))

    # Iterate through all logs to mark vouchers that have already been spent
    for r_file in all_redeem_files:
        try:
            with open(r_file, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row["Household_ID"] == h_id:
                        v_code = row["Voucher_Code"]
                        # Search memory for the specific voucher and update its status
                        for v_list in hh.tranches.values():
                            for v in v_list:
                                if v.voucher_code == v_code:
                                    v.is_redeemed = True
        except Exception as e:
            print(f"Error reading {r_file}: {e}")

    # Enforce balance synchronization based on the newly restored state
    hh.update_balance()

def household_from_csv(search_term: str, csv_file: str):
    """
    Support the ability to search for families by Address or Household_ID and load them into memory.
    """
    normalized = search_term.strip().lower()
    if not os.path.isfile(csv_file): 
        return None

    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Match both the address column and the ID column simultaneously
            if row["Address"].strip().lower() == normalized or \
               row["Household_ID"].strip().lower() == normalized:
                h_id = row["Household_ID"]
                if h_id not in cdc_system.households:
                    cdc_system.add_household(h_id)
                    # Trigger the recovery logic to retrieve the historical coupon and consumption records
                    restore_household_state(h_id) 
                return cdc_system.households[h_id]
    return None

def generate_hh_id():
    """
    Generates a Household ID following the requirement:
    The letter 'H' followed by 11 random digits.
    """
    return "H" + "".join(random.choices(string.digits, k=11))

def update_household_balance_csv(household_id, new_balance):
    """
    Update the balance of a specific household in the Households.csv file
    """
    basedir = os.path.abspath(os.path.dirname(__file__))
    csv_file = os.path.join(basedir, "Households.csv")
    
    if not os.path.isfile(csv_file):
        return False
    
    rows = []
    with open(csv_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            if row["Household_ID"] == household_id:
                row["Total_Balance"] = f"${new_balance:.2f}"
            rows.append(row)
    
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    return True

# =============================
# (Liu Jiani) validate if the user exists in merchant.csv
# =============================

# verify if the merchant exists om Merchant.csv
def validate_merchant(merchant_id):
    if merchant_id in cdc_system.merchants:
        return True
    
    basedir = os.path.abspath(os.path.dirname(__file__))
    csv_file = os.path.join(basedir, "Merchant.csv")
    
    if not os.path.isfile(csv_file):
        return False
        
    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["Merchant_ID"] == merchant_id:
                from data_structure import Merchant 
                m_obj = Merchant(
                    merchant_id=row["Merchant_ID"],
                    merchant_name=row["Merchant_Name"],
                    uen=row["UEN"],
                    bank_name=row["Bank_Name"],
                    bank_code=row["Bank_Code"],
                    branch_code=row["Branch_Code"],
                    account_number=row["Account_Number"],
                    account_holder_name=row["Account_Holder_Name"],
                    registration_date=row["Registration_Date"],
                    status=row["Status"]
                )
                cdc_system.add_merchant(m_obj)
                found = True
                break
    
    return found

# save the redemption record as RedeemYYYYMMDDHH.csv
def log_redemption_csv(transactions):
    now = datetime.now()
    filename = f"Redeem{now.strftime('%Y%m%d%H')}.csv" # form the file name structure
    
    basedir = os.path.abspath(os.path.dirname(__file__))
    file_path = os.path.join(basedir, filename)
    
    file_exists = os.path.isfile(file_path)
    
    fieldnames = [
        "Transaction_ID", "Household_ID", "Merchant_ID", 
        "Transaction_Date_Time", "Voucher_Code", "Denomination_Used", 
        "Amount_Redeemed", "Payment_Status", "Remarks"
    ]
    
    with open(file_path, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
            
        writer.writerows(transactions)
    
    print(f">>> Transaction logged to: {filename}")
# =======================================

@app.route("/household", methods=["GET"])
def household_home():
    """Renders the initial household registration form."""
    return render_template("register_hh.html")

@app.route("/household/register", methods=["POST"])
def register_household():
    """
    Handles the registration logic
    """
    name = request.form.get("name")
    email = request.form.get("email")
    address = request.form.get("address")
    members = request.form.get("members")
    
    # Validation: Ensure the address is provided
    if not address or not members:
        return "Missing data", 400
    
    # --- Persistent Storage (CSV) ---
    basedir = os.path.abspath(os.path.dirname(__file__))
    csv_file = os.path.join(basedir, "Households.csv")

    # Load household from CSV using address
    hh = household_from_csv(address, csv_file)
    is_new = False
    
    if hh:
        h_id = hh.household_id
        message = f"{name}, your household has already been registered."
    else:
        is_new = True
        h_id = generate_hh_id()
        message = f"{name}, your household has been successfully registered!"
        
        # --- Core Logic Execution ---
        # Register the household in the memory system
        cdc_system.add_household(h_id)
        
        # Save to CSV
        file_exists = os.path.isfile(csv_file)
        
        with open(csv_file, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Write the header if this is a new file
            if not file_exists:
                writer.writerow(["Household_ID", "Name", "Email", "Address", "Members", "Total_Balance"])
            
            current_balance = cdc_system.get_balance(h_id)  
            writer.writerow([h_id, name, email, address, members, f"${current_balance:.2f}"])
        
        print(f">>> Data successfully written to: {csv_file}")
  
    return render_template("hh_success.html", 
                          message=message, 
                          household_id=h_id, 
                          is_new=is_new)

@app.route("/household/login", methods=["GET", "POST"])
def household_login():
    if request.method == "POST":
        # Obtain the user's input address or ID
        search_input = request.form.get("search_input", "").strip()
        
        # Query function
        basedir = os.path.abspath(os.path.dirname(__file__))
        csv_file = os.path.join(basedir, "Households.csv")
        hh = household_from_csv(search_input, csv_file)
        
        if hh:
            # Find the user and directly navigate to the management page.
            return redirect(f"/household/vouchers/{hh.household_id}")
        else:
            # Not found. Returning error message.
            return render_template("login.html", error="Address or ID not found.")
            
    return render_template("login.html")

@app.route("/api/mobile/login", methods=["POST"])
def api_mobile_login():
    data = request.get_json()
    search_input = data.get("search_input", "").strip()
    
    basedir = os.path.abspath(os.path.dirname(__file__))
    csv_file = os.path.join(basedir, "Households.csv")
    
    hh = household_from_csv(search_input, csv_file)
    
    if hh:
        return jsonify({
            "success": True,
            "household_id": hh.household_id,
            "balance": cdc_system.get_balance(hh.household_id)
        })
    return jsonify({"success": False, "message": "User not found"}), 404

@app.route("/household/vouchers/<household_id>")
def view_vouchers(household_id):
    """voucher management page"""
    if household_id not in cdc_system.households:
        basedir = os.path.abspath(os.path.dirname(__file__))
        csv_file = os.path.join(basedir, "Households.csv")
        
        if not os.path.isfile(csv_file):
            return "Household not found", 404
        
        found = False
        with open(csv_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["Household_ID"] == household_id:
                    cdc_system.add_household(household_id)
                    found = True
                    break
        
        if not found:
            return "Household not found", 404
    
    cdc_household = cdc_system.households[household_id]
    
    household_info = {"address": "", "members": "", "name": "", "email": ""}
    basedir = os.path.abspath(os.path.dirname(__file__))
    csv_file = os.path.join(basedir, "Households.csv")
    
    if os.path.isfile(csv_file):
        with open(csv_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["Household_ID"] == household_id:
                    household_info = {
                        "address": row.get("Address", ""),
                        "members": row.get("Members", ""),
                        "name": row.get("Name", ""),
                        "email": row.get("Email", "")
                    }
                    break
    
    # ========== Batch configuration ==========
    TRANCHES = {
        "2025_may": {
            "name": "CDC Vouchers Scheme 2025 (May)",
            "total": 500,
            "breakdown": {2: 50, 5: 20, 10: 30}
        },
        "2026_jan": {
            "name": "CDC Vouchers Scheme 2026 (January)", 
            "total": 300,
            "breakdown": {2: 30, 5: 12, 10: 18}
        }
    }
    
    # ========== Get voucher information ==========
    all_vouchers = []
    denomination_counts = {"$2": 0, "$5": 0, "$10": 0}
    available_counts = {"$2": 0, "$5": 0, "$10": 0}
    
    for tranche_name, voucher_list in cdc_household.tranches.items():
        for voucher in voucher_list:
            denom = f"${voucher.denomination}"
            status = "used" if voucher.is_redeemed else "available"
            
            all_vouchers.append({
                "code": voucher.voucher_code,
                "denomination": denom,
                "amount": voucher.denomination,
                "status": status,
                "tranche_name": voucher.tranche_name
            })
            
            denomination_counts[denom] += 1
            if not voucher.is_redeemed:
                available_counts[denom] += 1
    
    # ========== Get batch status ==========
    tranches_info = []
    for tranche_key, config in TRANCHES.items():
        is_claimed = tranche_key in cdc_household.claimed_tranches
        tranches_info.append({
            "key": tranche_key,
            "name": config["name"],
            "total": config["total"],
            "is_claimed": is_claimed,
            "breakdown": config["breakdown"]
        })
    
    # ========== Get balance ==========
    total_balance = cdc_system.get_balance(household_id)
    
    # ========== Rendering template ==========
    return render_template(
        "vouchers.html",
        household_id=household_id,
        household_info=household_info,
        total_balance=total_balance,
        denomination_counts=denomination_counts,
        available_counts=available_counts,
        tranches_info=tranches_info,
        all_vouchers=all_vouchers,
        sum_available=sum(available_counts.values())
    )

@app.route("/household/api/voucher/claim", methods=["POST"])
def api_claim_vouchers():
    """API for claiming vouchers"""
    data = request.get_json()
    
    if not data:
        return jsonify({"error": "No data provided"}), 400
    
    household_id = data.get("household_id")
    tranche = data.get("tranche")
    
    if not household_id:
        return jsonify({"error": "household_id is required"}), 400
    if not tranche:
        return jsonify({"error": "tranche is required"}), 400
    
    if household_id not in cdc_system.households:
        return jsonify({"error": "Household not found"}), 404

    TRANCHES = {
        "2025_may": {
            "name": "CDC Vouchers Scheme 2025 (May)",
            "total": 500,
            "breakdown": {2: 50, 5: 20, 10: 30}
        },
        "2026_jan": {
            "name": "CDC Vouchers Scheme 2026 (January)",
            "total": 300, 
            "breakdown": {2: 30, 5: 12, 10: 18}
        }
    }
    
    if tranche not in TRANCHES:
        return jsonify({"error": f"Invalid tranche. Must be '2025_may' or '2026_jan'"}), 400
    
    household = cdc_system.households[household_id]

    if tranche in household.claimed_tranches:
        return jsonify({"error": f"Tranche '{TRANCHES[tranche]['name']}' already claimed"}), 400

    config = TRANCHES[tranche]
    success = cdc_system.claim_vouchers(household_id, tranche, config["breakdown"])
    
    if not success:
        return jsonify({"error": "Claim failed"}), 500

    new_balance = cdc_system.get_balance(household_id)

    update_household_balance_csv(household_id, new_balance)
    
    basedir = os.path.abspath(os.path.dirname(__file__))
    claims_csv = os.path.join(basedir, "Claims.csv")
    
    file_exists = os.path.isfile(claims_csv)
    with open(claims_csv, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # If the file is newly created, write the header information first.
        if not file_exists:
            writer.writerow(["Household_ID", "Tranche_Key"])
        # Record the family ID and the corresponding coupon batch key
        writer.writerow([household_id, tranche])

    return jsonify({
        "success": True,
        "household_id": household_id,
        "tranche": tranche,
        "tranche_name": config["name"],
        "total_value": config["total"],
        "denominations": config["breakdown"],
        "new_balance": new_balance,
        "message": f"Successfully claimed {config['total']} SGD in CDC vouchers"
    })

@app.route("/household/api/redemption/claim", methods=["POST"])
def merchant_claim_process():
    """
    Merchant end verification.
    Receive the Token string generated by the generate interface (or the data scanned from the barcode),
    After verifying the merchant's identity, deduct the coupon status in the memory and simultaneously update the CSV transaction record.
    """
    data = request.get_json()
    merchant_id = data.get("merchant_id")
    # Here, token_str corresponds to the complete data with the "+" sign generated by the "generate" interface.
    token_str = data.get("qr_code_data") 
    
    if not merchant_id or not token_str:
        return jsonify({"error": "Missing merchant_id or token"}), 400
        
    # 1. Verify the legitimacy of the merchant
    if not validate_merchant(merchant_id):
        return jsonify({"error": "Invalid Merchant ID"}), 403
        
    try:
        # 2. Parse the Token string
        # Household_ID + Voucher_Codes + Timestamp
        parts = token_str.split('+')
        if len(parts) < 3:
            raise ValueError("Token format error")
            
        household_id = parts[0]
        # Parse the list of codes (separated by commas)
        codes_to_redeem = parts[1].split(',') 
        
    except Exception as e:
        return jsonify({"error": f"Invalid Token Format: {str(e)}"}), 400

    # 3. Invoke the core business logic to execute the write-off operation
    # "Results" is a list of dictionaries (to be written into CSV), and "msg" is the status description.
    results, msg = cdc_system.redeem(household_id, merchant_id, codes_to_redeem)
    
    # If the write-off fails, the "results" will return "None".
    if results is None:
        return jsonify({"error": msg}), 400

    # 4. State persistence (saved to CSV)
    # A. Record the write-off transactions and save them in the file named "RedeemYYYYMMDDHH.csv"
    log_redemption_csv(results)
    
    # B. Update the total household balance to Households.csv
    new_balance = cdc_system.get_balance(household_id)
    update_household_balance_csv(household_id, new_balance)
    
    # 5. Return the JSON response to the front end
    # Extract the first record to display the transaction number, and extract the last record to obtain the total amount of write-offs.
    first_record = results[0]
    total_redeemed = results[-1]['Amount_Redeemed'] 
    
    return jsonify({
        "success": True,
        "transaction_id": first_record['Transaction_ID'],
        "amount_redeemed": total_redeemed,
        "new_balance": new_balance,
        "message": "Transaction completed successfully"
    })

# =======================================
# (Liu Jiani) generate a verification code after user redemption
# =======================================
@app.route("/household/api/redemption/generate", methods=["POST"])
def generate_redemption_token():
    """
    Step 1: Residents select denomination -> System locks specific voucher code -> Generate "intelligent semantic code"
    Return: Intelligent code + detailed list (Voucher ID & Amount)
    """
    data = request.get_json()
    household_id = data.get("household_id")
    selection = data.get("selected_items") # {"2": 2, "5": 1, "10": 1}
    
    if not household_id or not selection:
        return jsonify({"error": "Missing data"}), 400
        
    # 1. Ensure that the data is loaded into the memory.
    if household_id not in cdc_system.households:
        basedir = os.path.abspath(os.path.dirname(__file__))
        household_from_csv(household_id, os.path.join(basedir, "Households.csv")) 
        
    if household_id not in cdc_system.households:
        return jsonify({"error": "Household not found"}), 404
        
    household = cdc_system.households[household_id]
    
    # 2. Filter available coupon codes
    selected_codes = []
    vouchers_detail_list = [] 
    required_breakdown = {int(k): int(v) for k, v in selection.items()}
    
    # Count the actual number of selections for each denomination (for generating codes)
    # count_map structure: {2: quantity, 5: quantity, 10: quantity}
    count_map = {2: 0, 5: 0, 10: 0} 

    all_available = [v for t_list in household.tranches.values() for v in t_list if not v.is_redeemed]
    
    for denom, count_needed in required_breakdown.items():
        if count_needed <= 0: continue
        found = 0
        for v in all_available:
            if v.denomination == denom and v.voucher_code not in selected_codes:
                selected_codes.append(v.voucher_code)
                vouchers_detail_list.append({"id": v.voucher_code, "amount": v.denomination})
                found += 1
                if found == count_needed: break
        
        if found < count_needed:
            return jsonify({"error": f"Insufficient ${denom} vouchers"}), 400
        
        # Record the actual amount deducted
        count_map[denom] = found

    # 3. Generate "Smart Code" (Upgrade Version): Supports up to 99 cards for each denomination
    # Rules: Last 6 digits of HH + $2 quantity (2 digits) + $5 quantity (2 digits) + $10 quantity (2 digits) + Timestamp (4 digits)
    
    # A. Extract the last 6 digits of the ID
    hh_suffix = household_id.replace("H", "")[-6:]
    
    # B. After extracting the ID, extract the quantity of each denomination (using zfill(2) to ensure that each denomination always occupies 2 digits)
    c2 = str(count_map.get(2, 0)).zfill(2)
    c5 = str(count_map.get(5, 0)).zfill(2)
    c10 = str(count_map.get(10, 0)).zfill(2)
    qty_str = f"{c2}{c5}{c10}" # The result will be in the form of "020101" or "100502"
    
    # C. Generate timestamp suffix (keep 4 digits)
    ts = str(int(datetime.now().timestamp()))
    time_suffix = ts[-4:]
    
    # D. Concatenate the final code (with a fixed length of 6 + 6 + 4 = 16 bits)
    smart_code = f"{hh_suffix}{qty_str}{time_suffix}"
            
    # 4. Construct a complete Token string (for the backend verification logic to parse)
    codes_str = ",".join(selected_codes)
    token_str = f"{household_id}+{codes_str}+{ts}"

    # 5. Save to dictionary
    pending_redemptions[smart_code] = {
        "token": token_str,
        "details": vouchers_detail_list,
        "household_id": household_id
    }

    # 6. return data
    return jsonify({
        "status": "ready",
        "short_code": smart_code,           # The response here is the new smart code
        "household_id": household_id,
        "selected_vouchers": vouchers_detail_list,
        "total_amount": sum(v['amount'] for v in vouchers_detail_list)
    })

@app.route("/household/redeem/<household_id>")
def redemption_page(household_id):
    """Renders the redemption UI (Select amounts -> Generate QR)"""
    # 1. verify whether the household has registered
    if household_id not in cdc_system.households:
        # load from the CSV
        basedir = os.path.abspath(os.path.dirname(__file__))
        household_from_csv("", os.path.join(basedir, "Households.csv")) 
    
    if household_id not in cdc_system.households:
        return "Household not found", 404

    hh = cdc_system.households[household_id]
    
    # 2. calculatethe distribution of current available balance
    available_counts = {2: 0, 5: 0, 10: 0}
    
    for t_list in hh.tranches.values():
        for v in t_list:
            if not v.is_redeemed:
                if v.denomination in available_counts:
                    available_counts[v.denomination] += 1

    return render_template(
        "redeem.html", 
        household_id=household_id, 
        available_counts=available_counts,
        total_balance=cdc_system.get_balance(household_id)
    )

# =======================================
# Merchant Registration System
# =======================================

def _normalize(s: str) -> str:
    return (s or "").strip().lower()


# Load bank names from BankCode.csv
def load_bank_names(bankcode_csv: str) -> list[str]:
    if not os.path.isfile(bankcode_csv):
        return []

    names = []
    seen = set()
    with open(bankcode_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("Bank_Name") or "").strip()
            key = _normalize(name)
            if name and key not in seen:
                names.append(name)
                seen.add(key)
    return names

# Lookup (bank_code, branch_code) by Bank_Name
def lookup_bank_codes(bank_name: str, bankcode_csv: str):
    if not os.path.isfile(bankcode_csv):
        return None, None

    target = _normalize(bank_name)
    with open(bankcode_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if _normalize(row.get("Bank_Name")) == target:
                return (
                    str(row.get("Bank_Code") or "").strip(),
                    str(row.get("Branch_Code") or "").strip(),
                )
    return None, None


# Find merchant by UEN from Merchant.csv
def merchant_from_csv_by_uen(uen: str, merchant_csv: str):
    if not os.path.isfile(merchant_csv):
        return None

    target = _normalize(uen)
    with open(merchant_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if _normalize(row.get("UEN")) == target:
                return row
    return None


# Generate next Merchant_ID
def generate_merchant_id(merchant_csv: str) -> str:
    next_num = 1
    if os.path.isfile(merchant_csv):
        with open(merchant_csv, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                mid = (row.get("Merchant_ID") or "").strip()
                if mid.startswith("M") and mid[1:].isdigit():
                    try:
                        num = int(mid[1:])
                        if num >= next_num:
                            next_num = num + 1
                    except ValueError:
                        pass
    return f"M{next_num:03d}"

# Load merchant into in-memory CDCSystem if not already present
def load_merchant_into_memory_if_needed(row: dict):
    merchant_id = (row.get("Merchant_ID") or "").strip()
    if not merchant_id:
        return
    if merchant_id in cdc_system.merchants:
        return

    from data_structure import Merchant

    m_obj = Merchant(
        merchant_id=row.get("Merchant_ID"),
        merchant_name=row.get("Merchant_Name"),
        uen=row.get("UEN"),
        bank_name=row.get("Bank_Name"),
        bank_code=row.get("Bank_Code"),
        branch_code=row.get("Branch_Code"),
        account_number=row.get("Account_Number"),
        account_holder_name=row.get("Account_Holder_Name"),
        registration_date=row.get("Registration_Date"),
        status=row.get("Status"),
    )
    cdc_system.add_merchant(m_obj)


# Convert Merchant.csv row to template friendly dict
def to_template_merchant(row: dict) -> dict:
    return {
        "merchant_id": (row.get("Merchant_ID") or "").strip(),
        "merchant_name": (row.get("Merchant_Name") or "").strip(),
        "uen": (row.get("UEN") or "").strip(),
        "bank_name": (row.get("Bank_Name") or "").strip(),
        "bank_code": (row.get("Bank_Code") or "").strip(),
        "branch_code": (row.get("Branch_Code") or "").strip(),
        "account_number": (row.get("Account_Number") or "").strip(),
        "account_holder_name": (row.get("Account_Holder_Name") or "").strip(),
        "registration_date": (row.get("Registration_Date") or "").strip(),
        "status": (row.get("Status") or "").strip(),
    }


# home route to render registration form
@app.route("/merchant", methods=["GET"])
def merchant_home():
    basedir = os.path.abspath(os.path.dirname(__file__))
    bankcode_csv = os.path.join(basedir, "BankCode.csv")
    bank_names = load_bank_names(bankcode_csv)
    return render_template("register_merchant.html", bank_names=bank_names)


# Merchant registration route
@app.route("/merchant/register", methods=["POST"])
def register_merchant():
    merchant_name = (request.form.get("merchant_name") or "").strip()
    uen = (request.form.get("uen") or "").strip()
    bank_name = (request.form.get("bank_name") or "").strip()  
    account_number = (request.form.get("account_number") or "").strip()
    account_holder_name = (request.form.get("account_holder_name") or "").strip()

    if not (merchant_name and uen and bank_name and account_number and account_holder_name):
        return render_template(
            "merchant_success.html",
            message="Missing required fields. Please fill in all fields.",
            merchant=None
        ), 400

    basedir = os.path.abspath(os.path.dirname(__file__))
    bankcode_csv = os.path.join(basedir, "BankCode.csv")
    merchant_csv = os.path.join(basedir, "Merchant.csv") 

    # Validate bank_name: must be one of the banks in BankCode.csv
    bank_names = load_bank_names(bankcode_csv)
    if bank_name not in bank_names:
        return render_template(
            "merchant_success.html",
            message="Invalid bank selection. Please choose a bank from the dropdown list.",
            merchant=None
        ), 400

    # Check existing merchant by UEN
    existing = merchant_from_csv_by_uen(uen, merchant_csv)
    if existing:
        load_merchant_into_memory_if_needed(existing)
        return render_template(
            "merchant_success.html",
            message=f"{merchant_name}, your merchant account has already been registered.",
            merchant=to_template_merchant(existing)
        ), 200

    # Lookup bank code and branch code
    bank_code, branch_code = lookup_bank_codes(bank_name, bankcode_csv)
    if not bank_code or not branch_code:
        return render_template(
            "merchant_success.html",
            message=f"Bank codes not found for selected bank: {bank_name}",
            merchant=None
        ), 400

    # Create new record 
    merchant_id = generate_merchant_id(merchant_csv)
    registration_date = datetime.now().strftime("%Y-%m-%d")
    status = "Active"

    record = {
        "Merchant_ID": merchant_id,
        "Merchant_Name": merchant_name,
        "UEN": uen,
        "Bank_Name": bank_name,
        "Bank_Code": bank_code,
        "Branch_Code": branch_code,
        "Account_Number": account_number,
        "Account_Holder_Name": account_holder_name,
        "Registration_Date": registration_date,
        "Status": status
    }

    # Append to Merchant.csv
    file_exists = os.path.isfile(merchant_csv)
    fieldnames = list(record.keys())
    with open(merchant_csv, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(record)

    # Load into in-memory CDCSystem
    load_merchant_into_memory_if_needed(record)

    return render_template(
        "merchant_success.html",
        message=f"{merchant_name}, your merchant account has been successfully registered!",
        merchant=to_template_merchant(record)
    ), 201


# return bank names from BankCode.csv
@app.get("/api/banks")
def api_list_banks():
    basedir = os.path.abspath(os.path.dirname(__file__))
    bankcode_csv = os.path.join(basedir, "BankCode.csv")
    bank_names = load_bank_names(bankcode_csv)
    return jsonify({
        "success": True,
        "count": len(bank_names),
        "banks": bank_names
    }), 200

# Merchant registration API
@app.post("/api/merchant/register")
def api_register_merchant():
    data = request.get_json(silent=True) or {}

    merchant_name = (data.get("merchant_name") or "").strip()
    uen = (data.get("uen") or "").strip()
    bank_name = (data.get("bank_name") or "").strip()
    account_number = (data.get("account_number") or "").strip()
    account_holder_name = (data.get("account_holder_name") or "").strip()

    if not (merchant_name and uen and bank_name and account_number and account_holder_name):
        return jsonify({
            "success": False,
            "error": "Missing required fields",
            "required": ["merchant_name", "uen", "bank_name", "account_number", "account_holder_name"]
        }), 400

    basedir = os.path.abspath(os.path.dirname(__file__))
    bankcode_csv = os.path.join(basedir, "BankCode.csv")
    merchant_csv = os.path.join(basedir, "Merchant.csv")

    # Validate bank selection
    bank_names = load_bank_names(bankcode_csv)
    if bank_name not in bank_names:
        return jsonify({
            "success": False,
            "error": "Invalid bank selection",
            "allowed_banks": bank_names
        }), 400

    # Existing merchant by UEN
    existing = merchant_from_csv_by_uen(uen, merchant_csv)
    if existing:
        load_merchant_into_memory_if_needed(existing)
        return jsonify({
            "success": True,
            "is_new": False,
            "message": f"{merchant_name}, your merchant account has already been registered.",
            "merchant_csv": existing,
            "merchant_view": to_template_merchant(existing)
        }), 200

    bank_code, branch_code = lookup_bank_codes(bank_name, bankcode_csv)
    if not bank_code or not branch_code:
        return jsonify({
            "success": False,
            "error": f"Bank codes not found for selected bank: {bank_name}"
        }), 400

    merchant_id = generate_merchant_id(merchant_csv)
    registration_date = datetime.now().strftime("%Y-%m-%d")
    status = "Active"

    record = {
        "Merchant_ID": merchant_id,
        "Merchant_Name": merchant_name,
        "UEN": uen,
        "Bank_Name": bank_name,
        "Bank_Code": bank_code,
        "Branch_Code": branch_code,
        "Account_Number": account_number,
        "Account_Holder_Name": account_holder_name,
        "Registration_Date": registration_date,
        "Status": status
    }

    file_exists = os.path.isfile(merchant_csv)
    fieldnames = list(record.keys())
    with open(merchant_csv, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(record)

    load_merchant_into_memory_if_needed(record)

    return jsonify({
        "success": True,
        "is_new": True,
        "message": f"{merchant_name}, your merchant account has been successfully registered!",
        "merchant_csv": record,
        "merchant_view": to_template_merchant(record)
    }), 201


# Get merchant by Merchant_ID
@app.get("/api/merchant/<merchant_id>")
def api_get_merchant(merchant_id: str):
    merchant_id = (merchant_id or "").strip()
    if not merchant_id:
        return jsonify({"success": False, "error": "Missing merchant_id"}), 400

    # 1) In-memory lookup
    if merchant_id in cdc_system.merchants:
        m = cdc_system.merchants[merchant_id]
        row = {
            "Merchant_ID": m.merchant_id,
            "Merchant_Name": m.merchant_name,
            "UEN": m.uen,
            "Bank_Name": m.bank_name,
            "Bank_Code": m.bank_code,
            "Branch_Code": m.branch_code,
            "Account_Number": m.account_number,
            "Account_Holder_Name": m.account_holder_name,
            "Registration_Date": m.registration_date,
            "Status": m.status
        }
        return jsonify({
            "success": True,
            "merchant_csv": row,
            "merchant_view": to_template_merchant(row)
        }), 200

    # 2) Load from CSV if exists
    basedir = os.path.abspath(os.path.dirname(__file__))
    merchant_csv = os.path.join(basedir, "Merchant.csv")
    if not os.path.isfile(merchant_csv):
        return jsonify({"success": False, "error": "Merchant.csv not found"}), 404

    with open(merchant_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("Merchant_ID") or "").strip() == merchant_id:
                load_merchant_into_memory_if_needed(row)
                return jsonify({
                    "success": True,
                    "merchant_csv": row,
                    "merchant_view": to_template_merchant(row)
                }), 200

    return jsonify({"success": False, "error": "Merchant not found"}), 404

@app.route("/api/mobile/dashboard/<household_id>", methods=["GET"])
def api_mobile_dashboard(household_id):
    """
    Provide homepage data for the mobile version: balance, batch status, available coupon statistics
    """
    if household_id not in cdc_system.households:
        # If it is not available in the memory, try loading from CSV and restoring the state.
        basedir = os.path.abspath(os.path.dirname(__file__))
        household_from_csv(household_id, os.path.join(basedir, "Households.csv"))
    
    if household_id not in cdc_system.households:
        return jsonify({"success": False, "message": "Household not found"}), 404

    hh = cdc_system.households[household_id]
    
    # Count the number of available coupons
    available_counts = {2: 0, 5: 0, 10: 0}
    for t_list in hh.tranches.values():
        for v in t_list:
            if not v.is_redeemed:
                available_counts[v.denomination] += 1

    # Batch collection status
    TRANCHES_CONFIG = {
        "2025_may": {"name": "CDC 2025 (May)", "total": 500},
        "2026_jan": {"name": "CDC 2026 (Jan)", "total": 300}
    }
    tranches_info = []
    for key, cfg in TRANCHES_CONFIG.items():
        tranches_info.append({
            "key": key,
            "name": cfg["name"],
            "total": cfg["total"],
            "is_claimed": key in hh.claimed_tranches
        })

    return jsonify({
        "success": True,
        "household_id": household_id,
        "total_balance": cdc_system.get_balance(household_id),
        "available_counts": available_counts,
        "tranches": tranches_info
    })

@app.route("/api/merchant/redeem_by_code", methods=["POST"])
def api_merchant_redeem_by_code():
    """
    Step 2: Merchant enters the redemption code (intelligent short code) -> Perform redemption -> Return the summary list of denominations
    """
    data = request.get_json()
    merchant_id = data.get("merchant_id")
    # This is receiving a new long write-off code.
    short_code = data.get("barcode_number") 

    # 1. Search for and pop the dictionary object in the pending processing queue
    # (Note: If the server has been restarted, the previous memory data "pending_redemptions" will be cleared. An error 404 will be reported here.)
    if short_code not in pending_redemptions:
        return jsonify({"success": False, "error": "Invalid or Expired Code"}), 404

    # Extract the original token
    redemption_data = pending_redemptions.pop(short_code)
    full_token = redemption_data["token"] 
    
    # 2. Parsing Token
    try:
        parts = full_token.split('+')
        hh_id = parts[0]
        codes_to_redeem = parts[1].split(',')
    except:
        return jsonify({"success": False, "error": "Internal Token Error"}), 500

    # === Added a robustness step: Preventing the home application from not being loaded into memory after the server restarts. ===
    if hh_id not in cdc_system.households:
        basedir = os.path.abspath(os.path.dirname(__file__))
        household_from_csv(hh_id, os.path.join(basedir, "Households.csv"))

    # 3. Execute the core write-off logic
    results, msg = cdc_system.redeem(hh_id, merchant_id, codes_to_redeem)
    
    # === Implement core write-off logic core repair 1: Handle failure scenarios (Missing Else Block) ===
    # If the "results" is None, it indicates that the write-off has failed (such as the family not existing, the coupon being invalid, etc.)
    # It is necessary to explicitly return the error; otherwise, Flask will report a 500 error.
    if results is None:
        return jsonify({"success": False, "error": msg}), 400

    # 4. Debiting operation successfully processed
    # A. Persist and record to CSV
    log_redemption_csv(results)
    # B. Update family balance
    new_balance = cdc_system.get_balance(hh_id)
    update_household_balance_csv(hh_id, new_balance)
    
    # C. Statistical write-off list
    breakdown = {}
    total_val = 0.0
    
    for r in results:
        # Core Fix 2: Prevent double '$' characters from appearing in amount display (e.g. $$10.00)
        # The data_structure.py script already returns the result in the "$10.00" format.
        val_str = str(r['Denomination_Used']) 
        
        if val_str.startswith('$'):
            denom_key = val_str
        else:
            denom_key = f"${val_str}"
            
        breakdown[denom_key] = breakdown.get(denom_key, 0) + 1
        
        # Calculate the total amount
        denom_clean = val_str.replace('$', '')
        total_val += float(denom_clean)
    
    return jsonify({
        "success": True, 
        "message": "Transaction Successful!",
        "household_id": hh_id,
        "breakdown": breakdown,
        "total_amount": total_val 
    })
    
# =========================
# Compatibility routes (no-prefix)
# =========================

@app.get("/vouchers/<household_id>")
def vouchers_alias(household_id):
    return redirect(f"/household/vouchers/{household_id}")


@app.get("/redeem/<household_id>")
def redeem_alias(household_id):
    return redirect(f"/household/redeem/{household_id}")


@app.post("/api/voucher/claim")
def api_claim_vouchers_alias():
    # Call the household-prefixed handler
    return api_claim_vouchers()


@app.post("/api/redemption/generate")
def generate_redemption_token_alias():
    return generate_redemption_token()


@app.post("/api/redemption/claim")
def merchant_claim_process_alias():
    return merchant_claim_process()

def sync_max_tx_id():
    """
    Synchronizes the global transaction counter by scanning all storage zones.
    This prevents Transaction ID collisions by identifying the highest existing ID 
    in both active and archived log files.
    """
    # Initialize with the base starting ID (Requirement: TX1001 series)
    max_id = 1000
    basedir = os.path.abspath(os.path.dirname(__file__))
    
    # Define search paths across the entire data lifecycle: Active, Staging, and Archived
    search_paths = [
        os.path.join(basedir, "Redeem*.csv"),                   # Logs in root
        os.path.join(basedir, "redemptions", "Redeem*.csv"),    # Pending settlement
        os.path.join(basedir, "processed_logs", "Redeem*.csv")  # Processed archives
    ]
    
    # Aggregate all matching file paths from the defined zones
    all_files = []
    for path in search_paths:
        all_files.extend(glob.glob(path))

    # Parse every discovered file to extract and compare transaction IDs
    for f in all_files:
        try:
            with open(f, newline="", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    # Strip the "TX" prefix to isolate the numeric ID for comparison
                    # Example: "TX1234" -> "1234"
                    tid_str = row["Transaction_ID"].replace("TX", "")
                    if tid_str.isdigit():
                        # Update the counter if a higher ID is found
                        max_id = max(max_id, int(tid_str))
        except Exception as e:
            # Silently skip unreadable files to maintain system uptime
            print(f"Warning: Skipping {f} during ID sync due to error: {e}")
            continue
            
    # Apply the highest discovered ID to the system's global counter
    cdc_system.tx_counter = max_id
    print(f">>> Global TX Counter synchronized to: {cdc_system.tx_counter}")

sync_max_tx_id()

if __name__ == "__main__":
    print(">>> Starting CDC System Server...")
    app.run(debug=True, use_reloader=False, port=5001)