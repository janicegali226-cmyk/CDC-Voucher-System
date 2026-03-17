import datetime
from typing import List, Dict, Optional, Tuple

# ============================================================
# 1. Voucher Class
# ============================================================
class Voucher:
    def __init__(self, voucher_code: str, denomination: int, tranche_name: str):
        self.voucher_code = voucher_code
        self.denomination = denomination
        self.tranche_name = tranche_name
        self.is_redeemed = False

# ============================================================
# 2. Household Account Class
# ============================================================
class Household:
    def __init__(self, household_id: str):
        self.household_id = household_id
        self.tranches: Dict[str, List[Voucher]] = {}
        self.cached_balance = 0.0  # Satisfies Requirement 1: High-speed retrieval
        self.claimed_tranches = set()

    def update_balance(self):
        """Real-time balance update to ensure accuracy for API (d)"""
        total = 0
        for vouchers in self.tranches.values():
            total += sum(v.denomination for v in vouchers if not v.is_redeemed)
        self.cached_balance = float(total)
        return self.cached_balance
    
# ============================================================
# 3. Merchant Class (Strictly matches Merchant.csv metadata)
# ============================================================
class Merchant:
    def __init__(self, merchant_id, merchant_name, uen, bank_name, bank_code, 
                 branch_code, account_number, account_holder_name, registration_date, status):
        self.merchant_id = merchant_id
        self.merchant_name = merchant_name
        self.uen = uen
        self.bank_name = bank_name
        self.bank_code = bank_code
        self.branch_code = branch_code
        self.account_number = account_number
        self.account_holder_name = account_holder_name
        self.registration_date = registration_date
        self.status = status

# ============================================================
# 4. CDC Management System (Core Logic Entry Point)
# ============================================================
class CDCSystem:
    def __init__(self):
        self.households: Dict[str, Household] = {}
        self.merchants: Dict[str, Merchant] = {}
        self.tx_counter = 1000  # Used to simulate TX1001 series IDs

    # Support for API (a) & (b)
    def add_household(self, h_id: str) -> bool:
        if h_id in self.households: return False
        self.households[h_id] = Household(h_id)
        return True

    def add_merchant(self, m_obj: Merchant) -> bool:
        self.merchants[m_obj.merchant_id] = m_obj
        return True

    # Support for API (c): Multi-tranche voucher claiming
    def claim_vouchers(self, h_id: str, tranche: str, breakdown: Dict[int, int]) -> bool:
        hh = self.households.get(h_id)
        if not hh or tranche in hh.claimed_tranches: return False
        
        # 1. Calculate how many vouchers the household has already claimed
        total_existing = sum(len(v_list) for v_list in hh.tranches.values())
        
        # --- Debug Prints: Visualizing the counting process in terminal ---
        print(f"\n[DEBUG] Issuing tranche: {tranche} for household {h_id}")
        print(f"[DEBUG] Previous voucher count for this household: {total_existing}")
        # ------------------------------------

        new_v_list = []
        for denom, count in breakdown.items():
            for _ in range(count):
                # Core logic: Serial = previous total + current batch count + 1
                current_serial = total_existing + len(new_v_list) + 1
                v_code = f"V{current_serial:07d}"
                new_v_list.append(Voucher(v_code, denom, tranche))
        
        hh.tranches[tranche] = new_v_list
        hh.claimed_tranches.add(tranche)
        hh.update_balance()
        
        print(f"[DEBUG] Issuance complete. Last voucher code: {new_v_list[-1].voucher_code}")
        return True

    # Support for API (d): High-speed balance retrieval
    def get_balance(self, h_id: str) -> float:
        hh = self.households.get(h_id)
        return hh.cached_balance if hh else 0.0

    # Support for API (e): Generate output matching sample requirements
    def redeem(self, h_id: str, m_id: str, codes: List[str]) -> Tuple[Optional[List[dict]], str]:
        hh = self.households.get(h_id)
        if not hh: return None, "Household not found"
        
        to_redeem = []
        total_amt = 0
        all_v = {v.voucher_code: v for t in hh.tranches.values() for v in t}
        
        for c in codes:
            v = all_v.get(c)
            if not v or v.is_redeemed: return None, f"Voucher {c} is invalid"
            to_redeem.append(v)
            total_amt += v.denomination

        # Generate transaction data
        self.tx_counter += 1
        tx_id = f"TX{self.tx_counter}"
        dt = datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")
        
        output = []
        for i, v in enumerate(to_redeem):
            v.is_redeemed = True
            # Implement Remarks serialization: 1, 2, ... Final denomination used
            rem = str(i + 1) if i < len(to_redeem) - 1 else "Final denomination used"
            
            # Content matches the Redemptions.csv format required by the API
            output.append({
                "Transaction_ID": tx_id,
                "Household_ID": h_id,
                "Merchant_ID": m_id,
                "Transaction_Date_Time": dt,
                "Voucher_Code": v.voucher_code,
                "Denomination_Used": f"${float(v.denomination):.2f}",
                "Amount_Redeemed": f"${float(total_amt):.2f}",
                "Payment_Status": "Completed",
                "Remarks": rem
            })
        
        hh.update_balance()
        return output, "Success"