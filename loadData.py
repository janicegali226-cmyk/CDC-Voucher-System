"""
loadData.py
--------------------------------------------------
Student Name: Tang Yiwei
Admin Number: G2500285L
"""

import pandas as pd
import os
import glob 
from typing import Optional, List, Dict, Any, Union

def load_bank_codes(file_path: str = "Bankcode.csv") -> Dict[str, str]:
    """Task (iii): Loads bank metadata to retrieve SWIFT codes."""
    try:
        df = pd.read_csv(file_path, dtype={'Bank_Code': str})
        return df.set_index('Bank_Code')['SWIFT_Code'].to_dict()
    except Exception as e:
        print(f"Error loading bank codes: {e}")
        return {}

def load_redemptions_data(file_path: str) -> Optional[pd.DataFrame]:
    """Loads a single CSV file and performs data cleaning (Task iii)."""
    try:
        df = pd.read_csv(file_path, dtype={
            'Merchant_ID': str, 
            'Voucher_Code': str,
            'Transaction_ID': str
        })

        # --- ENHANCED CLEANING ---
        # 处理可能存在的 $ 符号和千分位逗号
        for col in ['Denomination_Used', 'Amount_Redeemed']:
            if col in df.columns:
                if df[col].dtype == object: # 如果是字符串
                    df[col] = df[col].str.replace('$', '', regex=False)\
                                     .str.replace(',', '', regex=False)\
                                     .astype(float)
                else:
                    df[col] = df[col].astype(float)

        return df
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

# --- CRITICAL UPDATE: 适应 api.py 生成的文件分布 ---
def load_all_redemptions_from_folder(base_path: str = ".") -> Optional[pd.DataFrame]:
    """
    Scans for all Redeem*.csv files in the specified directory.
    Default search path is the current root ('.') where api.py generates files.
    """
    all_dfs = []
    try:
        # 自动搜索所有以 'Redeem' 开头并以 '.csv' 结尾的文件
        files = glob.glob(os.path.join(base_path, "Redeem*.csv"))
        
        if not files:
            print(f"No redemption files (Redeem*.csv) found in {base_path}")
            return None

        for file in files:
            df = load_redemptions_data(file)
            if df is not None:
                all_dfs.append(df)
        
        return pd.concat(all_dfs, ignore_index=True) if all_dfs else None
    except Exception as e:
        print(f"Error scanning for redemption files: {e}")
        return None

# --- UPDATE: Filename changed to 'Merchant.csv' (Uppercase M) ---
def load_merchant_info(file_path: str = "Merchant.csv", bank_swift_map: Optional[Dict[str, str]] = None) -> Optional[pd.DataFrame]:
    """Task (iii): Loads merchant data from production Merchant.csv."""
    try:
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return None
            
        df = pd.read_csv(file_path, dtype={
            'Merchant_ID': str,
            'Bank_Code': str,
            'Branch_Code': str
        })
        
        # 数据清洗：仅保留活跃商户
        df = df[df['Status'] == 'Active'].copy()

        # 挂载 SWIFT 代码
        if bank_swift_map is not None:
            df['SWIFT_Code'] = df['Bank_Code'].map(bank_swift_map)
            
        return df
    except Exception as e:
        print(f"Error loading merchant data: {e}")
        return None