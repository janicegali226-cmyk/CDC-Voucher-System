# CDC-Voucher-System

# Description 
A system for Singaporean residents and merchants. Residents can register their household to claim and redeem CDC vouchers, whereas merchants can also register a merchant account as participants in the program.

# Built With 
Python 3.13
Flask 3.0.0
Pillow 10.2.0
flet 0.28.3
Flask-CORS 6.0.2

# Dependencies
The required python libraries should be installed prior to running the program files.
These are all compiled into a requirements file. Before installation, please make sure to be in the same directory as the txt file..
To install, run the following in the terminal:
pip install -r requirements.txt

# Executing Program
Execute Web Application: Run api.py and enter the website link from the terminal into a browser.
Execute Mobile Application: Firstly run api.py, then split the terminal, and run mobile_household.py and mobile_merchant.py in the two respective split terminals.

# Usage
For usage in registration of a household account or registration of a merchant account.
After registration, household accounts can claim CDC vouchers and redeem for usage at a registered merchant. 
For household registration, the user must fill in the user’s name, email, residential address, and household members.
Registered households can log in using the household ID or residential address. The available voucher amount is displayed, and the user can select how much to use in amounts of $2, $5, and $10 vouchers. 
Merchants can login using their merchant ID. Once logged in, they can input the voucher code to claim the amount from the user’s account.
