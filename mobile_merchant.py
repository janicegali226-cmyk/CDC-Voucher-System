import flet as ft
import requests

# Make sure that api.py is running on port 5001.
BASE_URL = "http://127.0.0.1:5001"

def main(page: ft.Page):
    page.title = "CDC Merchant Terminal"
    page.window_width = 390
    page.window_height = 844
    page.theme_mode = ft.ThemeMode.LIGHT
    page.horizontal_alignment = ft.CrossAxisAlignment.CENTER
    page.vertical_alignment = ft.MainAxisAlignment.CENTER

    # Store the current status of the currently logged-in merchant
    merchant_state = {"id": None, "name": ""}

    # ==========================================
    # Interface 2: Redemption Operation Interface
    # ==========================================
    def show_redeem_view():
        page.clean()
        page.vertical_alignment = ft.MainAxisAlignment.START
        
        page.appbar = ft.AppBar(
            title=ft.Text(f"Store: {merchant_state['name']}"), 
            bgcolor="blue", 
            color="white"
        )
        
        # 1. Core input box: Change to 16 digits
        code_input = ft.TextField(
            label="Enter Redemption Code", 
            text_align="center", 
            text_size=20,
            width=300,
            keyboard_type="number",
            max_length=16
        )
        
        status_msg = ft.Text()
        
        # 2. Result display container: Used to show the detailed denomination distribution after write-off.
        breakdown_container = ft.Column(horizontal_alignment="center", spacing=10)
        
        def on_confirm_click(e):
            if not code_input.value:
                status_msg.value = "Please enter the code"
                status_msg.color = "red"
                page.update()
                return
            
            # Clear the previous write-off results
            breakdown_container.controls.clear()
            status_msg.value = "Processing..."
            page.update()
            
            try:
                # Call the 16-bit short code verification interface of the backend
                res = requests.post(f"{BASE_URL}/api/merchant/redeem_by_code", 
                                 json={
                                     "merchant_id": merchant_state["id"], 
                                     "barcode_number": code_input.value
                                 })
                data = res.json()
                
                if data.get("success"):
                    status_msg.value = "TRANSACTION SUCCESSFUL!"
                    status_msg.color = "green"
                    status_msg.weight = "bold"
                    
                    # A. Display the family ID
                    breakdown_container.controls.append(
                        ft.Text(f"From Household: {data['household_id']}", size=16, weight="bold")
                    )
                    
                    # B. Automatically generate a list of denomination statistics
                    # Backend returns data format: "breakdown": {"$2": 3, "$10": 1}
                    breakdown_container.controls.append(ft.Text("Vouchers Received:", color="grey"))
                    
                    for denom, count in data['breakdown'].items():
                        breakdown_container.controls.append(
                            ft.Container(
                                content=ft.Row([
                                    ft.Text(denom, size=18, weight="bold"),
                                    ft.Text(f"x {count}", size=18)
                                ], alignment="spaceBetween"),
                                padding=10, bgcolor="#F5F5F5", border_radius=8, width=280
                            )
                        )
                    
                    # C. Display the total amount
                    breakdown_container.controls.append(ft.Divider())
                    breakdown_container.controls.append(
                        ft.Text(f"Total Amount: ${data['total_amount']}", size=24, color="blue", weight="bold")
                    )
                    
                    code_input.value = "" # Clear the input field for future use.
                else:
                    status_msg.value = f"FAILED: {data.get('error')}"
                    status_msg.color = "red"
                    
            except Exception as ex:
                status_msg.value = f"Connection Error: {ex}"
                status_msg.color = "red"
                
            page.update()

        page.add(
            ft.Container(height=20),
            ft.Text(f"Merchant ID: {merchant_state['id']}", color="grey"),
            ft.Divider(),
            ft.Container(height=20),
            ft.Text("Ask customer for the redemption code:", size=16),
            code_input,
            ft.ElevatedButton(
                "CONFIRM REDEMPTION", 
                on_click=on_confirm_click, 
                bgcolor="green", color="white", width=300, height=50
            ),
            status_msg,
            breakdown_container, # Dynamic write-off result display area
            ft.Container(height=20),
            ft.TextButton("Logout", on_click=lambda _: main(page))
        )
        page.update()

    # ==========================================
    # Interface 1: Merchant Login Interface (Login)
    # ==========================================
    page.clean()
    page.vertical_alignment = ft.MainAxisAlignment.CENTER
    
    title = ft.Text("Merchant Login", size=28, weight="bold", color="blue")
    login_id_input = ft.TextField(label="Enter Merchant ID (e.g., M001)", width=300)
    login_error = ft.Text()

    def do_merchant_login(e):
        mid = login_id_input.value.strip()
        if not mid: return
        
        try:
            res = requests.get(f"{BASE_URL}/api/merchant/{mid}")
            if res.status_code == 200:
                data = res.json()
                merchant_state["id"] = mid
                merchant_state["name"] = data["merchant_view"]["merchant_name"]
                show_redeem_view() # Login successful. Entering the verification page.
            else:
                login_error.value = "Invalid Merchant ID!"
                login_error.color = "red"
                page.update()
        except Exception:
            login_error.value = "API Server is offline!"
            page.update()

    page.add(
        ft.Text("CDC MERCHANT TERMINAL", size=14, color="grey"),
        title,
        ft.Container(height=20),
        login_id_input,
        ft.ElevatedButton(
            "LOGIN", 
            on_click=do_merchant_login, 
            width=300, height=50, bgcolor="blue", color="white"
        ),
        login_error
    )
    page.update()

# Start the merchant end
if __name__ == "__main__":
    ft.app(target=main)