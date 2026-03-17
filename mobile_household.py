import flet as ft
import requests
from datetime import datetime

# Make sure that your api.py is running on port 5001.
BASE_URL = "http://127.0.0.1:5001"

def main(page: ft.Page):
    page.title = "CDC Mobile Portal"
    page.window_width = 390
    page.window_height = 844
    page.theme_mode = ft.ThemeMode.LIGHT
    page.horizontal_alignment = ft.CrossAxisAlignment.CENTER
    page.scroll = ft.ScrollMode.AUTO

    # --- 1. Coupon redemption logic ---
    def claim_voucher(hh_id, tranche_key):
        try:
            requests.post(f"{BASE_URL}/household/api/voucher/claim", 
                         json={"household_id": hh_id, "tranche": tranche_key})
            refresh_data(hh_id)
        except Exception as e:
            print(f"Claim Error: {e}")

    # --- 2. Data refresh logic ---
    def refresh_data(hh_id):
        try:
            res = requests.get(f"{BASE_URL}/api/mobile/dashboard/{hh_id}")
            if res.status_code == 200:
                d = res.json()
                show_dashboard(d["household_id"], d["total_balance"], d["tranches"], d["available_counts"])
        except Exception as e:
            print(f"Refresh Error: {e}")

    # --- 3. Redeem Selection Interface ---
    def show_redeem_view(hh_id, available_counts):
        page.clean()
        page.appbar = ft.AppBar(title=ft.Text("Voucher Redemption"), bgcolor="blue", color="white")
        
        counts = {2: 0, 5: 0, 10: 0}
        
        # Result display container
        result_container = ft.Column(horizontal_alignment="center", spacing=15, visible=False)
        # Selection Region
        selection_area = ft.Column(horizontal_alignment="center", spacing=10)

        def update_val(denom, delta, text_obj):
            new_val = counts[denom] + delta
            limit = available_counts.get(str(denom), 0)
            if 0 <= new_val <= limit:
                counts[denom] = new_val
                text_obj.value = str(new_val)
                page.update()

        def build_row(denom):
            t = ft.Text("0", size=25, weight="bold")
            return ft.Row([
                ft.Text(f"${denom}", size=20, width=50),
                ft.ElevatedButton("-", on_click=lambda _: update_val(denom, -1, t), width=50),
                t,
                ft.ElevatedButton("+", on_click=lambda _: update_val(denom, 1, t), width=50),
            ], alignment="center")

        def on_generate(e):
            if sum(counts.values()) == 0: return
            
            e.control.disabled = True
            e.control.text = "GENERATING..."
            page.update()

            try:
                res = requests.post(f"{BASE_URL}/household/api/redemption/generate", 
                                 json={"household_id": hh_id, "selected_items": counts})
                data = res.json()
                
                if "error" in data:
                    raise Exception(data["error"])

                # --- Rendering result area ---
                result_container.controls.clear()
                
                # A. Obvious short code (fixed: removed the "letter_spacing" that caused the error)
                result_container.controls.append(ft.Text("SHOW TO MERCHANT:", size=14, color="grey"))
                result_container.controls.append(
                    ft.Container(
                        content=ft.Text(data['short_code'], size=45, weight="bold", color="blue"),
                        padding=15, border=ft.border.all(2, "blue"), border_radius=10, bgcolor="#F0F4F8"
                    )
                )
                
                # B. Details of the list
                details_box = ft.Column([
                    ft.Text(f"Household ID: {data['household_id']}", weight="bold", size=16),
                    ft.Divider(height=10),
                    ft.Text("Vouchers Selected:", size=14, color="grey"),
                ])
                
                for v in data['selected_vouchers']:
                    details_box.controls.append(
                        ft.Row([
                            ft.Text(f"ID: {v['id']}", size=12, font_family="monospace"),
                            ft.Text(f"${v['amount']}", weight="bold", color="green")
                        ], alignment="spaceBetween", width=320)
                    )
                
                result_container.controls.append(
                    ft.Container(content=details_box, padding=15, bgcolor="#FAFAFA", border_radius=8, width=350)
                )
                
                # C. Show total sum
                result_container.controls.append(
                    ft.Text(f"TOTAL VALUE: ${data['total_amount']}", size=24, weight="bold", color="red")
                )
                
                # D. Finish Button
                result_container.controls.append(
                    ft.ElevatedButton("DONE", on_click=lambda _: refresh_data(hh_id), 
                                     width=300, bgcolor="blue", color="white", height=50)
                )

                selection_area.visible = False
                result_container.visible = True
                
            except Exception as ex:
                page.snack_bar = ft.SnackBar(ft.Text(f"Error: {ex}"), bgcolor="red")
                page.snack_bar.open = True
                e.control.disabled = False
                e.control.text = "CONFIRM & GENERATE CODE"
            
            page.update()

        selection_area.controls.extend([
            ft.Text("Select vouchers to spend:", size=16),
            build_row(2), build_row(5), build_row(10),
            ft.Container(height=20),
            ft.ElevatedButton("CONFIRM & GENERATE CODE", on_click=on_generate, 
                             width=300, bgcolor="red", color="white", height=50),
        ])

        page.add(
            ft.Container(height=20),
            selection_area,
            result_container,
            ft.TextButton("Back", on_click=lambda _: refresh_data(hh_id))
        )
        page.update()

    # --- 4. Main Interface (Dashboard) ---
    def show_dashboard(hh_id, balance, tranches, available_counts):
        page.clean()
        page.appbar = ft.AppBar(title=ft.Text("My CDC Wallet"), bgcolor="blue", color="white")
        
        balance_card = ft.Container(
            content=ft.Column([
                ft.Text("Total Balance", color="white"),
                ft.Text(f"${balance:.2f}", size=40, weight="bold", color="white"),
            ]),
            padding=30, bgcolor="green", border_radius=15, width=350
        )

        tranche_ui = ft.Column(spacing=10)
        for t in tranches:
            def handle_claim(e, key=t['key']):
                claim_voucher(hh_id, key)

            tranche_ui.controls.append(
                ft.Row([
                    ft.Text(t['name'], expand=True),
                    ft.ElevatedButton("Claim", on_click=handle_claim) 
                    if not t['is_claimed'] else ft.Text("CLAIMED ✓", color="green", weight="bold")
                ])
            )

        page.add(
            ft.Container(height=10),
            balance_card,
            ft.Text("Voucher Schemes:", weight="bold"),
            tranche_ui,
            ft.Divider(),
            ft.ElevatedButton("SPEND NOW", bgcolor="red", color="white", width=350, height=50,
                             on_click=lambda _: show_redeem_view(hh_id, available_counts)),
            ft.TextButton("Logout", on_click=lambda _: main(page))
        )
        page.update()

    # --- 5. Initial login interface ---
    title = ft.Text("CDC VOUCHERS", size=30, weight="bold", color="blue")
    id_input = ft.TextField(label="Enter Household ID", width=300)
    status_msg = ft.Text()

    def on_login(e):
        try:
            res = requests.post(f"{BASE_URL}/api/mobile/login", json={"search_input": id_input.value})
            if res.status_code == 200:
                refresh_data(res.json()["household_id"])
            else:
                status_msg.value = "ID not found!"
                page.update()
        except Exception as ex:
            status_msg.value = f"API Offline"
            page.update()

    page.add(ft.Container(height=100), title, id_input, 
             ft.ElevatedButton("LOGIN", on_click=on_login, width=300), status_msg)
    page.update()

if __name__ == "__main__":
    ft.app(target=main)