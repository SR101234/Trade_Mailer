import pandas as pd
from datetime import timedelta
import random
import string
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.units import inch
from reportlab.lib.utils import ImageReader
import os
import threading
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

import customtkinter as ctk
from tkinter import filedialog

# --- 1. SET UP THE MODERN GUI THEME ---
ctk.set_appearance_mode("Dark")  # Modes: "System" (standard), "Dark", "Light"
ctk.set_default_color_theme("blue")  # Themes: "blue" (standard), "green", "dark-blue"


class OrderGeneratorApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("PDF Order Generator")
        self.geometry("650x550")
        self.resizable(False, False)

        # Variables to store file paths
        self.trade_file_path = ctk.StringVar()
        self.email_file_path = ctk.StringVar()
        self.template_file_path = ctk.StringVar()
        self.output_dir_path = ctk.StringVar(value=os.path.join(os.getcwd(), "Generated_Orders"))

        self.create_widgets()

    def create_widgets(self):
        # Title
        title_label = ctk.CTkLabel(self, text="PDF Order Generator", font=ctk.CTkFont(size=24, weight="bold"))
        title_label.pack(pady=(20, 20))

        # Main Frame for Inputs
        main_frame = ctk.CTkFrame(self)
        main_frame.pack(pady=10, padx=20, fill="x")

        # --- Helper function to create file input rows ---
        def create_file_row(parent, label_text, string_var, browse_command):
            row_frame = ctk.CTkFrame(parent, fg_color="transparent")
            row_frame.pack(fill="x", pady=10, padx=10)
            
            label = ctk.CTkLabel(row_frame, text=label_text, width=120, anchor="w")
            label.pack(side="left")
            
            entry = ctk.CTkEntry(row_frame, textvariable=string_var, width=320, state="disabled")
            entry.pack(side="left", padx=10)
            
            btn = ctk.CTkButton(row_frame, text="Browse", width=80, command=browse_command)
            btn.pack(side="left")

        # Add the 4 input rows
        create_file_row(main_frame, "Trade Excel:", self.trade_file_path, self.browse_trade)
        create_file_row(main_frame, "Email Excel:", self.email_file_path, self.browse_email)
        create_file_row(main_frame, "Template JPG:", self.template_file_path, self.browse_template)
        create_file_row(main_frame, "Output Folder:", self.output_dir_path, self.browse_output)

        # --- Progress and Status Section ---
        self.status_label = ctk.CTkLabel(self, text="Ready to generate.", font=ctk.CTkFont(size=14))
        self.status_label.pack(pady=(20, 5))

        self.progress_bar = ctk.CTkProgressBar(self, width=500)
        self.progress_bar.pack(pady=10)
        self.progress_bar.set(0) # Start at 0%

        # Start Button
        self.start_btn = ctk.CTkButton(self, text="GENERATE PDFs", font=ctk.CTkFont(size=16, weight="bold"), height=40, command=self.start_processing)
        self.start_btn.pack(pady=20)

    # --- File Dialog Functions ---
    def browse_trade(self):
        filename = filedialog.askopenfilename(title="Select Trade Excel", filetypes=[("Excel files", "*.xlsx *.xls")])
        if filename: self.trade_file_path.set(filename)

    def browse_email(self):
        filename = filedialog.askopenfilename(title="Select Email Excel", filetypes=[("Excel files", "*.xlsx *.xls")])
        if filename: self.email_file_path.set(filename)

    def browse_template(self):
        filename = filedialog.askopenfilename(title="Select Template Image", filetypes=[("Image files", "*.jpg *.jpeg *.png")])
        if filename: self.template_file_path.set(filename)

    def browse_output(self):
        foldername = filedialog.askdirectory(title="Select Output Folder")
        if foldername: self.output_dir_path.set(foldername)

    # --- Execution Logic ---
    def start_processing(self):
        # Validation
        if not self.trade_file_path.get() or not self.template_file_path.get():
            self.status_label.configure(text="Error: Trade file and Template Image are required!", text_color="red")
            return

        # Disable button and reset progress
        self.start_btn.configure(state="disabled")
        self.progress_bar.set(0)
        self.status_label.configure(text="Initializing...", text_color="white")

        # Run the heavy work in a background thread so the GUI doesn't freeze
        threading.Thread(target=self.run_generation_task, daemon=True).start()

    def update_gui_status(self, message, progress=None, color="white"):
        # Safe way to update GUI from a background thread
        self.after(0, lambda: self.status_label.configure(text=message, text_color=color))
        if progress is not None:
            self.after(0, lambda: self.progress_bar.set(progress))

    def run_generation_task(self):
        try:
            trades_file = self.trade_file_path.get()
            emails_file = self.email_file_path.get()
            template_file = self.template_file_path.get()
            output_dir = self.output_dir_path.get()

            self.update_gui_status("Step 1: Processing Trade Data and Creating Buckets...", 0.1)
            
            # --- 1. LOAD AND PROCESS DATA ---
            df = pd.read_excel(trades_file)
            
            # Identify columns G, H, and I dynamically (Indexes 6, 7, and 8)
            col_g = df.columns[6]
            col_h = df.columns[7]
            col_i = df.columns[8]
            
            filtered_df = df[
                (df['Exchange'].isin(['NSE', 'BSE', 'NFO','BFO'])) & 
                (df['Terminal ID'].isin(['XM3004', 'XM5488']))
            ].copy()
            
            filtered_df['DateTime'] = pd.to_datetime(filtered_df['Date'].astype(str) + ' ' + filtered_df['Trade Time'].astype(str))
            
            # Normalize Transaction Type (Buy/Sell) to group correctly
            filtered_df['Txn_Type'] = filtered_df['Transaction Type'].astype(str).str.strip().str.capitalize()
            
            # --- BUCKETING LOGIC ---
            def assign_bucket_ids(group):
                group = group.sort_values('DateTime')
                bucket_ids = []
                current_bucket_start = None
                bucket_id = 1
                for time in group['DateTime']:
                    if current_bucket_start is None or pd.Timedelta(time - current_bucket_start).total_seconds() > 3600:
                        current_bucket_start = time
                        bucket_id += 1
                    bucket_ids.append(bucket_id)
                group['Bucket_ID'] = bucket_ids
                return group

            filtered_df = filtered_df.groupby('Ucc Code', group_keys=False).apply(assign_bucket_ids)

            # Added 'Txn_Type' to groupby so Buys and Sells do not net each other out
            trade_summary = filtered_df.groupby(['Ucc Code', 'Bucket_ID', 'Symbol Name', 'Exchange', col_g, col_h, col_i, 'Txn_Type'], dropna=False).agg(
                Client_Name=('Client Name', 'first'),
                Bucket_Start_Time=('DateTime', 'min'),
                Total_Quantity=('Quantity', 'sum') 
            ).reset_index()

            valid_trades = trade_summary[trade_summary['Total_Quantity'] > 0].copy()

            def compile_bucket(group):
                trades = []
                for _, row in group.iterrows():
                    action = row['Txn_Type']
                    qty = row['Total_Quantity']
                    
                    symbol = str(row['Symbol Name']).lower()
                    exchange = str(row['Exchange']).upper()
                    
                    if exchange == 'NFO' or exchange == 'BFO':
                        val_g = row[col_g]
                        val_h = row[col_h]
                        val_i = row[col_i]
                        
                        try:
                            month = pd.to_datetime(val_i).strftime('%b').lower()
                            year = pd.to_datetime(val_i).strftime('%Y')
                        except:
                            month = str(val_i).split('-')[1].lower() if pd.notna(val_i) else ""
                            year = str(val_i).split('-')[2] if pd.notna(val_i) else ""

                        if pd.notna(val_g) and str(val_g).strip() != "" and pd.notna(val_h) and str(val_h).strip() != "":
                            try:
                                strike = str(int(float(val_g)))
                            except ValueError:
                                strike = str(val_g)
                                
                            opt_type = str(val_h).lower()
                            trade_str = f"{action} {int(qty)} {symbol} {strike} {opt_type} {month} {year} at cmp"
                            
                        else:
                            trade_str = f"{action} {int(qty)} {symbol} {month} {year} at cmp"
                    else:
                        trade_str = f"{action} {int(qty)} {symbol} at cmp"

                    trades.append(trade_str)
                    
                raw_name = str(group['Client_Name'].iloc[0])
                formatted_name = " ".join(raw_name.split()).title()
                
                return pd.Series({
                    'Client_Name': formatted_name,
                    'Bucket_Start_Time': group['Bucket_Start_Time'].min(),
                    'Trades': trades
                })

            if valid_trades.empty:
                buckets_df = pd.DataFrame()
            else:
                buckets_df = valid_trades.groupby(['Ucc Code', 'Bucket_ID']).apply(compile_bucket).reset_index()

            self.update_gui_status("Step 2: Loading Emails and Template...", 0.3)
            
            # --- EMAIL LOADING & SANITIZATION ---
            try:
                emails_df = pd.read_excel(emails_file)
                # Clean up column headers in case of invisible spaces (e.g., " EMAIL ")
                emails_df.columns = emails_df.columns.str.strip().str.upper()
            except Exception:
                emails_df = pd.DataFrame(columns=['UCC', 'EMAIL'])
                
            os.makedirs(output_dir, exist_ok=True)
            
            try:
                cached_template = ImageReader(template_file)
            except Exception as e:
                self.update_gui_status(f"CRITICAL ERROR: Could not load template. {e}", color="red")
                self.after(0, lambda: self.start_btn.configure(state="normal"))
                return
            
            if buckets_df.empty:
                self.update_gui_status("No valid trades found to generate PDFs.", 1.0, color="orange")
                self.after(0, lambda: self.start_btn.configure(state="normal"))
                return

            # Clean Trade Excel UCC (remove spaces, make uppercase)
            buckets_df['Ucc Code'] = buckets_df['Ucc Code'].astype(str).str.strip().str.upper()
            
            # Clean Email Excel UCC to guarantee a match
            if 'UCC' in emails_df.columns:
                emails_df['UCC'] = emails_df['UCC'].astype(str).str.strip().str.upper()
            else:
                self.update_gui_status("Error: Could not find 'UCC' column in Email Excel.", color="red")
                self.after(0, lambda: self.start_btn.configure(state="normal"))
                return
                
            if 'EMAIL' not in emails_df.columns:
                self.update_gui_status("Error: Could not find 'EMAIL' column in Email Excel.", color="red")
                self.after(0, lambda: self.start_btn.configure(state="normal"))
                return

            # Perform the merge
            merged_df = pd.merge(buckets_df, emails_df, left_on='Ucc Code', right_on='UCC', how='left')
            
            # Identify missing emails
            is_missing_email = merged_df['EMAIL'].isna() | \
                               (merged_df['EMAIL'].astype(str).str.strip() == '') | \
                               (merged_df['EMAIL'].astype(str).str.strip().str.lower() == 'nan')
            
            ignored_clients_df = merged_df[is_missing_email][['Ucc Code', 'Client_Name']].drop_duplicates()
            if not ignored_clients_df.empty:
                ignored_file_path = os.path.join(output_dir, "Ignored_Clients.xlsx")
                ignored_clients_df.to_excel(ignored_file_path, index=False)
            
            valid_clients_df = merged_df[~is_missing_email].reset_index(drop=True)
            total_pdfs = len(valid_clients_df)
            
            if total_pdfs == 0:
                self.update_gui_status("No valid emails found for trades. All clients ignored.", 1.0, color="orange")
                self.after(0, lambda: self.start_btn.configure(state="normal"))
                return
            
            self.update_gui_status(f"Generating {total_pdfs} PDFs...", 0.4)
            
            pdf_count = 0
            last_top_left_dt = None 
            
            for index, row in valid_clients_df.iterrows():
                
                start_time = pd.to_datetime(row['Bucket_Start_Time'])
                offset_minutes = random.choice([2, 3])
                email_time = start_time - timedelta(minutes=offset_minutes)
                
                base_1530 = start_time.replace(hour=15, minute=30, second=0)
                max_1630 = start_time.replace(hour=16, minute=30, second=0)

                if last_top_left_dt is None or last_top_left_dt.date() != base_1530.date():
                    top_left_dt = base_1530 + timedelta(minutes=random.randint(0, 3))
                else:
                    min_next_time = last_top_left_dt + timedelta(minutes=2)
                    top_left_dt = min_next_time + timedelta(minutes=random.randint(0, 3))
                    
                    if top_left_dt > max_1630:
                        top_left_dt = max_1630
                
                last_top_left_dt = top_left_dt 
                
                top_left_date = f"{top_left_dt.strftime('%d/%m/%Y')},{top_left_dt.hour}:{top_left_dt.strftime('%M')}"
                email_header_date = f"{email_time.strftime('%a, %b %d, %Y at')} {email_time.strftime('%I').lstrip('0')}:{email_time.strftime('%M %p')}"

                client_email = row['EMAIL'] 
                
                filename = f"{output_dir}/{row['Client_Name']}_{row['Ucc Code']}_{top_left_dt.strftime('%H%M%S')}_{index}.pdf"
                
                self.generate_single_pdf_from_template(
                    filename=filename,
                    top_left_date=top_left_date,
                    email_header_date=email_header_date,
                    client_name=row['Client_Name'],
                    client_email=client_email,
                    trades_list=row['Trades'],
                    ucc=row['Ucc Code'],
                    template_obj=cached_template
                )
                
                pdf_count += 1
                progress = 0.4 + (0.6 * (pdf_count / total_pdfs))
                self.update_gui_status(f"Generated {pdf_count} of {total_pdfs} PDFs...", progress)

            if not ignored_clients_df.empty:
                ignored_msg = f"\n(Ignored {len(ignored_clients_df)} clients without email. See Ignored_Clients.xlsx)"
            else:
                ignored_msg = ""
                
            self.update_gui_status(f"Success! {pdf_count} PDFs saved to:\n{output_dir}{ignored_msg}", 1.0, color="#2FA572")

        except Exception as e:
            self.update_gui_status(f"Error: {str(e)}", color="red")
        
        finally:
            self.after(0, lambda: self.start_btn.configure(state="normal"))

    # --- PDF GENERATION LOGIC ---
    def generate_single_pdf_from_template(self, filename, top_left_date, email_header_date, client_name, client_email, trades_list, ucc, template_obj):
        c = canvas.Canvas(filename, pagesize=LETTER)
        width, height = LETTER
        
        c.drawImage(template_obj, 0, 0, width=width, height=height)
        
        left_margin = 0.6 * inch
        right_margin = width - 0.6 * inch
        
        c.setFillColorRGB(0, 0, 0)
        pdfmetrics.registerFont(TTFont('ArialMT', 'arial.ttf'))
        c.setFont("ArialMT", 8)

        c.drawString(left_margin - 0.3 * inch, height - 0.3 * inch, top_left_date)
        pdfmetrics.registerFont(TTFont('Arial-Bold', 'arialbd.ttf'))
        c.setFont("Arial-Bold", 9)
        y_pos_from = height - 1.68 * inch
        c.drawString(left_margin - 0.12 * inch, y_pos_from, str(client_name))
        
        name_width = c.stringWidth(str(client_name), "Arial-Bold", 9)
        pdfmetrics.registerFont(TTFont('ArialMT', 'arial.ttf'))
        
        c.setFont("ArialMT", 9)

        c.drawString(left_margin + name_width - 0.1 * inch, y_pos_from, f"<{client_email}>")
        c.setFont("ArialMT", 9)
        c.drawRightString(right_margin + 0.14 * inch, y_pos_from, email_header_date)
        
        c.setFont("ArialMT", 9)
        c.setFillColorRGB(0, 0, 0)
        
        y_pos_body_start = height - 2.5 * inch
        
        c.drawString(left_margin, y_pos_body_start, "Dear Team,")
        c.drawString(left_margin, y_pos_body_start - 25, "Please execute below order-")
        
        y_offset = 38
        
        for trade_str in trades_list:
            c.drawString(left_margin, y_pos_body_start - y_offset, trade_str)
            y_offset += 12 
        
        y_offset += 15 
        c.drawString(left_margin, y_pos_body_start - y_offset, "Regards")
        c.drawString(left_margin, y_pos_body_start - y_offset - 12, f"{client_name} ")
        c.drawString(left_margin, y_pos_body_start - y_offset - 24, str(ucc))
        
        # --- ADD RANDOMIZED FOOTER URL ---
        random_ik = "8104d236f3"
        random_msg_num = ''.join(random.choices(string.digits, k=19))
        
        footer_url = f"https://mail.google.com/mail/u/0/?ik={random_ik}&view=pt&search=all&permmsgid=msg-f:{random_msg_num}&simpl=msg-f:{random_msg_num}"
        
        c.setFont("Helvetica", 7.65)  
        c.setFillColorRGB(0, 0, 0)
        
        footer_y_pos = 0.22 * inch
        c.drawString(left_margin - 0.05 * inch, footer_y_pos, footer_url)
        
        c.save()

# ==========================================
# RUN THE APP
# ==========================================
if __name__ == "__main__":
    app = OrderGeneratorApp()
    app.mainloop()

