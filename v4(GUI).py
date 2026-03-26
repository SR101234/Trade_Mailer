import pandas as pd
from datetime import timedelta
import random
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import inch
from reportlab.lib.utils import ImageReader
import os
import threading

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
            
            filtered_df = df[
                (df['Exchange'].isin(['NSE', 'BSE'])) & 
                (df['Terminal ID'].isin(['XM3004', 'XM5488']))
            ].copy()
            
            filtered_df['DateTime'] = pd.to_datetime(filtered_df['Date'].astype(str) + ' ' + filtered_df['Trade Time'].astype(str))
            filtered_df['Signed_Quantity'] = filtered_df.apply(
                lambda row: row['Quantity'] if str(row['Transaction Type']).strip().upper() == 'BUY' else -row['Quantity'], 
                axis=1
            )
            filtered_df = filtered_df.sort_values(by=['Ucc Code', 'Symbol Name', 'DateTime'])
            
            def bucket_and_aggregate(group):
                bucket_ids = []
                current_bucket_start = None
                bucket_id = 1
                for time in group['DateTime']:
                    if current_bucket_start is None or pd.Timedelta(time - current_bucket_start).total_seconds() > 3600:
                        current_bucket_start = time
                        bucket_id += 1
                    bucket_ids.append(bucket_id)
                group['Bucket_ID'] = bucket_ids
                aggregated = group.groupby('Bucket_ID').agg(
                    Client_Name=('Client Name', 'first'),
                    Bucket_Start_Time=('DateTime', 'min'),
                    Net_Quantity=('Signed_Quantity', 'sum')
                )
                return aggregated

            buckets_df = filtered_df.groupby(['Ucc Code', 'Symbol Name']).apply(bucket_and_aggregate).reset_index()
            buckets_df.to_excel('processed_hourly_buckets.xlsx', index=False)

            self.update_gui_status("Step 2: Loading Emails and Template...", 0.3)
            
            # Load Emails
            try:
                emails_df = pd.read_excel(emails_file)
            except Exception:
                emails_df = pd.DataFrame(columns=['UCC', 'EMAIL'])
                
            os.makedirs(output_dir, exist_ok=True)
            
            # Load Template
            try:
                cached_template = ImageReader(template_file)
            except Exception as e:
                self.update_gui_status(f"CRITICAL ERROR: Could not load template. {e}", color="red")
                self.after(0, lambda: self.start_btn.configure(state="normal"))
                return
            
            merged_df = pd.merge(buckets_df, emails_df, left_on='Ucc Code', right_on='UCC', how='left')
            
            # Count valid PDFs for progress bar calculation
            valid_rows = merged_df[merged_df['Net_Quantity'] != 0]
            total_pdfs = len(valid_rows)
            
            if total_pdfs == 0:
                self.update_gui_status("No valid trades found to generate PDFs.", 1.0, color="orange")
                self.after(0, lambda: self.start_btn.configure(state="normal"))
                return

            self.update_gui_status(f"Generating {total_pdfs} PDFs...", 0.4)
            
            pdf_count = 0
            for index, row in valid_rows.iterrows():
                action = "Buy" if row['Net_Quantity'] > 0 else "Sell"
                quantity = abs(row['Net_Quantity'])
                
                start_time = pd.to_datetime(row['Bucket_Start_Time'])
                offset_minutes = random.choice([2, 3])
                email_time = start_time - timedelta(minutes=offset_minutes)
                
                top_left_date = email_time.strftime("%d/%m/%Y,%H:%M")
                email_header_date = email_time.strftime("%a, %b %d, %Y at %I:%M %p")
                
                client_email = row.get('EMAIL')
                if pd.isna(client_email):
                    client_email = "client@example.com"
                
                filename = f"{output_dir}/{row['Ucc Code']}_{row['Symbol Name']}_{email_time.strftime('%H%M%S')}.pdf"
                
                self.generate_single_pdf_from_template(
                    filename=filename,
                    top_left_date=top_left_date,
                    email_header_date=email_header_date,
                    client_name=row['Client_Name'],
                    client_email=client_email,
                    action=action,
                    quantity=quantity,
                    stock_code=row['Symbol Name'],
                    ucc=row['Ucc Code'],
                    template_obj=cached_template
                )
                
                pdf_count += 1
                
                # Update progress bar (mapping 0.4 to 1.0)
                progress = 0.4 + (0.6 * (pdf_count / total_pdfs))
                self.update_gui_status(f"Generated {pdf_count} of {total_pdfs} PDFs...", progress)

            self.update_gui_status(f"Success! {pdf_count} PDFs saved to {output_dir}", 1.0, color="#2FA572") # Green

        except Exception as e:
            self.update_gui_status(f"Error: {str(e)}", color="red")
        
        finally:
            # Re-enable the button when done or if it crashed
            self.after(0, lambda: self.start_btn.configure(state="normal"))

    # --- Your fine-tuned PDF generation logic ---
    def generate_single_pdf_from_template(self, filename, top_left_date, email_header_date, client_name, client_email, action, quantity, stock_code, ucc, template_obj):
        c = canvas.Canvas(filename, pagesize=A4)
        width, height = A4
        
        c.drawImage(template_obj, 0, 0, width=width, height=height)
        
        left_margin = 0.6 * inch
        right_margin = width - 0.6 * inch
        
        c.setFillColorRGB(0, 0, 0)
        c.setFont("Helvetica", 8)
        c.drawString(left_margin, height - 0.3 * inch, top_left_date)
        
        c.setFont("Helvetica-Bold", 9)
        y_pos_from = height - 1.8 * inch
        c.drawString(left_margin, y_pos_from, str(client_name))
        
        name_width = c.stringWidth(str(client_name), "Helvetica-Bold", 9)
        c.setFont("Helvetica", 9)
        c.drawString(left_margin + name_width + 4 , y_pos_from, f"<{client_email}>")
        
        c.drawRightString(right_margin, y_pos_from, email_header_date)
        
        c.setFont("Helvetica", 10)
        c.setFillColorRGB(0.35, 0.1, 0.35)
        
        y_pos_body_start = height - 2.5 * inch
        
        c.drawString(left_margin, y_pos_body_start, "Dear Team,")
        c.drawString(left_margin, y_pos_body_start - 15, f"{action} {int(quantity)} {str(stock_code).lower()} at cmp")
        c.drawString(left_margin, y_pos_body_start - 35, str(client_name))
        c.drawString(left_margin, y_pos_body_start - 55, str(ucc))
        
        c.save()

# ==========================================
# RUN THE APP
# ==========================================
if __name__ == "__main__":
    app = OrderGeneratorApp()
    app.mainloop()