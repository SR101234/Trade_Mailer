import pandas as pd
from datetime import timedelta
import random
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import inch
from reportlab.lib.utils import ImageReader
import os

def process_trades_and_generate_pdfs(trades_file, emails_file, template_file, output_dir):
    print("Step 1: Processing Trade Data and Creating Buckets...")
    
    try:
        df = pd.read_excel(trades_file)
    except FileNotFoundError:
        print(f"Error: Could not find '{trades_file}'.")
        return

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

    print("\nStep 2: Generating Order PDFs from Template...")
    
    try:
        emails_df = pd.read_excel(emails_file)
    except FileNotFoundError:
        print(f"Warning: Could not find '{emails_file}'. Using example email.")
        emails_df = pd.DataFrame(columns=['UCC', 'Email ID'])
        
    os.makedirs(output_dir, exist_ok=True)
    
    # ---------------------------------------------------------
    # NEW: LOAD THE BACKGROUND TEMPLATE ONCE
    # ---------------------------------------------------------
    try:
        cached_template = ImageReader(template_file)
        print("Successfully loaded background template.")
    except Exception as e:
        print(f"CRITICAL ERROR: Could not load template '{template_file}'. Please ensure the file exists. Error: {e}")
        return
    
    merged_df = pd.merge(buckets_df, emails_df, left_on='Ucc Code', right_on='UCC', how='left')
    
    pdf_count = 0
    for index, row in merged_df.iterrows():
        if row['Net_Quantity'] == 0:
            continue
            
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
        
        generate_single_pdf_from_template(
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

    print(f"\nSuccess! Generated {pdf_count} PDF files.")

def generate_single_pdf_from_template(filename, top_left_date, email_header_date, client_name, client_email, action, quantity, stock_code, ucc, template_obj):
    c = canvas.Canvas(filename, pagesize=A4)
    width, height = A4
    
    # 1. DRAW THE BACKGROUND TEMPLATE FULL SCREEN
    # This covers the whole page with your pre-made design
    c.drawImage(template_obj, 0, 0, width=width, height=height)
    
    # ---------------------------------------------------------
    # 2. STAMP THE DYNAMIC TEXT ON TOP
    # *Note: You will likely need to adjust these Y-coordinates 
    # slightly up or down to match where the blank spaces are 
    # on your specific JPG template!
    # ---------------------------------------------------------
    left_margin = 0.6 * inch
    right_margin = width - 0.6 * inch
    
    # Top Left Date
    c.setFillColorRGB(0, 0, 0)
    c.setFont("Helvetica", 8)
    c.drawString(left_margin, height - 0.3 * inch, top_left_date)
    
    # "From" Name & Email (Approx row 4)
    c.setFont("Helvetica-Bold", 9)
    y_pos_from = height - 1.8 * inch # Adjust this number to move text up/down
    c.drawString(left_margin, y_pos_from, str(client_name))
    
    name_width = c.stringWidth(str(client_name), "Helvetica-Bold", 9)
    c.setFont("Helvetica", 9)
    c.drawString(left_margin + name_width + 4 , y_pos_from, f"<{client_email}>")
    
    # Right-aligned Date
    c.drawRightString(right_margin, y_pos_from, email_header_date)
    
    # Email Body Content
    c.setFont("Helvetica", 10)
    c.setFillColorRGB(0.35, 0.1, 0.35) # Purple text
    
    y_pos_body_start = height - 2.5 * inch # Adjust this number to move body up/down
    
    c.drawString(left_margin, y_pos_body_start, "Dear Team,")
    c.drawString(left_margin, y_pos_body_start - 15, f"{action} {int(quantity)} {str(stock_code).lower()} at cmp")
    c.drawString(left_margin, y_pos_body_start - 35, str(client_name))
    c.drawString(left_margin, y_pos_body_start - 55, str(ucc))
    
    c.save()

# ==========================================
# RUN THE SCRIPT HERE
# ==========================================
if __name__ == "__main__":
    TRADES_EXCEL_FILE = "Trade.xlsx"
    EMAILS_EXCEL_FILE = "email.xlsx" 
    TEMPLATE_IMAGE_FILE = "sample.jpg" # Your clean background image
    OUTPUT_FOLDER = "Generated_Orders"
    
    process_trades_and_generate_pdfs(TRADES_EXCEL_FILE, EMAILS_EXCEL_FILE, TEMPLATE_IMAGE_FILE, OUTPUT_FOLDER)