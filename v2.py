import pandas as pd

file_path = r"D:/C_lang/Code/Python/Trade_Mailer/Trade.xlsx"  # Replace with your actual file path

def create_hourly_trade_buckets(file_path):
    # 1. Load the data (replace with your actual file path)
    df = pd.read_excel(file_path) # or pd.read_csv if it's a CSV
    
    # 2. Apply Filters: Exchange (NSE/BSE) and Terminal ID (XM3004/XM5488)
    filtered_df = df[
        (df['Exchange'].isin(['NSE', 'BSE'])) & 
        (df['Terminal ID'].isin(['XM3004', 'XM5488']))
    ].copy()
    
    # 3. Pre-process Time and Quantity
    # Combine Date and Trade Time into a single DateTime object for accurate time math
    filtered_df['DateTime'] = pd.to_datetime(filtered_df['Date'].astype(str) + ' ' + filtered_df['Trade Time'].astype(str))
    
    # Create a signed quantity (Buy is positive, Sell is negative) for net calculation
    filtered_df['Signed_Quantity'] = filtered_df.apply(
        lambda row: row['Quantity'] if str(row['Transaction Type']).strip().upper() == 'BUY' else -row['Quantity'], 
        axis=1
    )
    
    # 4. Sort the data sequentially to prepare for grouping
    filtered_df = filtered_df.sort_values(by=['Ucc Code', 'Symbol Name', 'DateTime'])
    
    # 5. Define the dynamic bucketing logic
    def bucket_and_aggregate(group):
        bucket_ids = []
        current_bucket_start = None
        bucket_id = 1
        
        # Iterate through times to create dynamic 1-hour windows
        for time in group['DateTime']:
            if current_bucket_start is None or pd.Timedelta(time - current_bucket_start).total_seconds() > 3600:
                # Start a new bucket if it's the first trade or > 1 hour from the current bucket's start
                current_bucket_start = time
                bucket_id += 1
            bucket_ids.append(bucket_id)
            
        group['Bucket_ID'] = bucket_ids
        
        # Aggregate the data within these newly defined buckets
        aggregated = group.groupby('Bucket_ID').agg(
            Client_Name=('Client Name', 'first'),
            Bucket_Start_Time=('DateTime', 'min'),
            Bucket_End_Time=('DateTime', 'max'),
            Net_Quantity=('Signed_Quantity', 'sum'),
            Total_Trades_In_Bucket=('Trade ID', 'count')
        )
        return aggregated

    # 6. Apply the logic grouped by Client (UCC Code) and Stock (Symbol Name)
    final_df = filtered_df.groupby(['Ucc Code', 'Symbol Name']).apply(bucket_and_aggregate).reset_index()
    
    # Clean up the output dataframe
    final_df.drop(columns=['Bucket_ID'], inplace=True, errors='ignore')
    
    return final_df

# --- Execution Example ---
result = create_hourly_trade_buckets(file_path)
print(result)
result.to_excel('bucketed_trades.xlsx', index=False)