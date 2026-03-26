import pandas as pd

df = pd.read_excel('Trade.xlsx')
df['Terminal ID'] = df['Terminal ID'].astype(str).str.strip()
df['Exchange'] = df['Exchange'].astype(str).str.strip()
df['Exchange'] = df['Exchange'].str.upper()
df['Terminal ID'] = df['Terminal ID'].str.upper()

df = df[(df['Exchange'] == 'NSE') &
    (df['Terminal ID'].isin(['XM3004', 'XM5488']))].copy()


# Combine Date + Trade Time
df['DateTime'] = pd.to_datetime(
    df['Date'].astype(str) + ' ' + df['Trade Time'].astype(str)
)

# Sort properly
df = df.sort_values(['Client Name', 'DateTime'])

def create_buckets(group):
    bucket_id = 0
    first_time = group.iloc[0]['DateTime']
    bucket_ids = []

    for _, row in group.iterrows():
        current_time = row['DateTime']
        
        # Compare with FIRST trade of current bucket
        if (current_time - first_time).total_seconds() > 3600:
            bucket_id += 1
            first_time = current_time
        
        bucket_ids.append(bucket_id)

    group['bucket'] = bucket_ids
    return group

# Apply per client
df = df.groupby('Client Name', group_keys=False).apply(create_buckets)

# Final output → first trade time of each bucket
bucket_summary = (
    df.groupby(['Client Name', 'bucket'])
    .agg(first_trade_time=('DateTime', 'min'), total_quantity=('Quantity', 'sum'))
    .reset_index()
)

print(bucket_summary)