import pandas as pd
from collections import defaultdict
from datetime import datetime

file_path = "utxodump.csv"
chunksize = 100_000
min_height = 716600
progress_interval = 5

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
output_file = f"utxo_summary_{timestamp}.csv"

type_counts = defaultdict(int)
type_sums = defaultdict(float)
rows_processed = 0
chunk_index = 0

try:
    for chunk in pd.read_csv(file_path, usecols=["type", "amount", "height"], chunksize=chunksize):
        chunk = chunk[chunk["height"] >= min_height]

        counts = chunk["type"].value_counts()
        sums = chunk.groupby("type")["amount"].sum()

        for t, count in counts.items():
            type_counts[t] += count
        for t, amt in sums.items():
            type_sums[t] += amt

        rows_processed += len(chunk)
        chunk_index += 1

        if chunk_index % progress_interval == 0:
            print(f"Processed {rows_processed:,} rows so far...")

    summary_df = pd.DataFrame({
        "type": list(type_counts.keys()),
        "count": [type_counts[t] for t in type_counts],
        "total_amount": [type_sums[t] for t in type_counts]
    }).sort_values(by="total_amount", ascending=False)

    print("\nFinal summary:\n")
    print(summary_df.to_string(index=False))

    with open(output_file, "w") as f:
        f.write(f"# Minimum height filter: {min_height}\n")
        summary_df.to_csv(f, index=False)

    print(f"\nSummary saved to: {output_file}")

except FileNotFoundError:
    print(f"File not found: {file_path}")
except Exception as e:
    print(f"Error: {e}")
