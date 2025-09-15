import pyarrow.parquet as pq

table = pq.read_table(r"D:\Projects\Python\etl-currency\data\processed\2025-09-14\data.parquet")
df = table.to_pandas()
print(df.head())
