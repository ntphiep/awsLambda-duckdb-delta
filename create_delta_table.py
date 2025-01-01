import daft

df = daft.read_csv("2024-07-01.csv")

columns = ['date','serial_number','model','capacity_bytes','failure','datacenter','cluster_id','vault_id','pod_id','pod_slot_num']

df_cleaned = df.select(*columns)
# df_cleaned.write_deltalake("s3://hiep-delta-bk/ducklamb")

# df = daft.from_pydict({
#     "date": ['2024-12-30'], 
#     'model': ['ST4000DM000'], 
#     'failure_rate': [0]
# })

# df.write_deltalake("s3://hiep-delta-bk/ducklambcummulative", partition_cols=['date'])