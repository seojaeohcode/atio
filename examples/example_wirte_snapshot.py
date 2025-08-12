import pandas as pd
import numpy as np
from atio import read_table, write_snapshot


TABLE_DIR = "daily_sales_table"

# v1: 덮어쓰기로 첫 데이터 생성
print("v1: 첫 데이터 쓰기 (overwrite)")
df1 = pd.DataFrame({'date': ['2025-08-11'], 'sales': [100]})
write_snapshot(df1, TABLE_DIR, mode='overwrite')

# v2: 데이터 추가
print("v2: 데이터 추가하기 (append)")
df2 = pd.DataFrame({'date': ['2025-08-12'], 'sales': [120]})
write_snapshot(df2, TABLE_DIR, mode='append') # append 로직 구현 후 테스트

# 최신 데이터 읽기
print("\n[최신 데이터 읽기]")
latest_df = read_table(TABLE_DIR)
print(latest_df)

# 과거 데이터 읽기 (시간 여행)
print("\n[과거(v1) 데이터 읽기 - 시간 여행]")
v1_df = read_table(TABLE_DIR, version=1)
print(v1_df)