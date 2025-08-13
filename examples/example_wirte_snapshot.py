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

# 과거 데이터 읽기
print("\n[과거(v1) 데이터 읽기]")
v1_df = read_table(TABLE_DIR, version=1)
print(v1_df)

import atio
import pandas as pd
import os
import time
import shutil
from datetime import datetime, timedelta
import json

def read_json(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def write_json(data: dict, path: str):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)

# --- 테스트 설정 ---
TABLE_DIR = "expiration_test_table"
KEEP_FOR_DAYS = 7

def backdate_snapshot(table_path: str, version: int, days_ago: int):
    """
    테스트를 위해 특정 버전의 스냅샷 타임스탬프를 과거로 조작하는 헬퍼 함수
    """
    print(f"{version}번 버전의 타임스탬프를 {days_ago}일 전으로 변경합니다.")
    
    # 1. 버전 메타데이터에서 스냅샷 파일명 찾기
    metadata_path = os.path.join(table_path, 'metadata', f'v{version}.metadata.json')
    metadata = read_json(metadata_path)
    snapshot_filename = metadata['snapshot_filename']
    
    # 2. 스냅샷 파일 읽기
    snapshot_path = os.path.join(table_path, snapshot_filename)
    snapshot_data = read_json(snapshot_path)
    
    # 3. 타임스탬프를 과거로 변경
    past_datetime = datetime.now() - timedelta(days=days_ago)
    snapshot_data['timestamp'] = past_datetime.timestamp()
    
    # 4. 변경된 타임스탬프로 파일 덮어쓰기
    write_json(snapshot_data, snapshot_path)

def list_files(title, table_path):
    """테이블의 현재 파일 목록을 출력하는 헬퍼 함수"""
    print(f"\n--- {title} ---")
    data_path = os.path.join(table_path, 'data')
    meta_path = os.path.join(table_path, 'metadata')
    
    print(f"  [data 폴더]")
    if os.path.exists(data_path):
        for f in sorted(os.listdir(data_path)):
            print(f"    - {f}")
            
    print(f"  [metadata 폴더]")
    if os.path.exists(meta_path):
        for f in sorted(os.listdir(meta_path)):
            print(f"    - {f}")
    print("-" * (len(title) + 8))

def main():
    # 이전 테스트가 남긴 폴더가 있다면 깨끗하게 삭제
    if os.path.exists(TABLE_DIR):
        shutil.rmtree(TABLE_DIR)

    ## 1. 시간차를 두고 버전 3개 생성
    # v1 (10일 전 데이터)
    df1 = pd.DataFrame({'value': [10]})
    atio.write_snapshot(df1, TABLE_DIR, mode='overwrite')
    backdate_snapshot(TABLE_DIR, version=1, days_ago=10)
    time.sleep(1.1) # 타임스탬프가 겹치지 않도록 잠시 대기

    # v2 (5일 전 데이터)
    df2 = pd.DataFrame({'value': [20]})
    atio.write_snapshot(df2, TABLE_DIR, mode='append')
    backdate_snapshot(TABLE_DIR, version=2, days_ago=5)
    time.sleep(1.1)

    # v3 (오늘 데이터)
    df3 = pd.DataFrame({'value': [30]})
    atio.write_snapshot(df3, TABLE_DIR, mode='append')

    ## 2. 정리 전 상태 확인
    list_files("정리 전 파일 목록", TABLE_DIR)

    ## 3. Dry Run으로 삭제 대상 미리보기
    print(f"\n2. {KEEP_FOR_DAYS}일이 지난 스냅샷 정리 (Dry Run)")
    atio.expire_snapshots(TABLE_DIR, keep_for=timedelta(days=KEEP_FOR_DAYS), dry_run=True)

    ## 4. 실제 정리 작업 실행
    print(f"\n3. 실제 정리 작업 실행 (dry_run=False)")
    atio.expire_snapshots(TABLE_DIR, keep_for=timedelta(days=KEEP_FOR_DAYS), dry_run=False)

    ## 5. 정리 후 상태 확인
    list_files("정리 후 파일 목록", TABLE_DIR)
    
    ## 6. 테스트 폴더 정리
    print(f"\n4. 테스트 완료 후 '{TABLE_DIR}' 폴더 삭제")
    shutil.rmtree(TABLE_DIR)
    print("정리 완료")

if __name__ == "__main__":
    main()