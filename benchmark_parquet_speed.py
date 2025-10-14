# Copyright 2025 Atio Developers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from atio import write_snapshot, read_table, write
import pandas as pd
import numpy as np
import polars as pl
import os
import tempfile
import shutil
import time
import sys
import json
import gc

def _get_dir_size_bytes(path='.'):
    """[헬퍼 함수] 디렉토리의 전체 크기를 '바이트' 단위로 재귀적으로 계산합니다."""
    total = 0
    try:
        with os.scandir(path) as it:
            for entry in it:
                if entry.is_file():
                    total += entry.stat().st_size
                elif entry.is_dir():
                    total += _get_dir_size_bytes(entry.path)
    except FileNotFoundError:
        return 0
    return total

def get_dir_size(path='.'):
    """디렉토리의 전체 크기를 MB 단위로 반환하는 메인 함수"""
    total_bytes = _get_dir_size_bytes(path)
    # 최종적으로 한 번만 MB 단위로 변환합니다.
    return total_bytes / (1024 * 1024)

def create_large_dataframe(rows, cols, lib='pandas'):
    """테스트용 대용량 데이터 객체 생성 함수"""
    print(f"\n- {rows:,}행 x {cols}열 크기의 대용량 데이터프레임 생성 중...")
    # Polars가 Pandas보다 데이터 생성 속도가 훨씬 빠릅니다.
    pl_df = pl.DataFrame({f'col_{i}': np.random.rand(rows) for i in range(cols)})
    
    if lib == 'pandas':
        df = pl_df.to_pandas()
        print(f"- Pandas DF 생성 완료 (메모리 크기: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB)")
        return df
    elif lib == 'numpy':
        arr = pl_df.to_numpy()
        print(f"- NumPy Array 생성 완료 (메모리 크기: {arr.nbytes / 1024**2:.2f} MB)")
        return arr
    else: # polars
        print(f"- Polars DF 생성 완료 (메모리 크기: {pl_df.estimated_size('mb'):.2f} MB)")
        return pl_df

def benchmark_numpy_write(data, temp_dir, format_type='parquet'):
    """NumPy 쓰기 벤치마크"""
    file_path = os.path.join(temp_dir, f'numpy_test.{format_type}')
    start_time = time.perf_counter()
    if format_type == 'csv':
        np.savetxt(file_path, data, delimiter=',')
    elif format_type == 'parquet':
        pd.DataFrame(data).to_parquet(file_path)
    return time.perf_counter() - start_time

def benchmark_pandas_write(data, temp_dir, format_type='parquet'):
    """Pandas 쓰기 벤치마크"""
    file_path = os.path.join(temp_dir, f'pandas_test.{format_type}')
    start_time = time.perf_counter()
    if format_type == 'csv':
        data.to_csv(file_path, index=False)
    elif format_type == 'parquet':
        data.to_parquet(file_path)
    return time.perf_counter() - start_time

def benchmark_polars_write(data, temp_dir, format_type='parquet'):
    """Polars 쓰기 벤치마크"""
    file_path = os.path.join(temp_dir, f'polars_test.{format_type}')
    start_time = time.perf_counter()
    if format_type == 'csv':
        data.write_csv(file_path)
    elif format_type == 'parquet':
        data.write_parquet(file_path)
    return time.perf_counter() - start_time

def benchmark_atio_write(data, temp_dir, format_type='parquet'):
    """Atio write 벤치마크"""
    file_path = os.path.join(temp_dir, f'atio_test.{format_type}')
    start_time = time.perf_counter()
    write(data, file_path, format=format_type)
    return time.perf_counter() - start_time


def run_performance_test():
    """성능 측정 메인 함수 (수정된 버전)"""
    # --- 테스트 환경 설정 ---
    NUM_ROWS = 5_400_000  # 540만 행
    NUM_COLS = 25        # 25 열
    TEST_DIR = tempfile.mkdtemp(prefix="atio_perf_test_")
    TABLE_PATH = os.path.join(TEST_DIR, "my_large_table")
    
    print("="*60)
    print(f"atio 성능 측정을 시작합니다.")
    print(f"테스트 디렉토리: {TEST_DIR}")
    print("="*60)

    try:
        # --- 1. 초기 데이터 생성 및 v1 저장 ---
        df1 = create_large_dataframe(NUM_ROWS, NUM_COLS)
        
        print("\n[V1] 첫 번째 스냅샷 저장 테스트 (overwrite mode)")
        start_time = time.perf_counter()
        write_snapshot(df1, TABLE_PATH, mode='overwrite')
        end_time = time.perf_counter()
        write_snapshot_time = end_time - start_time
        print(f"  - write_snapshot 시간: {write_snapshot_time:.4f} 초")

        start_time = time.perf_counter()
        loaded_df1 = read_table(TABLE_PATH, version=1)
        end_time = time.perf_counter()
        read_table_time = end_time - start_time
        print(f"  - read_table 시간: {read_table_time:.4f} 초")
        print(f"  - 총 스냅샷 폴더 크기: {get_dir_size(TABLE_PATH):.2f} MB")
        assert df1.equals(loaded_df1), "V1 데이터 검증 실패!"
        print("  - 데이터 무결성 검증 완료.")

        del df1, loaded_df1
        gc.collect()
        # --- 2. 두 번째 데이터 생성 및 v2 저장 (OVERWRITE) ---
        
        data2 = create_large_dataframe(NUM_ROWS, NUM_COLS)
        df2 = pd.DataFrame(data2)
        print(f"- 추가 데이터프레임 생성 완료 (메모리 크기: {df2.memory_usage(deep=True).sum() / 1024**2:.2f} MB)")
        
        print("\n[V2] 두 번째 스냅샷 저장 테스트 (append mode)")
        start_time = time.perf_counter()
        # 💡 변경점 2: 'append' 모드를 사용하고 '추가분'만 전달
        write_snapshot(df2, TABLE_PATH, mode='overwrite')  # atio의 append 모드가 overwrite로 변경됨
        end_time = time.perf_counter()
        write_snapshot_time2 = end_time - start_time
        print(f"  - write_snapshot 시간: {write_snapshot_time2:.4f} 초")


        start_time = time.perf_counter()
        loaded_df2 = read_table(TABLE_PATH, version=2)
        end_time = time.perf_counter()
        read_table_time2 = end_time - start_time
        print(f"  - read_table 시간: {read_table_time2:.4f} 초")
        print(f"  - 총 스냅샷 폴더 크기: {get_dir_size(TABLE_PATH):.2f} MB")

        del data2, df2
        gc.collect()
        # ---- numpy ----
        df1 = create_large_dataframe(NUM_ROWS, NUM_COLS, lib='numpy')  # NumPy 배열 생성
        start_time = time.perf_counter()
        benchmark_numpy_write(df1, TEST_DIR)
        end_time = time.perf_counter()
        numpy_time = end_time - start_time
        print(f"  - numpy 시간: {numpy_time:.4f} 초")

        start_time = time.perf_counter()
        numpy_array_from_pandas = pd.read_parquet(os.path.join(TEST_DIR, 'numpy_test.parquet')).to_numpy()
        end_time = time.perf_counter()
        numpy_read_time = end_time - start_time
        print(f"  - numpy 읽기 시간: {numpy_read_time:.4f} 초")

        del df1, numpy_array_from_pandas
        gc.collect()
        # ---- pandas ----
        df1 = create_large_dataframe(NUM_ROWS, NUM_COLS, lib='pandas')
        start_time = time.perf_counter()
        benchmark_pandas_write(df1, TEST_DIR)
        end_time = time.perf_counter()
        pandas_time = end_time - start_time
        print(f"  - pandas 시간: {pandas_time:.4f} 초")
        start_time = time.perf_counter()
        pandas_df = pd.read_parquet(os.path.join(TEST_DIR, 'pandas_test.parquet'))
        end_time = time.perf_counter()
        pandas_read_time = end_time - start_time
        print(f"  - pandas 읽기 시간: {pandas_read_time:.4f} 초")

        del df1, pandas_df
        gc.collect()
        # ---- polars ----
        df1 = create_large_dataframe(NUM_ROWS, NUM_COLS, lib='polars')
        start_time = time.perf_counter()
        benchmark_polars_write(df1, TEST_DIR)
        end_time = time.perf_counter()
        polars_time = end_time - start_time
        print(f"  - polars 시간: {polars_time:.4f} 초")

        start_time = time.perf_counter()
        polars_df = pl.read_parquet(os.path.join(TEST_DIR, 'polars_test.parquet'))
        end_time = time.perf_counter()
        polars_read_time = end_time - start_time
        print(f"  - polars 읽기 시간: {polars_read_time:.4f} 초")

        del df1, polars_df
        gc.collect()
        print("\n[Atio] 대용량 데이터 쓰기 벤치마크 테스트")
        print(f"- 데이터 크기: {NUM_ROWS:,}행 x {NUM_COLS}열")
        print(f"- 파일 형식: Parquet")
        print(f"- NumPy 시간: {numpy_time:.4f} 초")
        print(f"- Pandas 시간: {pandas_time:.4f} 초")
        print(f"- Polars 시간: {polars_time:.4f} 초")
        print(f"- Atio 시간: {write_snapshot_time:.4f} 초 (v1), {write_snapshot_time2:.4f} 초 (v2 append)")

        print(f"  - numpy 읽기 시간: {numpy_read_time:.4f} 초")
        print(f"  - pandas 읽기 시간: {pandas_read_time:.4f} 초")
        print(f"  - polars 읽기 시간: {polars_read_time:.4f} 초")

    except Exception as e:
        print(f"테스트 중 오류 발생: {e}")
    
    finally:
            # --- 테스트 종료 후 임시 디렉토리 삭제 ---
            print("\n- 테스트 완료, 임시 디렉토리 정리 중...")
            # 디렉토리가 존재하는지 한 번 더 확인하여 안전하게 삭제
            if os.path.exists(TEST_DIR):
                shutil.rmtree(TEST_DIR)
                print(f"- 임시 디렉토리 삭제 완료: {TEST_DIR}")
            print("="*60)

def write_snapshot_compression():
    """압축된 스냅샷 저장 테스트 함수"""
    NUM_ROWS = 5_400_000  # 540만 행
    NUM_COLS = 25        # 25 열
    TEST_DIR = tempfile.mkdtemp(prefix="atio_perf_test_")
    TABLE_PATH = os.path.join(TEST_DIR, "my_large_table")
    try:
        df1 = create_large_dataframe(NUM_ROWS, NUM_COLS)
        print(f"- 데이터프레임 생성 완료 (메모리 크기: {df1.memory_usage(deep=True).sum() / 1024**2:.2f} MB)")
        
        start_time = time.perf_counter()
        write_snapshot(df1, TABLE_PATH, mode='overwrite')
        end_time = time.perf_counter()
        print(f"  - 압축된 write_snapshot 시간: {end_time - start_time:.4f} 초")
        print(f"  - 압축된 스냅샷 폴더 크기: {get_dir_size(TABLE_PATH):.2f} MB")
    except Exception as e:
        print(f"테스트 중 오류 발생: {e}")
    
    finally:
            # --- 테스트 종료 후 임시 디렉토리 삭제 ---
            print("\n- 테스트 완료, 임시 디렉토리 정리 중...")
            # 디렉토리가 존재하는지 한 번 더 확인하여 안전하게 삭제
            if os.path.exists(TEST_DIR):
                shutil.rmtree(TEST_DIR)
                print(f"- 임시 디렉토리 삭제 완료: {TEST_DIR}")
            print("="*60)

if __name__ == '__main__':
    run_performance_test()
    write_snapshot_compression()