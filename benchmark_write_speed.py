#!/usr/bin/env python3

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

"""
쓰기 속도 벤치마크: NumPy, Pandas, Polars vs Atio
CSV와 Parquet 포맷에 대한 성능 비교
"""

import time
import tempfile
import os
import shutil
from pathlib import Path
import numpy as np
import pandas as pd
import polars as pl

# src 폴더가 현재 위치의 상위 폴더에 있다고 가정하고 경로 추가
# 이 스크립트가 프로젝트 루트에 있다면 아래 줄은 필요 없을 수 있습니다.
try:
    from src.atio.core import write, write_snapshot
except ImportError:
    print("atio 라이브러리를 찾을 수 없습니다. 경로를 확인해주세요.")
    # 개발 환경을 위해 경로를 동적으로 추가
    import sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from src.atio.core import write, write_snapshot


def create_test_data(rows=10, cols=100000):
    """테스트용 데이터 생성"""
    print(f"테스트 데이터 생성 중... ({rows:,} 행 x {cols} 열)")
    np_data = np.random.randn(rows, cols)
    columns = [f'col_{i}' for i in range(cols)]
    pd_data = pd.DataFrame(np_data, columns=columns)
    pl_data = pl.DataFrame(np_data, schema=columns)
    return np_data, pd_data, pl_data

def benchmark_numpy_write(data, format_type, temp_dir):
    """NumPy 쓰기 벤치마크"""
    file_path = os.path.join(temp_dir, f'numpy_test.{format_type}')
    start_time = time.perf_counter()
    if format_type == 'csv':
        np.savetxt(file_path, data, delimiter=',')
    elif format_type == 'parquet':
        pd.DataFrame(data).to_parquet(file_path)
    return time.perf_counter() - start_time

def benchmark_pandas_write(data, format_type, temp_dir):
    """Pandas 쓰기 벤치마크"""
    file_path = os.path.join(temp_dir, f'pandas_test.{format_type}')
    start_time = time.perf_counter()
    if format_type == 'csv':
        data.to_csv(file_path, index=False)
    elif format_type == 'parquet':
        data.to_parquet(file_path)
    return time.perf_counter() - start_time

def benchmark_polars_write(data, format_type, temp_dir):
    """Polars 쓰기 벤치마크"""
    file_path = os.path.join(temp_dir, f'polars_test.{format_type}')
    start_time = time.perf_counter()
    if format_type == 'csv':
        data.write_csv(file_path)
    elif format_type == 'parquet':
        data.write_parquet(file_path)
    return time.perf_counter() - start_time

def benchmark_atio_write(data, format_type, temp_dir):
    """Atio write 벤치마크"""
    file_path = os.path.join(temp_dir, f'atio_test.{format_type}')
    start_time = time.perf_counter()
    write(data, file_path, format=format_type)
    return time.perf_counter() - start_time

def benchmark_atio_snapshot(data, temp_dir):
    """Atio write_snapshot 벤치마크"""
    snapshot_dir = os.path.join(temp_dir, 'snapshot_test_table')
    start_time = time.perf_counter()
    write_snapshot(data, snapshot_dir)
    return time.perf_counter() - start_time

def _run_single_benchmark(results, name, func, *args):
    """단일 벤치마크를 실행하고 결과를 저장하는 헬퍼 함수"""
    try:
        duration = func(*args)
        results[name] = duration
        print(f"{name:<20}: {duration:.4f}s")
    except Exception as e:
        results[name] = None
        print(f"{name:<20}: FAILED ({e})")

def run_benchmark(data_size='medium'):
    """벤치마크 실행 (리팩토링됨)"""
    rows_cols_map = {
        'small': (10_000, 10),
        'medium': (100_000, 10),
        'large': (1_000_000, 10)
    }
    rows, cols = rows_cols_map.get(data_size, (100_000, 10))
    
    print(f"\n=== {data_size.upper()} 데이터셋 벤치마크 ({rows:,} 행 x {cols} 열) ===")
    
    np_data, pd_data, pl_data = create_test_data(rows, cols)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        results = {}
        
        print("\n--- CSV 포맷 벤치마크 ---")
        _run_single_benchmark(results, 'NumPy CSV', benchmark_numpy_write, np_data, 'csv', temp_dir)
        _run_single_benchmark(results, 'Pandas CSV', benchmark_pandas_write, pd_data, 'csv', temp_dir)
        _run_single_benchmark(results, 'Polars CSV', benchmark_polars_write, pl_data, 'csv', temp_dir)
        _run_single_benchmark(results, 'Atio CSV', benchmark_atio_write, pd_data, 'csv', temp_dir)

        print("\n--- Parquet 포맷 벤치마크 ---")
        _run_single_benchmark(results, 'NumPy Parquet', benchmark_numpy_write, np_data, 'parquet', temp_dir)
        _run_single_benchmark(results, 'Pandas Parquet', benchmark_pandas_write, pd_data, 'parquet', temp_dir)
        _run_single_benchmark(results, 'Polars Parquet', benchmark_polars_write, pl_data, 'parquet', temp_dir)
        _run_single_benchmark(results, 'Atio Parquet', benchmark_atio_write, pd_data, 'parquet', temp_dir)

        print("\n--- Atio Snapshot 벤치마크 ---")
        _run_single_benchmark(results, 'Atio Snapshot', benchmark_atio_snapshot, pd_data, temp_dir)

        return results

def print_results_table(results, data_size):
    """결과를 표 형태로 출력 (수정됨)"""
    print(f"\n{'='*80}")
    print(f"벤치마크 결과 요약 - {data_size.upper()} 데이터셋")
    print(f"{'='*80}")
    
    def print_sorted_results(title, filtered_results):
        print(f"\n📊 {title}")
        print("-" * 65)
        if not filtered_results:
            print("결과 없음")
            return
            
        fastest_time = min(filtered_results.values())
        for method, time_taken in sorted(filtered_results.items(), key=lambda item: item[1]):
            speedup = time_taken / fastest_time if fastest_time > 0 else 0
            print(f"{method:<25} | {time_taken:>8.4f}s | (Fastest 대비 {speedup:.2f}x 느림)")

    csv_results = {k: v for k, v in results.items() if 'CSV' in k and v is not None}
    print_sorted_results("CSV 포맷 성능 비교", csv_results)

    parquet_results = {k: v for k, v in results.items() if 'Parquet' in k and v is not None}
    print_sorted_results("Parquet 포맷 성능 비교", parquet_results)
    
    snapshot_time = results.get('Atio Snapshot')
    if snapshot_time is not None:
        print("\n📊 Atio Snapshot 성능")
        print("-" * 65)
        print(f"{'Atio Snapshot':<25} | {snapshot_time:>8.4f}s | (버전 관리 기능 포함)")

def main():
    """메인 함수"""
    print("🚀 Atio 쓰기 속도 벤치마크 시작")
    print("=" * 50)
    
    data_sizes = ['small', 'medium', 'large']
    
    for size in data_sizes:
        try:
            results = run_benchmark(size)
            print_results_table(results, size)
        except Exception as e:
            print(f"❌ {size} 데이터셋 벤치마크 실패: {e}")
            continue
    
    print(f"\n{'='*80}")
    print("🎯 벤치마크 완료!")
    print("💡 참고사항:")
    print("  - Atio의 `write` 함수는 원자적 쓰기를 보장하므로 약간의 오버헤드가 있을 수 있습니다.")
    print("  - Atio의 `write_snapshot` 함수는 열 단위 중복 제거 및 버전 관리 기능이 포함되어 있어 추가 시간이 소요됩니다.")
    print("  - 실제 성능은 하드웨어, 데이터 크기, 파일 시스템에 따라 달라질 수 있습니다.")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()
