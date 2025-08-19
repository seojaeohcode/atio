#!/usr/bin/env python3
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
from src.atio.core import write, write_snapshot

def create_test_data(rows=100000, cols=10):
    """테스트용 데이터 생성"""
    print(f"테스트 데이터 생성 중... ({rows:,} 행 x {cols} 열)")
    
    # NumPy 배열
    np_data = np.random.randn(rows, cols)
    
    # Pandas DataFrame
    columns = [f'col_{i}' for i in range(cols)]
    pd_data = pd.DataFrame(np_data, columns=columns)
    
    # Polars DataFrame
    pl_data = pl.DataFrame(np_data, schema=columns)
    
    return np_data, pd_data, pl_data

def benchmark_numpy_write(data, format_type, temp_dir):
    """NumPy 쓰기 벤치마크"""
    if format_type == 'csv':
        start_time = time.perf_counter()
        np.savetxt(os.path.join(temp_dir, 'numpy_test.csv'), data, delimiter=',')
        end_time = time.perf_counter()
        return end_time - start_time
    elif format_type == 'parquet':
        # NumPy는 parquet을 직접 지원하지 않으므로 pandas를 통해 변환
        start_time = time.perf_counter()
        df = pd.DataFrame(data)
        df.to_parquet(os.path.join(temp_dir, 'numpy_test.parquet'))
        end_time = time.perf_counter()
        return end_time - start_time
    return None

def benchmark_pandas_write(data, format_type, temp_dir):
    """Pandas 쓰기 벤치마크"""
    start_time = time.perf_counter()
    if format_type == 'csv':
        data.to_csv(os.path.join(temp_dir, 'pandas_test.csv'), index=False)
    elif format_type == 'parquet':
        data.to_parquet(os.path.join(temp_dir, 'pandas_test.parquet'))
    end_time = time.perf_counter()
    return end_time - start_time

def benchmark_polars_write(data, format_type, temp_dir):
    """Polars 쓰기 벤치마크"""
    start_time = time.perf_counter()
    if format_type == 'csv':
        data.write_csv(os.path.join(temp_dir, 'polars_test.csv'))
    elif format_type == 'parquet':
        data.write_parquet(os.path.join(temp_dir, 'polars_test.parquet'))
    end_time = time.perf_counter()
    return end_time - start_time

def benchmark_atio_write(data, format_type, temp_dir):
    """Atio write 벤치마크"""
    start_time = time.perf_counter()
    if format_type == 'csv':
        write(data, os.path.join(temp_dir, 'atio_test.csv'), format='csv')
    elif format_type == 'parquet':
        write(data, os.path.join(temp_dir, 'atio_test.parquet'), format='parquet')
    end_time = time.perf_counter()
    return end_time - start_time

def benchmark_atio_snapshot(data, format_type, temp_dir):
    """Atio write_snapshot 벤치마크"""
    start_time = time.perf_counter()
    snapshot_dir = os.path.join(temp_dir, 'snapshot_test')
    write_snapshot(data, snapshot_dir, format=format_type)
    end_time = time.perf_counter()
    return end_time - start_time

def run_benchmark(data_size='medium'):
    """벤치마크 실행"""
    # 데이터 크기 설정
    if data_size == 'small':
        rows, cols = 10000, 10
    elif data_size == 'medium':
        rows, cols = 100000, 10
    elif data_size == 'large':
        rows, cols = 1000000, 10
    else:
        rows, cols = 100000, 10
    
    print(f"\n=== {data_size.upper()} 데이터셋 벤치마크 ({rows:,} 행 x {cols} 열) ===")
    
    # 테스트 데이터 생성
    np_data, pd_data, pl_data = create_test_data(rows, cols)
    
    # 임시 디렉토리 생성
    with tempfile.TemporaryDirectory() as temp_dir:
        results = {}
        
        # CSV 포맷 벤치마크
        print("\n--- CSV 포맷 벤치마크 ---")
        
        # NumPy CSV
        try:
            np_csv_time = benchmark_numpy_write(np_data, 'csv', temp_dir)
            results['NumPy CSV'] = np_csv_time
            print(f"NumPy CSV: {np_csv_time:.4f}s")
        except Exception as e:
            print(f"NumPy CSV 오류: {e}")
            results['NumPy CSV'] = None
        
        # Pandas CSV
        try:
            pd_csv_time = benchmark_pandas_write(pd_data, 'csv', temp_dir)
            results['Pandas CSV'] = pd_csv_time
            print(f"Pandas CSV: {pd_csv_time:.4f}s")
        except Exception as e:
            print(f"Pandas CSV 오류: {e}")
            results['Pandas CSV'] = None
        
        # Polars CSV
        try:
            pl_csv_time = benchmark_polars_write(pl_data, 'csv', temp_dir)
            results['Polars CSV'] = pl_csv_time
            print(f"Polars CSV: {pl_csv_time:.4f}s")
        except Exception as e:
            print(f"Polars CSV 오류: {e}")
            results['Polars CSV'] = None
        
        # Atio CSV
        try:
            atio_csv_time = benchmark_atio_write(pd_data, 'csv', temp_dir)
            results['Atio CSV'] = atio_csv_time
            print(f"Atio CSV: {atio_csv_time:.4f}s")
        except Exception as e:
            print(f"Atio CSV 오류: {e}")
            results['Atio CSV'] = None
        
        # Parquet 포맷 벤치마크
        print("\n--- Parquet 포맷 벤치마크 ---")
        
        # NumPy Parquet (pandas를 통해)
        try:
            np_parquet_time = benchmark_numpy_write(np_data, 'parquet', temp_dir)
            results['NumPy Parquet'] = np_parquet_time
            print(f"NumPy Parquet: {np_parquet_time:.4f}s")
        except Exception as e:
            print(f"NumPy Parquet 오류: {e}")
            results['NumPy Parquet'] = None
        
        # Pandas Parquet
        try:
            pd_parquet_time = benchmark_pandas_write(pd_data, 'parquet', temp_dir)
            results['Pandas Parquet'] = pd_parquet_time
            print(f"Pandas Parquet: {pd_parquet_time:.4f}s")
        except Exception as e:
            print(f"Pandas Parquet 오류: {e}")
            results['Pandas Parquet'] = None
        
        # Polars Parquet
        try:
            pl_parquet_time = benchmark_polars_write(pl_data, 'parquet', temp_dir)
            results['Polars Parquet'] = pl_parquet_time
            print(f"Polars Parquet: {pl_parquet_time:.4f}s")
        except Exception as e:
            print(f"Polars Parquet 오류: {e}")
            results['Polars Parquet'] = None
        
        # Atio Parquet
        try:
            atio_parquet_time = benchmark_atio_write(pd_data, 'parquet', temp_dir)
            results['Atio Parquet'] = atio_parquet_time
            print(f"Atio Parquet: {atio_parquet_time:.4f}s")
        except Exception as e:
            print(f"Atio Parquet 오류: {e}")
            results['Atio Parquet'] = None
        
        # Atio Snapshot 벤치마크
        print("\n--- Atio Snapshot 벤치마크 ---")
        
        try:
            atio_snapshot_csv_time = benchmark_atio_snapshot(pd_data, 'csv', temp_dir)
            results['Atio Snapshot CSV'] = atio_snapshot_csv_time
            print(f"Atio Snapshot CSV: {atio_snapshot_csv_time:.4f}s")
        except Exception as e:
            print(f"Atio Snapshot CSV 오류: {e}")
            results['Atio Snapshot CSV'] = None
        
        try:
            atio_snapshot_parquet_time = benchmark_atio_snapshot(pd_data, 'parquet', temp_dir)
            results['Atio Snapshot Parquet'] = atio_snapshot_parquet_time
            print(f"Atio Snapshot Parquet: {atio_snapshot_parquet_time:.4f}s")
        except Exception as e:
            print(f"Atio Snapshot Parquet 오류: {e}")
            results['Atio Snapshot Parquet'] = None
        
        return results

def print_results_table(results, data_size):
    """결과를 표 형태로 출력"""
    print(f"\n{'='*80}")
    print(f"벤치마크 결과 요약 - {data_size.upper()} 데이터셋")
    print(f"{'='*80}")
    
    # CSV 결과
    print("\n📊 CSV 포맷 성능 비교")
    print("-" * 50)
    csv_results = {k: v for k, v in results.items() if 'CSV' in k and v is not None}
    if csv_results:
        fastest_csv = min(csv_results.values())
        for method, time_taken in csv_results.items():
            speedup = fastest_csv / time_taken if time_taken > 0 else 0
            print(f"{method:<25} | {time_taken:>8.4f}s | {speedup:>6.2f}x")
    
    # Parquet 결과
    print("\n📊 Parquet 포맷 성능 비교")
    print("-" * 50)
    parquet_results = {k: v for k, v in results.items() if 'Parquet' in k and v is not None}
    if parquet_results:
        fastest_parquet = min(parquet_results.values())
        for method, time_taken in parquet_results.items():
            speedup = fastest_parquet / time_taken if time_taken > 0 else 0
            print(f"{method:<25} | {time_taken:>8.4f}s | {speedup:>6.2f}x")
    
    # Snapshot 결과
    print("\n📊 Atio Snapshot 성능 비교")
    print("-" * 50)
    snapshot_results = {k: v for k, v in results.items() if 'Snapshot' in k and v is not None}
    if snapshot_results:
        fastest_snapshot = min(snapshot_results.values())
        for method, time_taken in snapshot_results.items():
            speedup = fastest_snapshot / time_taken if time_taken > 0 else 0
            print(f"{method:<25} | {time_taken:>8.4f}s | {speedup:>6.2f}x")

def main():
    """메인 함수"""
    print("🚀 Atio 쓰기 속도 벤치마크 시작")
    print("=" * 50)
    
    # 여러 데이터 크기로 테스트
    data_sizes = ['small', 'medium', 'large']
    
    all_results = {}
    
    for size in data_sizes:
        try:
            results = run_benchmark(size)
            all_results[size] = results
            print_results_table(results, size)
        except Exception as e:
            print(f"❌ {size} 데이터셋 벤치마크 실패: {e}")
            continue
    
    print(f"\n{'='*80}")
    print("🎯 벤치마크 완료!")
    print("💡 참고사항:")
    print("   - Atio는 원자적 쓰기를 보장하므로 약간의 오버헤드가 있을 수 있습니다")
    print("   - Snapshot은 버전 관리 기능이 포함되어 있어 추가 시간이 소요됩니다")
    print("   - 실제 성능은 하드웨어, 데이터 크기, 파일 시스템에 따라 달라질 수 있습니다")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()
