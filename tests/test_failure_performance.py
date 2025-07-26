#!/usr/bin/env python3
"""
실패 상황에서 성능 진단 로깅 테스트
"""

import pandas as pd
import numpy as np
import time
from atomicwriter import write

def create_slow_dataframe():
    """느린 처리를 시뮬레이션하는 DataFrame 생성"""
    print("=== 느린 데이터 생성 중 ===")
    
    # 매우 큰 데이터셋 생성 (메모리 압박 시뮬레이션)
    large_df = pd.DataFrame({
        'A': np.random.randn(1000000),  # 100만 행
        'B': np.random.randn(1000000),
        'C': np.random.randn(1000000),
        'D': np.random.randn(1000000),
        'E': np.random.randn(1000000),
        'F': np.random.randn(1000000),
        'G': np.random.randn(1000000),
        'H': np.random.randn(1000000),
    })
    
    print(f"생성된 데이터 크기: {large_df.shape}")
    return large_df

def test_slow_write():
    """느린 쓰기 작업 테스트"""
    print("\n=== 느린 쓰기 작업 테스트 ===")
    
    df = create_slow_dataframe()
    
    try:
        print("DEBUG 모드로 느린 쓰기 시작...")
        write(df, 'slow_output.parquet', format='parquet', debug_level=True)
        print("✅ 느린 쓰기 테스트 성공")
    except Exception as e:
        print(f"❌ 느린 쓰기 테스트 실패: {e}")

def test_io_bottleneck():
    """I/O 병목 상황 시뮬레이션"""
    print("\n=== I/O 병목 테스트 ===")
    
    # 작은 데이터로 I/O 병목 시뮬레이션
    df = pd.DataFrame({
        'A': np.random.randn(1000),
        'B': np.random.randn(1000),
    })
    
    try:
        print("DEBUG 모드로 I/O 병목 테스트 시작...")
        # 여러 번 연속으로 쓰기 (I/O 경합 시뮬레이션)
        for i in range(5):
            write(df, f'io_test_{i}.parquet', format='parquet', debug_level=True)
            print(f"  - {i+1}번째 파일 완료")
        
        print("✅ I/O 병목 테스트 성공")
    except Exception as e:
        print(f"❌ I/O 병목 테스트 실패: {e}")

def test_memory_pressure():
    """메모리 압박 상황 테스트"""
    print("\n=== 메모리 압박 테스트 ===")
    
    # 메모리 사용량이 많은 데이터 생성
    large_data = []
    for i in range(10):
        df = pd.DataFrame({
            'A': np.random.randn(50000),
            'B': np.random.randn(50000),
            'C': np.random.randn(50000),
        })
        large_data.append(df)
        print(f"  - {i+1}번째 대용량 DataFrame 생성 완료")
    
    try:
        print("DEBUG 모드로 메모리 압박 테스트 시작...")
        for i, df in enumerate(large_data):
            write(df, f'memory_test_{i}.parquet', format='parquet', debug_level=True)
            print(f"  - {i+1}번째 파일 저장 완료")
        
        print("✅ 메모리 압박 테스트 성공")
    except Exception as e:
        print(f"❌ 메모리 압박 테스트 실패: {e}")

def test_disk_space_issue():
    """디스크 공간 부족 상황 시뮬레이션"""
    print("\n=== 디스크 공간 테스트 ===")
    
    # 매우 큰 데이터로 디스크 공간 압박 시뮬레이션
    huge_df = pd.DataFrame({
        'A': np.random.randn(2000000),  # 200만 행
        'B': np.random.randn(2000000),
        'C': np.random.randn(2000000),
        'D': np.random.randn(2000000),
        'E': np.random.randn(2000000),
        'F': np.random.randn(2000000),
        'G': np.random.randn(2000000),
        'H': np.random.randn(2000000),
        'I': np.random.randn(2000000),
        'J': np.random.randn(2000000),
    })
    
    try:
        print("DEBUG 모드로 디스크 공간 테스트 시작...")
        write(huge_df, 'huge_output.parquet', format='parquet', debug_level=True)
        print("✅ 디스크 공간 테스트 성공")
    except Exception as e:
        print(f"❌ 디스크 공간 테스트 실패: {e}")

if __name__ == "__main__":
    print("실패 상황에서 성능 진단 로깅 테스트 시작")
    print("=" * 50)
    
    # 1. 느린 쓰기 테스트
    test_slow_write()
    
    # 2. I/O 병목 테스트
    test_io_bottleneck()
    
    # 3. 메모리 압박 테스트
    test_memory_pressure()
    
    # 4. 디스크 공간 테스트
    test_disk_space_issue()
    
    print("\n" + "=" * 50)
    print("모든 테스트 완료!") 