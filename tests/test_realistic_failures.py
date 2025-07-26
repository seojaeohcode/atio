#!/usr/bin/env python3
"""
실제 발생 가능한 실패 상황 시뮬레이션 테스트
"""

import pandas as pd
import numpy as np
import os
import time
from atomicwriter import write

def test_network_drive_slow():
    """네트워크 드라이브 느린 상황 시뮬레이션"""
    print("\n=== 네트워크 드라이브 느린 상황 테스트 ===")
    
    # 네트워크 드라이브 경로 시뮬레이션 (실제로는 존재하지 않을 수 있음)
    network_path = "/mnt/network_drive/slow_test.parquet"
    
    df = pd.DataFrame({
        'A': np.random.randn(10000),
        'B': np.random.randn(10000),
    })
    
    try:
        print(f"네트워크 경로에 저장 시도: {network_path}")
        write(df, network_path, format='parquet', debug_level=True)
        print("✅ 네트워크 드라이브 테스트 성공")
    except Exception as e:
        print(f"❌ 네트워크 드라이브 테스트 실패: {e}")
        print("  → 네트워크 드라이브 접근이 느리거나 실패한 경우")

def test_permission_denied():
    """권한 거부 상황 테스트"""
    print("\n=== 권한 거부 상황 테스트 ===")
    
    # 권한이 없는 경로 시뮬레이션
    restricted_path = "/root/restricted_test.parquet"
    
    df = pd.DataFrame({
        'A': np.random.randn(1000),
        'B': np.random.randn(1000),
    })
    
    try:
        print(f"권한 제한 경로에 저장 시도: {restricted_path}")
        write(df, restricted_path, format='parquet', debug_level=True)
        print("✅ 권한 테스트 성공")
    except Exception as e:
        print(f"❌ 권한 테스트 실패: {e}")
        print("  → 권한이 없는 경로에 접근하려고 시도한 경우")

def test_disk_full():
    """디스크 공간 부족 상황 테스트"""
    print("\n=== 디스크 공간 부족 테스트 ===")
    
    # 매우 큰 데이터로 디스크 공간 압박
    huge_df = pd.DataFrame({
        'A': np.random.randn(5000000),  # 500만 행
        'B': np.random.randn(5000000),
        'C': np.random.randn(5000000),
        'D': np.random.randn(5000000),
        'E': np.random.randn(5000000),
        'F': np.random.randn(5000000),
        'G': np.random.randn(5000000),
        'H': np.random.randn(5000000),
        'I': np.random.randn(5000000),
        'J': np.random.randn(5000000),
        'K': np.random.randn(5000000),
        'L': np.random.randn(5000000),
    })
    
    try:
        print("대용량 데이터 저장 시도 (디스크 공간 압박)...")
        write(huge_df, 'disk_full_test.parquet', format='parquet', debug_level=True)
        print("✅ 디스크 공간 테스트 성공")
    except Exception as e:
        print(f"❌ 디스크 공간 테스트 실패: {e}")
        print("  → 디스크 공간이 부족한 경우")

def test_memory_overflow():
    """메모리 오버플로우 상황 테스트"""
    print("\n=== 메모리 오버플로우 테스트 ===")
    
    # 메모리를 과도하게 사용하는 데이터 생성
    memory_hog_data = []
    
    try:
        print("메모리 과다 사용 데이터 생성 중...")
        for i in range(20):  # 메모리 압박을 위해 많은 DataFrame 생성
            df = pd.DataFrame({
                'A': np.random.randn(100000),  # 10만 행씩
                'B': np.random.randn(100000),
                'C': np.random.randn(100000),
                'D': np.random.randn(100000),
                'E': np.random.randn(100000),
            })
            memory_hog_data.append(df)
            print(f"  - {i+1}번째 대용량 DataFrame 생성 완료")
        
        print("메모리 과다 사용 데이터 저장 시도...")
        for i, df in enumerate(memory_hog_data):
            write(df, f'memory_overflow_test_{i}.parquet', format='parquet', debug_level=True)
            print(f"  - {i+1}번째 파일 저장 완료")
        
        print("✅ 메모리 오버플로우 테스트 성공")
    except Exception as e:
        print(f"❌ 메모리 오버플로우 테스트 실패: {e}")
        print("  → 메모리 부족으로 인한 실패")

def test_concurrent_access():
    """동시 접근 상황 테스트"""
    print("\n=== 동시 접근 상황 테스트 ===")
    
    import threading
    
    def write_file(file_num):
        """개별 쓰기 작업"""
        df = pd.DataFrame({
            'A': np.random.randn(5000),
            'B': np.random.randn(5000),
        })
        
        try:
            write(df, f'concurrent_test_{file_num}.parquet', format='parquet', debug_level=True)
            print(f"  - 파일 {file_num} 저장 완료")
        except Exception as e:
            print(f"  - 파일 {file_num} 저장 실패: {e}")
    
    # 여러 스레드에서 동시에 쓰기 작업 수행
    threads = []
    for i in range(5):
        thread = threading.Thread(target=write_file, args=(i,))
        threads.append(thread)
        thread.start()
    
    # 모든 스레드 완료 대기
    for thread in threads:
        thread.join()
    
    print("✅ 동시 접근 테스트 완료")

def test_corrupted_data():
    """손상된 데이터 상황 테스트"""
    print("\n=== 손상된 데이터 테스트 ===")
    
    # NaN 값이 많은 데이터 (손상된 데이터 시뮬레이션)
    corrupted_df = pd.DataFrame({
        'A': [np.nan] * 10000 + list(np.random.randn(10000)),
        'B': [np.nan] * 10000 + list(np.random.randn(10000)),
        'C': [np.nan] * 10000 + list(np.random.randn(10000)),
    })
    
    try:
        print("손상된 데이터 저장 시도...")
        write(corrupted_df, 'corrupted_test.parquet', format='parquet', debug_level=True)
        print("✅ 손상된 데이터 테스트 성공")
    except Exception as e:
        print(f"❌ 손상된 데이터 테스트 실패: {e}")
        print("  → 손상된 데이터로 인한 실패")

if __name__ == "__main__":
    print("실제 실패 상황 시뮬레이션 테스트 시작")
    print("=" * 60)
    
    # 1. 네트워크 드라이브 느린 상황
    test_network_drive_slow()
    
    # 2. 권한 거부 상황
    test_permission_denied()
    
    # 3. 디스크 공간 부족
    test_disk_full()
    
    # 4. 메모리 오버플로우
    test_memory_overflow()
    
    # 5. 동시 접근 상황
    test_concurrent_access()
    
    # 6. 손상된 데이터
    test_corrupted_data()
    
    print("\n" + "=" * 60)
    print("모든 실패 시나리오 테스트 완료!")
    print("\n💡 성능 진단 로깅의 장점:")
    print("  - 어느 단계에서 병목이 발생했는지 명확히 파악")
    print("  - AtomicWriter 자체의 오버헤드 vs 실제 쓰기 작업 시간 구분")
    print("  - I/O 문제인지 데이터 처리 문제인지 판단 가능")
    print("  - 디버깅 시간 단축 및 성능 최적화 가이드 제공") 