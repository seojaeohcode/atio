#!/usr/bin/env python3
"""
다양한 오류 상황에서 성능 진단 로깅 테스트
"""

import pandas as pd
import numpy as np
import os
import time
from atio import write

def test_unsupported_format(tmp_path):
    """지원하지 않는 형식 오류 테스트"""
    print("\n=== 지원하지 않는 형식 오류 테스트 ===")
    
    df = pd.DataFrame({
        'A': np.random.randn(1000),
        'B': np.random.randn(1000),
    })
    
    try:
        print("지원하지 않는 형식으로 저장 시도...")
        write(df, tmp_path/'test_unsupported.xyz', format='xyz', verbose=True)
        print("✅ 지원하지 않는 형식 테스트 성공")
    except Exception as e:
        print(f"❌ 지원하지 않는 형식 테스트 실패: {e}")
        print("  → DEBUG 로그에서 setup 단계에서 실패했는지 확인")

def test_permission_error(tmp_path):
    """권한 오류 테스트"""
    print("\n=== 권한 오류 테스트 ===")
    
    df = pd.DataFrame({
        'A': np.random.randn(1000),
        'B': np.random.randn(1000),
    })
    
    try:
        print("권한이 없는 경로에 저장 시도...")
        write(df, tmp_path/'root/permission_test.parquet', format='parquet', verbose=True)
        print("✅ 권한 오류 테스트 성공")
    except Exception as e:
        print(f"❌ 권한 오류 테스트 실패: {e}")
        print("  → DEBUG 로그에서 어느 단계에서 실패했는지 확인")

def test_disk_full_error(tmp_path):
    """디스크 공간 부족 오류 테스트"""
    print("\n=== 디스크 공간 부족 오류 테스트 ===")
    
    # 매우 큰 데이터로 디스크 공간 압박
    huge_df = pd.DataFrame({
        'A': np.random.randn(1000000),  # 100만 행
        'B': np.random.randn(1000000),
        'C': np.random.randn(1000000),
        'D': np.random.randn(1000000),
        'E': np.random.randn(1000000),
        'F': np.random.randn(1000000),
        'G': np.random.randn(1000000),
        'H': np.random.randn(1000000),
        'I': np.random.randn(1000000),
        'J': np.random.randn(1000000),
    })
    
    try:
        print("대용량 데이터 저장 시도 (디스크 공간 압박)...")
        write(huge_df, tmp_path/'disk_full_test.parquet', format='parquet', verbose=True)
        print("✅ 디스크 공간 부족 테스트 성공")
    except Exception as e:
        print(f"❌ 디스크 공간 부족 테스트 실패: {e}")
        print("  → DEBUG 로그에서 어느 단계에서 실패했는지 확인")

def test_memory_error(tmp_path):
    """메모리 부족 오류 테스트"""
    print("\n=== 메모리 부족 오류 테스트 ===")
    
    # 메모리를 과도하게 사용하는 데이터 생성
    memory_hog_data = []
    
    try:
        print("메모리 과다 사용 데이터 생성 중...")
        for i in range(50):  # 메모리 압박을 위해 많은 DataFrame 생성
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
            write(df, tmp_path/f'memory_error_test_{i}.parquet', format='parquet', verbose=True)
            print(f"  - {i+1}번째 파일 저장 완료")
        
        print("✅ 메모리 부족 테스트 성공")
    except Exception as e:
        print(f"❌ 메모리 부족 테스트 실패: {e}")
        print("  → DEBUG 로그에서 어느 단계에서 실패했는지 확인")

def test_network_error(tmp_path):
    """네트워크 오류 테스트"""
    print("\n=== 네트워크 오류 테스트 ===")
    
    df = pd.DataFrame({
        'A': np.random.randn(1000),
        'B': np.random.randn(1000),
    })
    
    try:
        print("네트워크 경로에 저장 시도...")
        write(df, tmp_path/'mnt/network_drive/network_test.parquet', format='parquet', verbose=True)
        print("✅ 네트워크 오류 테스트 성공")
    except Exception as e:
        print(f"❌ 네트워크 오류 테스트 실패: {e}")
        print("  → DEBUG 로그에서 어느 단계에서 실패했는지 확인")

def test_corrupted_data_error(tmp_path):
    """손상된 데이터 오류 테스트"""
    print("\n=== 손상된 데이터 오류 테스트 ===")
    
    # NaN 값이 많은 데이터 (손상된 데이터 시뮬레이션)
    corrupted_df = pd.DataFrame({
        'A': [np.nan] * 10000 + list(np.random.randn(10000)),
        'B': [np.nan] * 10000 + list(np.random.randn(10000)),
        'C': [np.nan] * 10000 + list(np.random.randn(10000)),
    })
    
    try:
        print("손상된 데이터 저장 시도...")
        write(corrupted_df, tmp_path/'corrupted_data_test.parquet', format='parquet', verbose=True)
        print("✅ 손상된 데이터 테스트 성공")
    except Exception as e:
        print(f"❌ 손상된 데이터 테스트 실패: {e}")
        print("  → DEBUG 로그에서 어느 단계에서 실패했는지 확인")

def test_concurrent_access_error():
    """동시 접근 오류 테스트"""
    print("\n=== 동시 접근 오류 테스트 ===")
    
    import threading
    
    def write_file(tmp_path, file_num):
        """개별 쓰기 작업"""
        df = pd.DataFrame({
            'A': np.random.randn(5000),
            'B': np.random.randn(5000),
        })
        
        try:
            write(df, tmp_path/f'concurrent_error_test_{file_num}.parquet', format='parquet', verbose=True)
            print(f"  - 파일 {file_num} 저장 완료")
        except Exception as e:
            print(f"  - 파일 {file_num} 저장 실패: {e}")
    
    # 여러 스레드에서 동시에 같은 파일에 쓰기 (충돌 시뮬레이션)
    threads = []
    for i in range(10):
        thread = threading.Thread(target=write_file, args=(i,))
        threads.append(thread)
        thread.start()
    
    # 모든 스레드 완료 대기
    for thread in threads:
        thread.join()
    
    print("✅ 동시 접근 오류 테스트 완료")

def test_keyboard_interrupt_error(tmp_path):
    """키보드 인터럽트 오류 테스트"""
    print("\n=== 키보드 인터럽트 오류 테스트 ===")
    print("이 테스트는 5초 후 자동으로 Ctrl+C를 시뮬레이션합니다.")
    
    import threading
    import signal
    
    df = pd.DataFrame({
        'A': np.random.randn(100000),  # 10만 행
        'B': np.random.randn(100000),
        'C': np.random.randn(100000),
        'D': np.random.randn(100000),
    })
    
    # 5초 후 인터럽트 시뮬레이션
    def simulate_interrupt():
        time.sleep(5)
        print("\n🔄 키보드 인터럽트 시뮬레이션...")
        import os
        os.kill(os.getpid(), signal.SIGINT)
    
    interrupt_thread = threading.Thread(target=simulate_interrupt)
    interrupt_thread.daemon = True
    interrupt_thread.start()
    
    try:
        print("DEBUG 모드로 대용량 데이터 쓰기 시작...")
        print("(5초 후 자동으로 인터럽트가 발생합니다)")
        write(df, tmp_path/'keyboard_interrupt_test.parquet', format='parquet', verbose=True)
        print("✅ 키보드 인터럽트 테스트 성공")
    except KeyboardInterrupt:
        print("\n❌ KeyboardInterrupt 발생!")
        print("  → DEBUG 로그에서 어느 단계에서 중단되었는지 확인")
    except Exception as e:
        print(f"❌ 다른 예외 발생: {e}")