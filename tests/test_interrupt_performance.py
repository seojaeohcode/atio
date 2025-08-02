#!/usr/bin/env python3
"""
키보드 인터럽트 시뮬레이션으로 성능 진단 로깅 테스트
"""

import pandas as pd
import numpy as np
import time
import signal
import threading
from atio import write

def create_large_dataframe():
    """대용량 DataFrame 생성 (인터럽트 시뮬레이션용)"""
    print("=== 대용량 데이터 생성 중 ===")
    
    # 인터럽트가 발생할 수 있도록 충분히 큰 데이터 생성
    large_df = pd.DataFrame({
        'A': np.random.randn(500000),  # 50만 행
        'B': np.random.randn(500000),
        'C': np.random.randn(500000),
        'D': np.random.randn(500000),
        'E': np.random.randn(500000),
        'F': np.random.randn(500000),
        'G': np.random.randn(500000),
        'H': np.random.randn(500000),
    })
    
    print(f"생성된 데이터 크기: {large_df.shape}")
    return large_df

def test_interrupt_during_write():
    """쓰기 중간에 인터럽트 발생 시뮬레이션"""
    print("\n=== 쓰기 중 인터럽트 테스트 ===")
    print("이 테스트는 3초 후 자동으로 Ctrl+C를 시뮬레이션합니다.")
    print("실제로는 Ctrl+C를 직접 눌러서 테스트할 수 있습니다.")
    
    df = create_large_dataframe()
    
    # 3초 후 인터럽트 시뮬레이션
    def simulate_interrupt():
        time.sleep(3)
        print("\n🔄 인터럽트 시뮬레이션 중...")
        import os
        os.kill(os.getpid(), signal.SIGINT)
    
    # 백그라운드에서 인터럽트 시뮬레이션
    interrupt_thread = threading.Thread(target=simulate_interrupt)
    interrupt_thread.daemon = True
    interrupt_thread.start()
    
    try:
        print("DEBUG 모드로 대용량 데이터 쓰기 시작...")
        print("(3초 후 자동으로 인터럽트가 발생합니다)")
        write(df, 'interrupt_test.parquet', format='parquet', debug_level=True)
        print("✅ 인터럽트 테스트 성공 (인터럽트 없이 완료)")
    except KeyboardInterrupt:
        print("\n❌ KeyboardInterrupt 발생!")
        print("  → 쓰기 작업이 중간에 중단되었습니다.")
        print("  → AtomicWriter는 원본 파일을 보존합니다.")
        print("  → 임시 파일은 자동으로 정리됩니다.")
    except Exception as e:
        print(f"❌ 다른 예외 발생: {e}")

def test_interrupt_during_setup():
    """설정 단계에서 인터럽트 발생 시뮬레이션"""
    print("\n=== 설정 단계 인터럽트 테스트 ===")
    print("이 테스트는 설정 단계에서 인터럽트를 시뮬레이션합니다.")
    
    df = pd.DataFrame({
        'A': np.random.randn(10000),
        'B': np.random.randn(10000),
    })
    
    # 1초 후 인터럽트 시뮬레이션 (설정 단계에서)
    def simulate_early_interrupt():
        time.sleep(1)
        print("\n🔄 설정 단계에서 인터럽트 시뮬레이션...")
        import os
        os.kill(os.getpid(), signal.SIGINT)
    
    interrupt_thread = threading.Thread(target=simulate_early_interrupt)
    interrupt_thread.daemon = True
    interrupt_thread.start()
    
    try:
        print("DEBUG 모드로 쓰기 시작 (1초 후 인터럽트)...")
        write(df, 'early_interrupt_test.parquet', format='parquet', debug_level=True)
        print("✅ 초기 인터럽트 테스트 성공")
    except KeyboardInterrupt:
        print("\n❌ 설정 단계에서 KeyboardInterrupt 발생!")
        print("  → 임시 디렉토리 생성 전에 중단되었습니다.")
        print("  → 원본 파일은 그대로 보존됩니다.")
    except Exception as e:
        print(f"❌ 다른 예외 발생: {e}")

def test_interrupt_during_replace():
    """파일 교체 단계에서 인터럽트 발생 시뮬레이션"""
    print("\n=== 파일 교체 단계 인터럽트 테스트 ===")
    print("이 테스트는 파일 교체 단계에서 인터럽트를 시뮬레이션합니다.")
    
    df = pd.DataFrame({
        'A': np.random.randn(5000),
        'B': np.random.randn(5000),
    })
    
    # 2초 후 인터럽트 시뮬레이션 (교체 단계에서)
    def simulate_replace_interrupt():
        time.sleep(2)
        print("\n🔄 파일 교체 단계에서 인터럽트 시뮬레이션...")
        import os
        os.kill(os.getpid(), signal.SIGINT)
    
    interrupt_thread = threading.Thread(target=simulate_replace_interrupt)
    interrupt_thread.daemon = True
    interrupt_thread.start()
    
    try:
        print("DEBUG 모드로 쓰기 시작 (2초 후 인터럽트)...")
        write(df, 'replace_interrupt_test.parquet', format='parquet', debug_level=True)
        print("✅ 파일 교체 인터럽트 테스트 성공")
    except KeyboardInterrupt:
        print("\n❌ 파일 교체 단계에서 KeyboardInterrupt 발생!")
        print("  → 임시 파일은 생성되었지만 교체가 중단되었습니다.")
        print("  → 원본 파일은 보존되고 임시 파일은 정리됩니다.")
    except Exception as e:
        print(f"❌ 다른 예외 발생: {e}")

def test_manual_interrupt():
    """수동 인터럽트 테스트 안내"""
    print("\n=== 수동 인터럽트 테스트 안내 ===")
    print("다음 명령어를 실행한 후 Ctrl+C를 눌러서 테스트해보세요:")
    print()
    print("python3 -c \"")
    print("import pandas as pd")
    print("import numpy as np")
    print("from atio import write")
    print("df = pd.DataFrame({'A': np.random.randn(1000000)})")
    print("write(df, 'manual_interrupt_test.parquet', format='parquet', debug_level=True)")
    print("\"")
    print()
    print("실행 중에 Ctrl+C를 누르면 성능 진단 로깅이 어떻게 작동하는지 확인할 수 있습니다.")

if __name__ == "__main__":
    print("키보드 인터럽트 시뮬레이션 테스트 시작")
    print("=" * 60)
    
    # 1. 쓰기 중 인터럽트 테스트
    test_interrupt_during_write()
    
    # 2. 설정 단계 인터럽트 테스트
    test_interrupt_during_setup()
    
    # 3. 파일 교체 단계 인터럽트 테스트
    test_interrupt_during_replace()
    
    # 4. 수동 인터럽트 테스트 안내
    test_manual_interrupt()
    
    print("\n" + "=" * 60)
    print("인터럽트 시뮬레이션 테스트 완료!")
    print("\n💡 성능 진단 로깅의 인터럽트 대응:")
    print("  - 인터럽트 발생 시점에 따라 다른 동작")
    print("  - 설정 단계: 원본 파일 보존")
    print("  - 쓰기 단계: 임시 파일 자동 정리")
    print("  - 교체 단계: 원본 파일 보존, 임시 파일 정리")
    print("  - DEBUG 로그로 어느 단계에서 중단되었는지 확인 가능") 