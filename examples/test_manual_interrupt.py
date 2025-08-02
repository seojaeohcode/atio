#!/usr/bin/env python3
"""
수동 인터럽트 테스트용 스크립트
실행 중에 Ctrl+C를 눌러서 성능 진단 로깅이 어떻게 작동하는지 확인하세요.
"""

import pandas as pd
import numpy as np
from atio import write

def main():
    print("=== 수동 인터럽트 테스트 ===")
    print("이 스크립트는 대용량 데이터를 쓰는 동안 Ctrl+C를 눌러서 테스트할 수 있습니다.")
    print("실행 중에 Ctrl+C를 눌러보세요!")
    print()
    
    # 대용량 데이터 생성 (인터럽트가 발생할 수 있도록 충분히 큰 데이터)
    print("대용량 데이터 생성 중...")
    df = pd.DataFrame({
        'A': np.random.randn(1000000),  # 100만 행
        'B': np.random.randn(1000000),
        'C': np.random.randn(1000000),
        'D': np.random.randn(1000000),
        'E': np.random.randn(1000000),
        'F': np.random.randn(1000000),
        'G': np.random.randn(1000000),
        'H': np.random.randn(1000000),
    })
    
    print(f"생성된 데이터 크기: {df.shape}")
    print("이제 DEBUG 모드로 쓰기를 시작합니다...")
    print("쓰기 중에 Ctrl+C를 눌러서 인터럽트를 발생시켜보세요!")
    print()
    
    try:
        # 성능 진단 로깅 활성화
        write(df, 'manual_interrupt_test.parquet', format='parquet', debug_level=True)
        print("✅ 쓰기 작업이 성공적으로 완료되었습니다!")
        print("  → 인터럽트 없이 모든 단계가 완료되었습니다.")
        
    except KeyboardInterrupt:
        print("\n❌ KeyboardInterrupt 발생!")
        print("  → 쓰기 작업이 중간에 중단되었습니다.")
        print("  → AtomicWriter는 원본 파일을 보존합니다.")
        print("  → 임시 파일은 자동으로 정리됩니다.")
        print("  → DEBUG 로그를 통해 어느 단계에서 중단되었는지 확인할 수 있습니다.")
        
    except Exception as e:
        print(f"\n❌ 다른 예외 발생: {e}")
        print("  → 예상치 못한 오류가 발생했습니다.")

if __name__ == "__main__":
    main() 