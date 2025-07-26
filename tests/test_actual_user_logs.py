#!/usr/bin/env python3
"""
실제 사용자가 보는 로그 확인 테스트
라이브러리를 사용할 때 실제로 어떤 로그가 출력되는지 확인
"""

import pandas as pd
import numpy as np
from atomicwriter import write

def show_actual_logs():
    """실제 사용자가 보는 로그들을 보여줍니다"""
    
    print("=" * 80)
    print("🔍 실제 사용자 로그 확인 테스트")
    print("=" * 80)
    
    # 테스트 데이터 생성
    df = pd.DataFrame({
        'A': np.random.randn(1000),
        'B': np.random.randn(1000),
    })
    
    print("\n📋 시나리오 1: 기본 사용법 (성공)")
    print("사용자 코드: write(df, 'test1.parquet', format='parquet')")
    print("-" * 50)
    write(df, 'test1.parquet', format='parquet')
    
    print("\n📋 시나리오 2: 기본 사용법 (오류)")
    print("사용자 코드: write(df, 'test2.xyz', format='xyz')")
    print("-" * 50)
    try:
        write(df, 'test2.xyz', format='xyz')
    except Exception:
        pass
    
    print("\n📋 시나리오 3: verbose 모드 (성공)")
    print("사용자 코드: write(df, 'test3.parquet', format='parquet', verbose=True)")
    print("-" * 50)
    write(df, 'test3.parquet', format='parquet', verbose=True)
    
    print("\n📋 시나리오 4: verbose 모드 (오류)")
    print("사용자 코드: write(df, 'test4.xyz', format='xyz', verbose=True)")
    print("-" * 50)
    try:
        write(df, 'test4.xyz', format='xyz', verbose=True)
    except Exception:
        pass
    
    print("\n📋 시나리오 5: 대용량 데이터 (기본)")
    print("사용자 코드: write(large_df, 'large.parquet', format='parquet')")
    print("-" * 50)
    large_df = pd.DataFrame({
        'A': np.random.randn(50000),  # 5만 행
        'B': np.random.randn(50000),
        'C': np.random.randn(50000),
    })
    write(large_df, 'large.parquet', format='parquet')
    
    print("\n📋 시나리오 6: 대용량 데이터 (verbose)")
    print("사용자 코드: write(large_df, 'large_verbose.parquet', format='parquet', verbose=True)")
    print("-" * 50)
    write(large_df, 'large_verbose.parquet', format='parquet', verbose=True)
    
    print("\n" + "=" * 80)
    print("✅ 테스트 완료!")
    print("\n💡 사용자 관점에서 확인된 것들:")
    print("  - 기본 사용법: 성공/실패 + 소요 시간 정보")
    print("  - verbose 모드: 상세한 단계별 성능 정보")
    print("  - 오류 발생 시: 원인과 소요 시간 정보")
    print("  - 대용량 데이터: 성능 추적 가능")

if __name__ == "__main__":
    show_actual_logs() 