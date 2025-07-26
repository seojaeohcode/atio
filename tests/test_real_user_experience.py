#!/usr/bin/env python3
"""
실제 사용자 경험 시뮬레이션 테스트
사용자가 라이브러리를 사용할 때 어떤 로그가 나오는지 확인
"""

import pandas as pd
import numpy as np
from atomicwriter import write

def simulate_real_user():
    """실제 사용자 시나리오 시뮬레이션"""
    print("=== 실제 사용자 시나리오 시뮬레이션 ===")
    print("사용자가 라이브러리를 사용할 때 나오는 로그들:")
    print("-" * 50)
    
    # 시나리오 1: 기본 사용법 (성공)
    print("\n1. 기본 사용법 - 성공 케이스:")
    print("사용자 코드: write(df, 'data.parquet', format='parquet')")
    df = pd.DataFrame({
        'A': np.random.randn(5000),
        'B': np.random.randn(5000),
    })
    write(df, 'user_data.parquet', format='parquet')
    
    # 시나리오 2: 기본 사용법 (오류)
    print("\n2. 기본 사용법 - 오류 케이스:")
    print("사용자 코드: write(df, 'data.xyz', format='xyz')")
    try:
        write(df, 'user_data.xyz', format='xyz')
    except Exception:
        pass
    
    # 시나리오 3: verbose 모드 (성공)
    print("\n3. verbose 모드 - 성공 케이스:")
    print("사용자 코드: write(df, 'data.parquet', format='parquet', verbose=True)")
    write(df, 'user_data_verbose.parquet', format='parquet', verbose=True)
    
    # 시나리오 4: verbose 모드 (오류)
    print("\n4. verbose 모드 - 오류 케이스:")
    print("사용자 코드: write(df, 'data.xyz', format='xyz', verbose=True)")
    try:
        write(df, 'user_data_verbose.xyz', format='xyz', verbose=True)
    except Exception:
        pass
    
    # 시나리오 5: 대용량 데이터
    print("\n5. 대용량 데이터 - 기본 사용법:")
    print("사용자 코드: write(large_df, 'large_data.parquet', format='parquet')")
    large_df = pd.DataFrame({
        'A': np.random.randn(50000),  # 5만 행
        'B': np.random.randn(50000),
        'C': np.random.randn(50000),
    })
    write(large_df, 'user_large_data.parquet', format='parquet')
    
    print("\n" + "=" * 50)
    print("시뮬레이션 완료!")
    print("\n💡 사용자 관점에서 확인할 수 있는 것들:")
    print("  - 기본 사용법에서도 성공/실패 정보 제공")
    print("  - verbose 모드에서 상세한 성능 정보 제공")
    print("  - 오류 발생 시 원인과 소요 시간 정보 제공")
    print("  - 대용량 데이터 처리 시 성능 추적 가능")

if __name__ == "__main__":
    simulate_real_user() 