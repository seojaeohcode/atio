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
실제 사용자가 보는 로그 확인 테스트
라이브러리를 사용할 때 실제로 어떤 로그가 출력되는지 확인
"""

import pandas as pd
import numpy as np
from atio import write

def show_actual_logs(tmp_path):
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
    write(df, tmp_path/'test1.parquet', format='parquet')
    
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
    write(df, tmp_path/'test3.parquet', format='parquet', verbose=True)
    
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
    write(large_df, tmp_path/'large.parquet', format='parquet')
    
    print("\n📋 시나리오 6: 대용량 데이터 (verbose)")
    print("사용자 코드: write(large_df, 'large_verbose.parquet', format='parquet', verbose=True)")
    print("-" * 50)
    write(large_df, tmp_path/'large_verbose.parquet', format='parquet', verbose=True)
    
    print("\n" + "=" * 80)
    print("✅ 테스트 완료!")
    print("\n💡 사용자 관점에서 확인된 것들:")
    print("  - 기본 사용법: 성공/실패 + 소요 시간 정보")
    print("  - verbose 모드: 상세한 단계별 성능 정보")
    print("  - 오류 발생 시: 원인과 소요 시간 정보")
    print("  - 대용량 데이터: 성능 추적 가능")
