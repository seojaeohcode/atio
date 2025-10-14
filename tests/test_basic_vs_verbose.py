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
기본 사용법 vs verbose 사용법 비교 테스트
"""

import pandas as pd
import numpy as np
from atio import write

def test_basic_usage(tmp_path):
    """기본 사용법 테스트 (verbose=False)"""
    print("\n=== 기본 사용법 테스트 (verbose=False) ===")
    
    df = pd.DataFrame({
        'A': np.random.randn(10000),
        'B': np.random.randn(10000),
    })
    
    try:
        print("기본 사용법으로 저장 시도...")
        write(df, tmp_path / 'basic_test.parquet', format='parquet')  # verbose=False (기본값)
        print("✅ 기본 사용법 테스트 성공")
    except Exception as e:
        print(f"❌ 기본 사용법 테스트 실패: {e}")

def test_verbose_usage(tmp_path):
    """verbose 사용법 테스트 (verbose=True)"""
    print("\n=== verbose 사용법 테스트 (verbose=True) ===")
    
    df = pd.DataFrame({
        'A': np.random.randn(10000),
        'B': np.random.randn(10000),
    })
    
    try:
        print("verbose 모드로 저장 시도...")
        write(df, tmp_path / 'verbose_test.parquet', format='parquet', verbose=True)
        print("✅ verbose 사용법 테스트 성공")
    except Exception as e:
        print(f"❌ verbose 사용법 테스트 실패: {e}")

def test_error_basic(tmp_path):
    """기본 사용법에서 오류 발생 시"""
    print("\n=== 기본 사용법에서 오류 발생 시 ===")
    
    df = pd.DataFrame({
        'A': np.random.randn(1000),
        'B': np.random.randn(1000),
    })
    
    try:
        print("지원하지 않는 형식으로 저장 시도 (기본 사용법)...")
        write(df, tmp_path / 'error_basic.xyz', format='xyz')  # verbose=False
        print("✅ 기본 사용법 오류 테스트 성공")
    except Exception as e:
        print(f"❌ 기본 사용법 오류 테스트 실패: {e}")

def test_error_verbose(tmp_path):
    """verbose 사용법에서 오류 발생 시"""
    print("\n=== verbose 사용법에서 오류 발생 시 ===")
    
    df = pd.DataFrame({
        'A': np.random.randn(1000),
        'B': np.random.randn(1000),
    })
    
    try:
        print("지원하지 않는 형식으로 저장 시도 (verbose 사용법)...")
        write(df, tmp_path / 'error_verbose.xyz', format='xyz', verbose=True)
        print("✅ verbose 사용법 오류 테스트 성공")
    except Exception as e:
        print(f"❌ verbose 사용법 오류 테스트 실패: {e}")