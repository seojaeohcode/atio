#!/usr/bin/env python3
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