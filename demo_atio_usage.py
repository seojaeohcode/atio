#!/usr/bin/env python3
"""
Atio 사용 예제 데모
안전한 원자적 파일 쓰기와 데이터베이스 쓰기의 다양한 사용법을 보여줍니다.
"""
# --- 모듈 경로 설정을 위한 코드 ---
import sys
import os

# 현재 스크립트의 경로를 기준으로 'src' 폴더의 절대 경로를 계산합니다.
# 이렇게 하면 어떤 위치에서 스크립트를 실행하더라도 'atio' 모듈을 찾을 수 있습니다.
project_root = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(project_root, 'src')
if src_path not in sys.path:
    sys.path.insert(0, src_path)
# ------------------------------------

import atio
import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine

def demo_basic_usage():
    """
    기본 파일 기반 쓰기 사용법 데모
    atio.write() 함수의 기본적인 사용법을 보여줍니다.
    """
    print("=" * 50)
    print("1. 기본 사용법 (파일 쓰기)")
    print("=" * 50)
    
    # 간단한 DataFrame 생성
    df = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "age": [25, 30, 35, 28],
        "city": ["Seoul", "Busan", "Incheon", "Daegu"],
        "salary": [50000, 60000, 70000, 55000]
    })
    
    print("📊 생성된 데이터:")
    print(df)
    print()
    
    # 다양한 형식으로 저장
    print("💾 파일 저장 중...")
    atio.write(df, "users.parquet", format="parquet")
    print("✅ users.parquet 저장 완료")
    
    # to_csv에 추가 인자(index=False)를 전달하는 예시
    atio.write(df, "users.csv", format="csv", index=False)
    print("✅ users.csv 저장 완료 (인덱스 제외)")
    
    print("\n📁 생성된 파일들:")
    for file in ["users.parquet", "users.csv"]:
        if os.path.exists(file):
            size = os.path.getsize(file)
            print(f"  - {file} ({size} bytes)")
            # _SUCCESS 파일 확인
            success_file = os.path.join(os.path.dirname(file), f".{os.path.basename(file)}._SUCCESS")
            if os.path.exists(success_file):
                print(f"    └─ {os.path.basename(success_file)} (저장 완료 플래그)")

def demo_excel_and_sql():
    """
    Excel 및 SQL 데이터베이스 쓰기 데모
    파일 시스템을 사용하지 않는 경우의 atio.write() 사용법을 보여줍니다.
    """
    print("\n" + "=" * 50)
    print("2. Excel 및 SQL 데이터베이스 쓰기")
    print("=" * 50)

    df = pd.DataFrame({
        "product_id": [101, 102, 103],
        "product_name": ["Laptop", "Mouse", "Keyboard"],
        "price": [1200, 25, 75]
    })
    print("📊 예제 데이터 (Products):")
    print(df)
    print()

    # --- Excel 쓰기 데모 ---
    print("💾 Excel 파일 저장 중...")
    try:
        # to_excel에 추가 인자를 kwargs로 전달
        atio.write(df, "products.xlsx", format="excel", index=False, sheet_name="Stock")
        print("✅ products.xlsx 저장 완료 (Sheet: Stock, 인덱스 제외)")
    except Exception as e:
        print(f"❌ Excel 저장 실패: {e}")
        print("  (필요 라이브러리 설치: pip install openpyxl)")
    
    print("-" * 20)

    # --- SQL 쓰기 데모 ---
    print("💾 SQL 데이터베이스에 저장 중...")
    try:
        # DB 쓰기를 위한 SQLAlchemy 엔진 생성 (인메모리 SQLite 사용)
        # 실제 사용 시에는 PostgreSQL, MySQL 등의 DB 연결 문자열 사용
        engine = create_engine('sqlite:///:memory:')

        # DB 쓰기 시 target_path는 None 또는 생략
        # 'name' (테이블명), 'con' (커넥션)은 kwargs로 전달
        atio.write(df, format="sql", name="products", con=engine, if_exists='replace', index=False)
        print("✅ 'products' 테이블에 데이터 저장 완료 (in-memory SQLite)")

        # 검증: 데이터베이스에서 다시 읽어오기
        print("\n🔍 검증: 데이터베이스에서 데이터 읽기...")
        with engine.connect() as connection:
            read_df = pd.read_sql("SELECT * FROM products", connection)
            print(read_df)
            print("✅ 검증 완료: 데이터가 성공적으로 저장되었습니다.")

    except ImportError:
        print("❌ SQL 저장 실패: sqlalchemy 라이브러리가 필요합니다.")
        print("  (설치: pip install sqlalchemy)")
    except Exception as e:
        print(f"❌ SQL 저장 중 오류 발생: {e}")


def demo_large_data():
    """
    대용량 데이터 저장 데모
    show_progress=True 옵션을 사용한 진행도 표시 기능을 보여줍니다.
    """
    print("\n" + "=" * 50)
    print("3. 대용량 데이터 저장 (진행도 표시)")
    print("=" * 50)
    
    print("📊 대용량 데이터 생성 중...")
    large_df = pd.DataFrame(np.random.randn(200000, 5), columns=list("ABCDE"))
    
    print(f"생성된 데이터 크기: {large_df.shape}")
    print(f"메모리 사용량: {large_df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
    print()
    
    print("💾 대용량 파일 저장 중 (진행도 표시)...")
    atio.write(large_df, "large_data.parquet", format="parquet", show_progress=True)

def demo_performance_analysis():
    """
    성능 분석 데모
    verbose=True 옵션을 사용한 상세한 성능 진단 정보를 보여줍니다.
    """
    print("\n" + "=" * 50)
    print("4. 성능 분석 (verbose 모드)")
    print("=" * 50)
    
    df = pd.DataFrame(np.random.randn(10000, 3), columns=list("xyz"))
    
    print("📊 성능 분석용 데이터 생성 완료")
    print(f"데이터 크기: {df.shape}")
    print()
    
    print("🔍 성능 분석 모드로 저장 중...")
    atio.write(df, "performance_test.parquet", format="parquet", verbose=True)
    print("✅ 성능 분석 완료")

def demo_numpy_arrays():
    """
    NumPy 배열 저장 데모
    NumPy 배열의 다양한 저장 형식을 보여줍니다.
    """
    print("\n" + "=" * 50)
    print("5. NumPy 배열 저장")
    print("=" * 50)
    
    arr = np.random.randn(1000, 100)
    print(f"📊 NumPy 배열 생성: {arr.shape}, dtype: {arr.dtype}")
    print()
    
    print("💾 NumPy 배열 저장 중...")
    atio.write(arr, "array.npy", format="npy", show_progress=True)
    
    # 딕셔너리를 npz로 저장
    atio.write({'arr1': arr, 'arr2': arr * 2}, "arrays.npz", format="npz")
    print("✅ array.npy 및 arrays.npz 저장 완료")

def demo_error_handling():
    """
    오류 처리 데모
    Atio의 안전한 오류 처리 기능을 보여줍니다.
    """
    print("\n" + "=" * 50)
    print("6. 오류 처리 데모")
    print("=" * 50)
    
    df = pd.DataFrame({"a": [1, 2, 3]})
    
    print("❌ 지원하지 않는 형식으로 저장 시도...")
    try:
        atio.write(df, "test.xyz", format="xyz")
    except ValueError as e:
        print(f"✅ 예상된 오류 발생: {e}")
        print("  → 원본 파일은 보존되고 임시 파일만 정리됨")

def demo_polars_integration():
    """
    Polars 통합 데모
    Polars DataFrame의 저장 기능을 보여줍니다.
    """
    print("\n" + "=" * 50)
    print("7. Polars 통합")
    print("=" * 50)
    
    try:
        import polars as pl
        
        df = pl.DataFrame({
            "name": ["Alice", "Bob"], "score": [95.5, 87.3], "active": [True, False]
        })
        
        print("📊 Polars DataFrame 생성:")
        print(df)
        print()
        
        print("💾 Polars DataFrame 저장 중...")
        atio.write(df, "polars_data.parquet", format="parquet")
        print("✅ polars_data.parquet 저장 완료")
        
    except ImportError:
        print("⚠️ Polars가 설치되지 않았습니다. (pip install polars)")

def cleanup_demo_files():
    """데모 실행 후 생성된 파일들을 정리합니다."""
    print("\n" + "=" * 50)
    print("8. 데모 파일 정리")
    print("=" * 50)
    
    demo_files = [
        "users.parquet", "users.csv", "products.xlsx",
        "large_data.parquet", "performance_test.parquet",
        "array.npy", "arrays.npz", "polars_data.parquet"
    ]
    
    all_files_to_check = []
    for f in demo_files:
        all_files_to_check.append(f)
        # Add success flag file to the list for cleanup
        success_flag = os.path.join(os.path.dirname(f), f".{os.path.basename(f)}._SUCCESS")
        all_files_to_check.append(success_flag)

    found_files = [f for f in all_files_to_check if os.path.exists(f)]

    if not found_files:
        print("🗑️ 정리할 데모 파일이 없습니다.")
        return

    print("🗑️ 생성된 데모 파일 목록:")
    for file in found_files:
        size = os.path.getsize(file)
        print(f"  - {file} ({size} bytes)")
    
    print("\n❓ 데모 파일들을 삭제하시겠습니까? (y/n): ", end="")
    try:
        response = input().lower().strip()
    except (EOFError, KeyboardInterrupt):
        response = 'n'
        print("\n입력 없이 종료하여 파일을 보존합니다.")

    if response == 'y':
        for file in found_files:
            if os.path.exists(file):
                os.remove(file)
                print(f"🗑️ {file} 삭제됨")
        print("\n✅ 모든 데모 파일이 정리되었습니다.")
    else:
        print("\n📁 데모 파일들이 보존되었습니다.")

def main():
    """
    메인 데모 실행 함수
    Atio의 모든 주요 기능을 순차적으로 실행합니다.
    """
    print("🚀 Atio 사용 예제 데모 시작!")
    print("안전한 원자적 파일 쓰기와 데이터베이스 쓰기의 다양한 기능을 보여줍니다.")
    
    # 각 데모 실행
    demo_basic_usage()
    demo_excel_and_sql()
    demo_large_data()
    demo_performance_analysis()
    demo_numpy_arrays()
    demo_error_handling()
    demo_polars_integration()
    
    # 파일 정리
    cleanup_demo_files()
    
    print("\n" + "=" * 50)
    print("🎉 Atio 데모 완료!")
    print("=" * 50)

if __name__ == "__main__":
    main()
