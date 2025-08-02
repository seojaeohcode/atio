#!/usr/bin/env python3
"""
Atio 사용 예제 데모
안전한 원자적 파일 쓰기의 다양한 사용법을 보여줍니다.

🎯 Atio의 주요 함수들 (Public API):
1. atio.write() - 메인 함수
   매개변수:
   - obj: 저장할 데이터 객체 (DataFrame, NumPy 배열 등)
   - target_path: 저장할 파일 경로
   - format: 파일 형식 ('parquet', 'csv', 'excel' 등)
   - show_progress: 진행도 표시 여부 (기본: False)
   - verbose: 상세한 성능 진단 정보 출력 (기본: False)
   - **kwargs: 저장 함수에 전달할 추가 인자

2. atio.register_writer() - 플러그인 등록
   매개변수:
   - obj_type: 객체 타입 (예: pd.DataFrame)
   - fmt: 파일 형식 (예: 'custom')
   - handler: 처리 함수

📊 지원하는 데이터 타입:
- Pandas: pd.DataFrame → csv, excel, parquet, json, pickle, html
- Polars: pl.DataFrame → csv, excel, parquet, json, ipc, avro, delta
- NumPy: np.ndarray → npy, npz, csv, bin
- dict → npz (압축)

🔧 내부 함수들 (Internal):
- Core 모듈: _execute_write(), _execute_write_with_progress()
- Plugins 모듈: get_writer()
- Utils 모듈: setup_logger(), ProgressBar 클래스
"""

import atio
import pandas as pd
import numpy as np
import time

def demo_basic_usage():
    """
    기본 사용법 데모
    atio.write() 함수의 기본적인 사용법을 보여줍니다.
    
    지원 형식:
    - Pandas: csv, excel, parquet, json, pickle, html
    - Polars: csv, excel, parquet, json, ipc, avro, delta
    - NumPy: npy, npz, csv, bin
    """
    print("=" * 50)
    print("1. 기본 사용법")
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
    
    atio.write(df, "users.csv", format="csv")
    print("✅ users.csv 저장 완료")
    
    print("\n📁 생성된 파일들:")
    import os
    for file in ["users.parquet", "users.csv"]:
        if os.path.exists(file):
            size = os.path.getsize(file)
            print(f"  - {file} ({size} bytes)")
            # _SUCCESS 파일 확인
            success_file = file + "._SUCCESS"
            if os.path.exists(success_file):
                print(f"    └─ {success_file} (저장 완료 플래그)")

def demo_large_data():
    """
    대용량 데이터 저장 데모
    show_progress=True 옵션을 사용한 진행도 표시 기능을 보여줍니다.
    
    매개변수:
    - show_progress: True일 때 실시간 진행도 표시
    - 멀티스레딩으로 진행도를 모니터링하면서 저장
    """
    print("\n" + "=" * 50)
    print("2. 대용량 데이터 저장 (진행도 표시)")
    print("=" * 50)
    
    # 대용량 DataFrame 생성 (10만 행 x 5열)
    print("📊 대용량 데이터 생성 중...")
    large_df = pd.DataFrame({
        "A": np.random.randn(100000),
        "B": np.random.randn(100000),
        "C": np.random.randn(100000),
        "D": np.random.randn(100000),
        "E": np.random.randn(100000)
    })
    
    print(f"생성된 데이터 크기: {large_df.shape}")
    print(f"메모리 사용량: {large_df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
    print()
    
    # 진행도를 보면서 저장
    print("💾 대용량 파일 저장 중 (진행도 표시)...")
    atio.write(large_df, "large_data.parquet", format="parquet", show_progress=True)
    print("✅ large_data.parquet 저장 완료")

def demo_performance_analysis():
    """
    성능 분석 데모
    verbose=True 옵션을 사용한 상세한 성능 진단 정보를 보여줍니다.
    
    측정되는 단계:
    - setup: 임시 폴더 생성 및 초기 설정
    - write_call: 실제 데이터 쓰기 함수 호출
    - replace: 원자적 파일 교체
    - success_flag: _SUCCESS 플래그 파일 생성
    - total: 전체 작업 시간
    """
    print("\n" + "=" * 50)
    print("3. 성능 분석 (verbose 모드)")
    print("=" * 50)
    
    # 중간 크기 데이터 생성
    df = pd.DataFrame({
        "x": np.random.randn(10000),
        "y": np.random.randn(10000),
        "z": np.random.randn(10000)
    })
    
    print("📊 성능 분석용 데이터 생성 완료")
    print(f"데이터 크기: {df.shape}")
    print()
    
    # verbose 모드로 저장 (각 단계별 시간 측정)
    print("🔍 성능 분석 모드로 저장 중...")
    atio.write(df, "performance_test.parquet", format="parquet", verbose=True)
    print("✅ 성능 분석 완료")

def demo_numpy_arrays():
    """
    NumPy 배열 저장 데모
    NumPy 배열의 다양한 저장 형식을 보여줍니다.
    
    지원 형식:
    - npy: NumPy 기본 바이너리 형식
    - npz: 압축된 NumPy 형식
    - csv: 텍스트 형식
    - bin: 바이너리 형식
    """
    print("\n" + "=" * 50)
    print("4. NumPy 배열 저장")
    print("=" * 50)
    
    # NumPy 배열 생성
    arr = np.random.randn(1000, 1000)
    print(f"📊 NumPy 배열 생성: {arr.shape}")
    print(f"데이터 타입: {arr.dtype}")
    print()
    
    # 다양한 형식으로 저장
    print("💾 NumPy 배열 저장 중...")
    atio.write(arr, "array.npy", format="npy")
    print("✅ array.npy 저장 완료")
    
    atio.write(arr, "array.csv", format="csv")
    print("✅ array.csv 저장 완료")

def demo_error_handling():
    """
    오류 처리 데모
    Atio의 안전한 오류 처리 기능을 보여줍니다.
    
    특징:
    - 지원하지 않는 형식 시도 시 안전한 오류 처리
    - 원본 파일 보존
    - 임시 파일 자동 정리
    - 상세한 오류 메시지 제공
    """
    print("\n" + "=" * 50)
    print("5. 오류 처리 데모")
    print("=" * 50)
    
    df = pd.DataFrame({"a": [1, 2, 3]})
    
    print("❌ 지원하지 않는 형식으로 저장 시도...")
    try:
        atio.write(df, "test.xyz", format="xyz")
    except ValueError as e:
        print(f"✅ 예상된 오류 발생: {e}")
        print("   → 원본 파일은 보존되고 임시 파일만 정리됨")

def demo_polars_integration():
    """
    Polars 통합 데모
    Polars DataFrame의 저장 기능을 보여줍니다.
    
    지원 형식:
    - csv, excel, parquet, json
    - ipc: Apache Arrow IPC 형식
    - avro: Apache Avro 형식
    - delta: Delta Lake 형식
    - database: 데이터베이스 연결
    """
    print("\n" + "=" * 50)
    print("6. Polars 통합")
    print("=" * 50)
    
    try:
        import polars as pl
        
        # Polars DataFrame 생성
        df = pl.DataFrame({
            "name": ["Alice", "Bob"],
            "score": [95.5, 87.3],
            "active": [True, False]
        })
        
        print("📊 Polars DataFrame 생성:")
        print(df)
        print()
        
        # Polars DataFrame 저장
        print("💾 Polars DataFrame 저장 중...")
        atio.write(df, "polars_data.parquet", format="parquet")
        print("✅ polars_data.parquet 저장 완료")
        
    except ImportError:
        print("⚠️ Polars가 설치되지 않았습니다.")
        print("   pip install polars 로 설치하세요.")

def cleanup_demo_files():
    """데모 파일 정리"""
    print("\n" + "=" * 50)
    print("7. 데모 파일 정리")
    print("=" * 50)
    
    import os
    import glob
    
    # 생성된 파일들 목록
    demo_files = [
        "users.parquet", "users.csv",
        "large_data.parquet",
        "performance_test.parquet",
        "array.npy", "array.csv",
        "polars_data.parquet"
    ]
    
    # _SUCCESS 파일들도 포함
    success_files = [f + "._SUCCESS" for f in demo_files]
    all_files = demo_files + success_files
    
    print("🗑️ 생성된 데모 파일들:")
    for file in all_files:
        if os.path.exists(file):
            size = os.path.getsize(file)
            print(f"  - {file} ({size} bytes)")
    
    # 정리 여부 확인
    print("\n❓ 데모 파일들을 삭제하시겠습니까? (y/n): ", end="")
    response = input().lower().strip()
    
    if response == 'y':
        for file in all_files:
            if os.path.exists(file):
                os.remove(file)
                print(f"🗑️ {file} 삭제됨")
        print("✅ 모든 데모 파일이 정리되었습니다.")
    else:
        print("📁 데모 파일들이 보존되었습니다.")

def main():
    """
    메인 데모 실행
    Atio의 모든 주요 기능을 순차적으로 실행합니다.
    
    실행 순서:
    1. 기본 사용법 (다양한 형식 저장)
    2. 대용량 데이터 (진행도 표시)
    3. 성능 분석 (verbose 모드)
    4. NumPy 배열 저장
    5. 오류 처리
    6. Polars 통합
    7. 파일 정리
    """
    print("🚀 Atio 사용 예제 데모 시작!")
    print("안전한 원자적 파일 쓰기의 다양한 기능을 보여줍니다.")
    print()
    
    # 각 데모 실행
    demo_basic_usage()
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
    print("📚 주요 기능:")
    print("  ✅ 원자적 파일 쓰기 (파일 손상 방지)")
    print("  ✅ 진행도 표시 (대용량 파일)")
    print("  ✅ 성능 분석 (verbose 모드)")
    print("  ✅ 다양한 데이터 형식 지원")
    print("  ✅ 오류 시 안전한 복구")
    print("  ✅ _SUCCESS 플래그 파일 생성")
    print()
    print("🔗 더 많은 정보: https://github.com/seojaeohcode/atomic-writer")

if __name__ == "__main__":
    main() 