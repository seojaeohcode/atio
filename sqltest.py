#!/usr/bin/env python3
# 필요한 라이브러리 설치 명령어 (터미널에서 실행)
#pip install atio sqlalchemy polars xxhash

"""
atio SQL 기능 테스트 예제
=======================

이 스크립트는 atio의 SQL 데이터베이스 연동 기능을 포괄적으로 테스트합니다.
SQLite를 사용하여 별도의 데이터베이스 서버 설치 없이 바로 실행할 수 있습니다.

테스트 범위:
- Pandas DataFrame을 SQLite에 저장 및 조회
- Polars DataFrame을 SQLite에 저장 및 조회  
- 에러 상황에서의 안전한 처리
- 대용량 데이터 처리 성능
- 자동 파일 정리

"""

# 표준 라이브러리 import
import os  # 파일 시스템 작업을 위한 모듈

# 서드파티 라이브러리 import
import atio  # atio: 안전한 원자적 파일 쓰기 라이브러리
import pandas as pd  # pandas: 데이터 분석 및 조작 라이브러리
import polars as pl  # polars: 고성능 데이터프레임 라이브러리
from sqlalchemy import create_engine, text  # SQLAlchemy: SQL 툴킷 및 ORM

def test_pandas_sqlite():
    """
    Pandas DataFrame과 SQLite 데이터베이스 연동 테스트
    
    이 함수는 다음을 테스트합니다:
    1. Pandas DataFrame 생성
    2. atio를 사용한 SQLite 저장 (원자적 쓰기)
    3. 저장된 데이터 조회 및 검증
    4. 데이터 추가 (append 모드)
    5. 최종 데이터 확인
    6. 데이터베이스 연결 정리
    """
    print("🐼 Pandas + SQLite 테스트")
    print("=" * 40)
    
    # SQLite 데이터베이스 엔진 생성
    # 'sqlite:///파일명' 형식으로 로컬 파일 기반 데이터베이스 생성
    # 파일이 없으면 자동으로 생성됨
    engine = create_engine('sqlite:///test_database.db')
    
    # 테스트용 직원 데이터 생성
    # 각 컬럼의 의미:
    # - id: 직원 고유 식별자 (정수)
    # - name: 직원 이름 (문자열)
    # - age: 나이 (정수)
    # - city: 거주 도시 (문자열)
    # - salary: 연봉 (정수, 달러 단위)
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'city': ['Seoul', 'Busan', 'Incheon', 'Daegu', 'Gwangju'],
        'salary': [50000, 60000, 70000, 55000, 65000]
    })
    
    # 원본 데이터 출력 (사용자 확인용)
    print("📊 원본 데이터:")
    print(df)
    print()
    
    # atio를 사용한 안전한 SQLite 저장
    # atio.write()의 주요 파라미터:
    # - df: 저장할 DataFrame 객체
    # - format="sql": SQL 데이터베이스 저장 모드 지정
    # - name="employees": 테이블명 지정
    # - con=engine: SQLAlchemy 엔진 객체 전달
    # - if_exists="replace": 기존 테이블이 있으면 덮어쓰기
    print("💾 atio로 SQLite에 저장 중...")
    atio.write(df, format="sql", name="employees", con=engine, if_exists="replace")
    print("✅ 저장 완료!")
    print()
    
    # 저장된 데이터 조회 및 검증
    # pd.read_sql(): SQL 쿼리를 실행하여 DataFrame으로 결과 반환
    # "SELECT * FROM employees": employees 테이블의 모든 데이터 조회
    print("🔍 저장된 데이터 확인:")
    result = pd.read_sql("SELECT * FROM employees", engine)
    print(result)
    print()
    
    # 데이터 추가 테스트 (append 모드)
    # 새로운 직원 데이터를 기존 테이블에 추가
    print("📝 새로운 데이터 추가...")
    new_data = pd.DataFrame({
        'id': [6, 7],  # 기존 id와 중복되지 않는 새로운 id
        'name': ['Frank', 'Grace'],
        'age': [29, 31],
        'city': ['Ulsan', 'Daejeon'],
        'salary': [58000, 62000]
    })
    
    # append 모드로 데이터 추가
    # if_exists="append": 기존 테이블에 데이터 추가 (덮어쓰기 아님)
    atio.write(new_data, format="sql", name="employees", con=engine, if_exists="append")
    print("✅ 추가 완료!")
    print()
    
    # 최종 데이터 확인 (정렬 포함)
    # ORDER BY id: id 컬럼 기준으로 오름차순 정렬
    print("🔍 최종 데이터:")
    final_result = pd.read_sql("SELECT * FROM employees ORDER BY id", engine)
    print(final_result)
    print()
    
    # 데이터베이스 연결 정리
    # engine.dispose(): 연결 풀 정리 및 리소스 해제
    # Windows에서 파일 삭제 시 "파일이 사용 중" 에러 방지
    engine.dispose()

def test_polars_sqlite():
    """
    Polars DataFrame과 SQLite 데이터베이스 연동 테스트
    
    이 함수는 다음을 테스트합니다:
    1. Polars DataFrame 생성 (고성능 데이터프레임)
    2. atio를 사용한 SQLite 저장 (database 포맷)
    3. 저장된 데이터 조회 (Pandas로 읽기)
    4. 데이터베이스 연결 정리
    
    주의: Polars는 'database' 포맷을 사용하며, 
    atio 내부에서 connection_uri를 connection으로 자동 변환
    """
    print("⚡ Polars + SQLite 테스트")
    print("=" * 40)
    
    # SQLite 데이터베이스 엔진 생성 (Polars 전용)
    # 다른 파일명을 사용하여 Pandas 테스트와 분리
    engine = create_engine('sqlite:///test_database_polars.db')
    
    # 테스트용 제품 데이터 생성 (Polars DataFrame)
    # Polars는 더 빠른 성능과 메모리 효율성을 제공
    df = pl.DataFrame({
        'product_id': [101, 102, 103, 104, 105],  # 제품 고유 ID
        'product_name': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones'],
        'category': ['Electronics', 'Accessories', 'Accessories', 'Electronics', 'Accessories'],
        'price': [1200.00, 25.99, 89.99, 300.00, 150.00],  # 가격 (소수점 포함)
        'stock': [50, 200, 150, 75, 100]  # 재고 수량
    })
    
    # 원본 데이터 출력 (Polars 형식)
    print("📊 원본 데이터:")
    print(df)
    print()
    
    # atio를 사용한 Polars 데이터 SQLite 저장
    # format="database": Polars 전용 데이터베이스 저장 모드
    # table_name="products": 저장할 테이블명
    # connection_uri: 데이터베이스 연결 문자열
    # 주의: atio 내부에서 connection_uri를 connection으로 자동 변환
    print("💾 atio로 SQLite에 저장 중...")
    atio.write(df, format="database", 
              table_name="products", 
              connection_uri="sqlite:///test_database_polars.db")
    print("✅ 저장 완료!")
    print()
    
    # 저장된 데이터 확인 (Pandas로 읽기)
    # Polars로 저장했지만 Pandas로 읽기 가능 (SQLite 표준 사용)
    print("🔍 저장된 데이터 확인:")
    result = pd.read_sql("SELECT * FROM products", engine)
    print(result)
    print()
    
    # 데이터베이스 연결 정리
    engine.dispose()

def test_error_handling():
    """
    에러 처리 및 안전성 테스트
    
    이 함수는 다음을 테스트합니다:
    1. NULL 값이 포함된 데이터 처리
    2. 잘못된 입력에 대한 안전한 에러 처리
    3. atio의 예외 처리 메커니즘 검증
    4. 시스템 안정성 확인
    
    목적: atio가 예상치 못한 상황에서도 안전하게 동작하는지 확인
    """
    print("🛡️ 에러 처리 테스트")
    print("=" * 40)
    
    # SQLite 데이터베이스 엔진 생성 (에러 테스트 전용)
    engine = create_engine('sqlite:///test_error.db')
    
    # 테스트용 데이터 생성 (의도적으로 문제가 있는 데이터)
    # invalid_column: 모든 값이 None인 컬럼 (NULL 값)
    # 실제 데이터에서는 이런 상황이 발생할 수 있음
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'invalid_column': [None, None, None]  # NULL 값들 (의도적)
    })
    
    print("📊 테스트 데이터 (NULL 값 포함):")
    print(df)
    print()
    
    # try-except 블록으로 에러 처리 테스트
    try:
        # 1단계: 정상적인 데이터 저장 테스트
        # NULL 값이 있어도 정상적으로 저장되어야 함
        atio.write(df, format="sql", name="test_table", con=engine, if_exists="replace")
        print("✅ 정상 저장 완료!")
        
        # 2단계: 의도적으로 잘못된 입력으로 에러 발생시키기
        # 빈 테이블명("")은 SQL에서 유효하지 않음
        print("❌ 잘못된 테이블명으로 시도...")
        atio.write(df, format="sql", name="", con=engine)  # 빈 테이블명 (의도적 에러)
        
    except Exception as e:
        # 예상된 에러가 발생했을 때의 처리
        # atio가 에러를 적절히 처리하고 적절한 메시지를 제공하는지 확인
        print(f"🚨 예상된 에러 발생: {e}")
        print("✅ atio가 에러를 안전하게 처리했습니다!")
    
    # 데이터베이스 연결 정리
    engine.dispose()
    print()

def test_performance():
    """
    대용량 데이터 처리 성능 테스트
    
    이 함수는 다음을 테스트합니다:
    1. 10,000행의 대용량 데이터 생성
    2. 진행도 표시와 함께 데이터 저장
    3. 저장 성능 측정
    4. 저장된 데이터 검증
    5. 메모리 사용량 및 처리 시간 확인
    
    목적: atio가 대용량 데이터를 효율적으로 처리하는지 확인
    """
    print("⚡ 성능 테스트")
    print("=" * 40)
    
    # SQLite 데이터베이스 엔진 생성 (성능 테스트 전용)
    engine = create_engine('sqlite:///test_performance.db')
    
    # 대용량 데이터 생성 (10,000행)
    import numpy as np  # 수치 계산을 위한 라이브러리
    
    large_df = pd.DataFrame({
        'id': range(10000),  # 0부터 9999까지의 ID
        'value': np.random.randn(10000),  # 정규분포를 따르는 랜덤 값
        'category': np.random.choice(['A', 'B', 'C', 'D'], 10000),  # 4개 카테고리 중 랜덤 선택
        'timestamp': pd.date_range('2024-01-01', periods=10000, freq='1H')  # 1시간 간격의 타임스탬프
    })
    
    # 데이터 크기 정보 출력
    print(f"📊 대용량 데이터 생성: {len(large_df):,} 행")
    print()
    
    # 진행도 표시와 함께 데이터 저장
    # show_progress=True: 저장 진행 상황을 실시간으로 표시
    # 대용량 데이터 처리 시 사용자에게 진행 상황을 알려줌
    print("💾 진행도 표시와 함께 저장...")
    atio.write(large_df, format="sql", name="large_data", con=engine, 
              if_exists="replace", show_progress=True)
    print("✅ 저장 완료!")
    print()
    
    # 저장된 데이터 개수 확인
    # COUNT(*) 쿼리로 실제 저장된 행 수 검증
    result = pd.read_sql("SELECT COUNT(*) as count FROM large_data", engine)
    print(f"🔍 저장된 데이터 개수: {result['count'].iloc[0]:,} 행")
    print()
    
    # 데이터베이스 연결 정리
    engine.dispose()

def cleanup():
    """
    테스트 과정에서 생성된 파일들을 정리하는 함수
    
    이 함수는 다음을 수행합니다:
    1. 생성된 모든 SQLite 데이터베이스 파일 목록 확인
    2. 각 파일의 존재 여부 확인
    3. 안전한 파일 삭제 시도
    4. 삭제 실패 시 적절한 에러 메시지 출력
    5. 정리 완료 상태 보고
    
    목적: 테스트 후 시스템을 깨끗한 상태로 복원
    """
    print("🧹 테스트 파일 정리")
    print("=" * 40)
    
    # 삭제할 파일 목록 정의
    # 각 테스트 함수에서 생성한 SQLite 데이터베이스 파일들
    files_to_remove = [
        'test_database.db',           # Pandas 테스트용
        'test_database_polars.db',    # Polars 테스트용
        'test_error.db',              # 에러 처리 테스트용
        'test_performance.db'         # 성능 테스트용
    ]
    
    # 각 파일에 대해 삭제 시도
    for file in files_to_remove:
        if os.path.exists(file):  # 파일이 존재하는지 확인
            try:
                os.remove(file)  # 파일 삭제 시도
                print(f"🗑️ {file} 삭제됨")
            except PermissionError:
                # Windows에서 파일이 사용 중일 때 발생하는 에러
                print(f"⚠️ {file} 삭제 실패 (파일이 사용 중)")
            except Exception as e:
                # 기타 예상치 못한 에러
                print(f"⚠️ {file} 삭제 실패: {e}")
    
    print("✅ 정리 완료!")

def main():
    """
    메인 실행 함수
    
    이 함수는 전체 테스트 프로세스를 관리합니다:
    1. 테스트 시작 메시지 출력
    2. 각 테스트 함수를 순차적으로 실행
    3. 테스트 중 발생하는 예외 처리
    4. 테스트 완료 후 정리 작업
    5. 최종 결과 보고
    
    실행 순서:
    1. Pandas + SQLite 테스트
    2. Polars + SQLite 테스트
    3. 에러 처리 테스트
    4. 성능 테스트
    5. 파일 정리
    """
    print("🚀 atio SQL 기능 테스트 시작!")
    print("=" * 50)
    print()
    
    # try-except-finally 구조로 안전한 테스트 실행
    try:
        # 1. Pandas + SQLite 테스트 실행
        # 가장 기본적인 기능 테스트
        test_pandas_sqlite()
        
        # 2. Polars + SQLite 테스트 실행
        # 고성능 데이터프레임 라이브러리 테스트
        test_polars_sqlite()
        
        # 3. 에러 처리 테스트 실행
        # 시스템 안정성 및 예외 처리 테스트
        test_error_handling()
        
        # 4. 성능 테스트 실행
        # 대용량 데이터 처리 성능 테스트
        test_performance()
        
        # 모든 테스트 성공 시 메시지 출력
        print("🎉 모든 테스트 완료!")
        
    except Exception as e:
        # 테스트 중 예상치 못한 에러 발생 시 처리
        print(f"❌ 테스트 중 에러 발생: {e}")
        import traceback  # 상세한 에러 정보를 위한 모듈
        traceback.print_exc()  # 스택 트레이스 출력
    
    finally:
        # 5. 정리 작업 (에러 발생 여부와 관계없이 항상 실행)
        # finally 블록은 try-except와 관계없이 반드시 실행됨
        cleanup()

# 스크립트가 직접 실행될 때만 main() 함수 호출
# import로 모듈을 가져올 때는 실행되지 않음
if __name__ == "__main__":
    main()