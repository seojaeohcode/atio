빠른 시작
==========

Atio를 사용하여 안전한 파일 쓰기를 시작해보세요.

기본 사용법
----------

가장 간단한 사용법부터 시작해보겠습니다:

.. code-block:: python

   import atio
   import pandas as pd

   # 샘플 데이터 생성
   df = pd.DataFrame({
       "name": ["Alice", "Bob", "Charlie"],
       "age": [25, 30, 35],
       "city": ["Seoul", "Busan", "Incheon"]
   })

   # 안전한 파일 쓰기
   atio.write(df, "users.parquet", format="parquet")

이 코드는 다음과 같은 안전장치를 제공합니다:

- ✅ 임시 파일에 먼저 저장
- ✅ 저장 완료 후 원자적으로 파일 교체
- ✅ 실패 시 원본 파일 보존
- ✅ `_SUCCESS` 플래그 파일 생성

다양한 형식 지원
---------------

Atio는 다양한 데이터 형식을 지원합니다:

.. code-block:: python

   # CSV 형식
   atio.write(df, "users.csv", format="csv", index=False)

   # Excel 형식
   atio.write(df, "users.xlsx", format="excel", sheet_name="Users")

   # JSON 형식
   atio.write(df, "users.json", format="json", orient="records")

   # Parquet 형식 (권장)
   atio.write(df, "users.parquet", format="parquet")

진행률 표시
----------

대용량 데이터 처리 시 진행률을 확인할 수 있습니다:

.. code-block:: python

   # 진행률 표시 활성화
   atio.write(large_df, "big_data.parquet", format="parquet", show_progress=True)

성능 모니터링
------------

상세한 성능 정보를 확인하려면:

.. code-block:: python

   # 성능 정보 출력
   atio.write(df, "data.parquet", format="parquet", verbose=True)

Polars DataFrame 지원
-------------------

Polars DataFrame도 지원합니다:

.. code-block:: python

   import polars as pl

   # Polars DataFrame 생성
   polars_df = pl.DataFrame({
       "a": [1, 2, 3],
       "b": [4, 5, 6]
   })

   # Polars DataFrame 저장
   atio.write(polars_df, "data.parquet", format="parquet")

데이터베이스 저장
---------------

SQL 데이터베이스에 직접 저장할 수도 있습니다:

.. code-block:: python

   from sqlalchemy import create_engine

   # 데이터베이스 연결
   engine = create_engine('postgresql://user:password@localhost/dbname')

   # 데이터베이스에 저장
   atio.write(df, format="sql", name="users", con=engine, if_exists="replace")

스냅샷 기능
----------

데이터 버전 관리를 위한 스냅샷 기능:

.. code-block:: python

   # 스냅샷 생성
   atio.write_snapshot(df, "users", format="parquet")

   # 스냅샷 읽기
   df = atio.read_table("users", snapshot_id="latest")

   # 오래된 스냅샷 정리
   atio.expire_snapshots("users", days=30)

다음 단계
--------

- :doc:`api` - 전체 API 참조
- :doc:`examples` - 고급 사용 예제
- :doc:`installation` - 설치 가이드 