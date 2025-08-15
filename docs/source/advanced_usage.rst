고급 사용법
==========

이 섹션에서는 Atio의 고급 기능들을 다룹니다.

스냅샷 기반 버전 관리
--------------------

Atio는 데이터의 버전을 관리할 수 있는 스냅샷 시스템을 제공합니다.

기본 스냅샷 쓰기
~~~~~~~~~~~~~~~

.. code-block:: python

   import atio
   import pandas as pd

   # 데이터 생성
   df = pd.DataFrame({
       "id": [1, 2, 3],
       "name": ["Alice", "Bob", "Charlie"]
   })

   # 스냅샷으로 저장 (버전 관리)
   atio.write_snapshot(df, "users_table", format="parquet")
   
   # 새로운 데이터로 업데이트
   df_new = pd.DataFrame({
       "id": [1, 2, 3, 4],
       "name": ["Alice", "Bob", "Charlie", "David"]
   })
   
   # append 모드로 스냅샷 추가
   atio.write_snapshot(df_new, "users_table", mode="append", format="parquet")

스냅샷 읽기
~~~~~~~~~~

.. code-block:: python

   # 최신 버전 읽기
   latest_data = atio.read_table("users_table")
   
   # 특정 버전 읽기
   version_1_data = atio.read_table("users_table", version=1)
   
   # Polars로 읽기
   polars_data = atio.read_table("users_table", output_as="polars")

스냅샷 정리
~~~~~~~~~~

.. code-block:: python

   from datetime import timedelta
   
   # 7일 이상 된 스냅샷 삭제 (dry run)
   atio.expire_snapshots("users_table", keep_for=timedelta(days=7), dry_run=True)
   
   # 실제 삭제 실행
   atio.expire_snapshots("users_table", keep_for=timedelta(days=7), dry_run=False)

데이터베이스 연동
----------------

Pandas와 Polars를 사용하여 데이터베이스에 안전하게 데이터를 저장할 수 있습니다.

Pandas SQL 연동
~~~~~~~~~~~~~~

.. code-block:: python

   import atio
   import pandas as pd
   from sqlalchemy import create_engine

   # 데이터베이스 연결
   engine = create_engine('postgresql://user:password@localhost/dbname')
   
   df = pd.DataFrame({
       "id": [1, 2, 3],
       "name": ["Alice", "Bob", "Charlie"]
   })
   
   # SQL 데이터베이스에 저장
   atio.write(df, format="sql", name="users", con=engine)

Polars 데이터베이스 연동
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import atio
   import polars as pl

   df = pl.DataFrame({
       "id": [1, 2, 3],
       "name": ["Alice", "Bob", "Charlie"]
   })
   
   # 데이터베이스에 저장
   atio.write(df, format="database", 
              table_name="users", 
              connection_uri="postgresql://user:password@localhost/dbname")

성능 최적화
-----------

진행도 표시
~~~~~~~~~~

대용량 파일 처리 시 진행 상황을 실시간으로 확인할 수 있습니다.

.. code-block:: python

   import atio
   import pandas as pd
   import numpy as np

   # 대용량 데이터 생성
   large_df = pd.DataFrame(np.random.randn(1000000, 10))
   
   # 진행도 표시와 함께 저장
   atio.write(large_df, "large_data.parquet", 
              format="parquet", 
              show_progress=True)

상세 로깅
~~~~~~~~~

성능 진단을 위한 상세한 로깅을 활성화할 수 있습니다.

.. code-block:: python

   # 상세한 성능 정보 출력
   atio.write(df, "data.parquet", format="parquet", verbose=True)

이를 통해 다음과 같은 정보를 확인할 수 있습니다:

- 각 단계별 소요 시간
- 임시 파일 생성 및 교체 과정
- 백업 및 롤백 과정
- 성능 병목점 분석

에러 처리
---------

Atio는 다양한 에러 상황에 대해 안전하게 처리합니다.

파일 시스템 에러
~~~~~~~~~~~~~~~

.. code-block:: python

   import atio
   import pandas as pd

   df = pd.DataFrame({"a": [1, 2, 3]})

   try:
       # 권한이 없는 디렉토리에 저장 시도
       atio.write(df, "/root/data.parquet", format="parquet")
   except PermissionError as e:
       print(f"권한 에러: {e}")
       # 원본 파일은 그대로 보존됨

포맷 에러
~~~~~~~~~

.. code-block:: python

   try:
       # 지원하지 않는 포맷 사용
       atio.write(df, "data.unknown", format="unknown")
   except ValueError as e:
       print(f"지원하지 않는 포맷: {e}")

데이터베이스 에러
~~~~~~~~~~~~~~~

.. code-block:: python

   try:
       # 잘못된 데이터베이스 연결 정보
       atio.write(df, format="sql", 
                  name="users", 
                  con="invalid_connection")
   except Exception as e:
       print(f"데이터베이스 에러: {e}")

플러그인 확장
------------

Atio는 플러그인 아키텍처를 통해 새로운 형식을 쉽게 추가할 수 있습니다.

커스텀 형식 등록
~~~~~~~~~~~~~~~

.. code-block:: python

   from atio.plugins import register_writer
   import pandas as pd

   # 커스텀 형식 등록
   def custom_writer(df, path, **kwargs):
       # 커스텀 저장 로직
       with open(path, 'w') as f:
           f.write("Custom format\n")
           f.write(df.to_string())

   # Pandas DataFrame에 대한 커스텀 형식 등록
   register_writer(pd.DataFrame, "custom", custom_writer)
   
   # 사용
   df = pd.DataFrame({"a": [1, 2, 3]})
   atio.write(df, "data.custom", format="custom")

NumPy 배열 처리
--------------

NumPy 배열의 다양한 저장 방식을 지원합니다.

단일 배열 저장
~~~~~~~~~~~~~

.. code-block:: python

   import atio
   import numpy as np

   arr = np.array([[1, 2, 3], [4, 5, 6]])
   
   # .npy 파일로 저장
   atio.write(arr, "array.npy", format="npy")
   
   # .csv 파일로 저장
   atio.write(arr, "array.csv", format="csv")

여러 배열 저장
~~~~~~~~~~~~~

.. code-block:: python

   # 여러 배열을 딕셔너리로 저장
   arrays = {
       "features": np.random.randn(1000, 10),
       "labels": np.random.randint(0, 2, 1000),
       "metadata": np.array([1, 2, 3, 4, 5])
   }
   
   # 압축된 .npz 파일로 저장
   atio.write(arrays, "data.npz", format="npz_compressed")

실제 사용 사례
-------------

머신러닝 파이프라인
~~~~~~~~~~~~~~~~~

.. code-block:: python

   import atio
   import pandas as pd
   from sklearn.model_selection import train_test_split
   from sklearn.ensemble import RandomForestClassifier

   # 데이터 로드 및 전처리
   df = pd.read_csv("raw_data.csv")
   
   # 전처리된 데이터를 안전하게 저장
   atio.write(df, "processed_data.parquet", format="parquet")
   
   # 학습/테스트 분할
   X_train, X_test, y_train, y_test = train_test_split(
       df.drop('target', axis=1), df['target'], test_size=0.2
   )
   
   # 분할된 데이터를 스냅샷으로 저장
   atio.write_snapshot(X_train, "training_data", format="parquet")
   atio.write_snapshot(X_test, "test_data", format="parquet")
   
   # 모델 학습
   model = RandomForestClassifier()
   model.fit(X_train, y_train)
   
   # 예측 결과를 안전하게 저장
   predictions = model.predict(X_test)
   results_df = pd.DataFrame({
       'actual': y_test,
       'predicted': predictions
   })
   
   atio.write(results_df, "predictions.parquet", format="parquet")

ETL 파이프라인
~~~~~~~~~~~~~

.. code-block:: python

   import atio
   import pandas as pd
   from sqlalchemy import create_engine

   # 1. 원본 데이터 로드
   raw_data = pd.read_csv("source_data.csv")
   
   # 2. 데이터 정제
   cleaned_data = raw_data.dropna()
   cleaned_data = cleaned_data[cleaned_data['value'] > 0]
   
   # 3. 안전하게 정제된 데이터 저장
   atio.write(cleaned_data, "cleaned_data.parquet", format="parquet")
   
   # 4. 데이터베이스에 저장
   engine = create_engine('postgresql://user:password@localhost/warehouse')
   atio.write(cleaned_data, format="sql", 
              name="processed_table", 
              con=engine, 
              if_exists='replace')
   
   # 5. 스냅샷으로 버전 관리
   atio.write_snapshot(cleaned_data, "daily_processed", format="parquet") 