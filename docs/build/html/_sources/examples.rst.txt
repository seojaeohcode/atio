사용 예제
=========

Atio의 다양한 사용 사례와 고급 기능을 살펴보세요.

기본 예제
--------

간단한 데이터 저장
~~~~~~~~~~~~~~~~~

.. code-block:: python

   import atio
   import pandas as pd

   # 샘플 데이터 생성
   data = {
       "id": [1, 2, 3, 4, 5],
       "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
       "age": [25, 30, 35, 28, 32],
       "city": ["Seoul", "Busan", "Incheon", "Daegu", "Daejeon"],
       "salary": [50000, 60000, 70000, 55000, 65000]
   }
   
   df = pd.DataFrame(data)
   
   # 다양한 형식으로 저장
   atio.write(df, "employees.parquet", format="parquet")
   atio.write(df, "employees.csv", format="csv", index=False)
   atio.write(df, "employees.xlsx", format="excel", sheet_name="Employees")

대용량 데이터 처리
----------------

진행률 표시와 성능 모니터링
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import numpy as np
   import pandas as pd
   import atio

   # 대용량 데이터 생성 (100만 행)
   large_data = {
       "id": range(1, 1000001),
       "value": np.random.randn(1000000),
       "category": np.random.choice(["A", "B", "C"], 1000000),
       "timestamp": pd.date_range("2024-01-01", periods=1000000, freq="S")
   }
   
   large_df = pd.DataFrame(large_data)
   
   # 진행률 표시와 성능 모니터링 활성화
   atio.write(
       large_df, 
       "large_dataset.parquet", 
       format="parquet",
       show_progress=True,
       verbose=True,
       compression='snappy'
   )

데이터베이스 연동
---------------

PostgreSQL 데이터베이스 저장
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import atio
   import pandas as pd
   from sqlalchemy import create_engine

   # 데이터베이스 연결
   engine = create_engine('postgresql://username:password@localhost:5432/mydb')
   
   # 샘플 데이터
   sales_data = {
       "order_id": [1001, 1002, 1003, 1004, 1005],
       "product_name": ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"],
       "quantity": [1, 2, 1, 1, 3],
       "price": [1200, 25, 75, 300, 150],
       "order_date": pd.date_range("2024-01-01", periods=5)
   }
   
   sales_df = pd.DataFrame(sales_data)
   
   # 데이터베이스에 안전하게 저장
   atio.write(
       sales_df,
       format="sql",
       name="sales_orders",
       con=engine,
       if_exists="replace",
       index=False
   )

스냅샷 기반 버전 관리
-------------------

데이터 버전 관리
~~~~~~~~~~~~~~~

.. code-block:: python

   import atio
   import pandas as pd
   from datetime import datetime, timedelta

   # 초기 데이터
   initial_data = {
       "user_id": [1, 2, 3],
       "name": ["Alice", "Bob", "Charlie"],
       "status": ["active", "active", "inactive"]
   }
   
   df = pd.DataFrame(initial_data)
   
   # 초기 스냅샷 생성
   snapshot_id_1 = atio.write_snapshot(df, "users", format="parquet")
   print(f"초기 스냅샷 생성: {snapshot_id_1}")
   
   # 데이터 업데이트
   df.loc[df['user_id'] == 3, 'status'] = 'active'
   df = df.append({"user_id": 4, "name": "Diana", "status": "active"}, ignore_index=True)
   
   # 업데이트된 스냅샷 생성
   snapshot_id_2 = atio.write_snapshot(df, "users", format="parquet")
   print(f"업데이트 스냅샷 생성: {snapshot_id_2}")
   
   # 최신 데이터 읽기
   latest_df = atio.read_table("users", format="parquet")
   print("최신 데이터:")
   print(latest_df)
   
   # 특정 스냅샷 읽기
   initial_df = atio.read_table("users", snapshot_id=snapshot_id_1, format="parquet")
   print("초기 데이터:")
   print(initial_df)
   
   # 오래된 스냅샷 정리 (7일 이상)
   deleted_count = atio.expire_snapshots("users", days=7, format="parquet")
   print(f"삭제된 스냅샷 수: {deleted_count}")

Polars DataFrame 활용
-------------------

고성능 데이터 처리
~~~~~~~~~~~~~~~~~

.. code-block:: python

   import atio
   import polars as pl
   import numpy as np

   # Polars DataFrame 생성
   polars_data = {
       "id": range(1, 10001),
       "value": np.random.randn(10000),
       "category": np.random.choice(["A", "B", "C", "D"], 10000),
       "score": np.random.uniform(0, 100, 10000)
   }
   
   polars_df = pl.DataFrame(polars_data)
   
   # Polars DataFrame 저장
   atio.write(
       polars_df,
       "polars_data.parquet",
       format="parquet",
       compression='snappy',
       show_progress=True
   )
   
   # 데이터 변환 후 저장
   filtered_df = polars_df.filter(pl.col("score") > 50)
   aggregated_df = filtered_df.group_by("category").agg(
       pl.col("value").mean().alias("avg_value"),
       pl.col("score").mean().alias("avg_score")
   )
   
   atio.write(aggregated_df, "aggregated_data.parquet", format="parquet")

에러 처리 및 복구
---------------

안전한 데이터 처리
~~~~~~~~~~~~~~~~~

.. code-block:: python

   import atio
   import pandas as pd
   import os

   def safe_data_processing():
       try:
           # 원본 파일이 있는지 확인
           if os.path.exists("important_data.parquet"):
               print("원본 파일이 존재합니다.")
           
           # 데이터 처리 및 저장
           df = pd.DataFrame({
               "id": [1, 2, 3],
               "data": ["important", "data", "here"]
           })
           
           atio.write(df, "important_data.parquet", format="parquet")
           print("데이터가 안전하게 저장되었습니다.")
           
           # SUCCESS 파일 확인
           if os.path.exists("important_data.parquet_SUCCESS"):
               print("저장 완료 플래그가 생성되었습니다.")
           
       except Exception as e:
           print(f"오류 발생: {e}")
           print("원본 파일이 보존되었습니다.")
           
           # 임시 파일 정리
           temp_files = [f for f in os.listdir(".") if f.startswith("important_data.parquet.tmp")]
           for temp_file in temp_files:
               try:
                   os.remove(temp_file)
                   print(f"임시 파일 정리: {temp_file}")
               except:
                   pass

   # 안전한 데이터 처리 실행
   safe_data_processing()

배치 처리
-------

여러 파일 동시 처리
~~~~~~~~~~~~~~~~~

.. code-block:: python

   import atio
   import pandas as pd
   import os
   from pathlib import Path

   def process_multiple_files():
       # 처리할 파일 목록
       files_to_process = [
           {"name": "users", "data": pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})},
           {"name": "products", "data": pd.DataFrame({"id": [1, 2], "product": ["Laptop", "Mouse"]})},
           {"name": "orders", "data": pd.DataFrame({"id": [1, 2], "amount": [100, 200]})}
       ]
       
       # 각 파일을 안전하게 처리
       for file_info in files_to_process:
           try:
               file_path = f"{file_info['name']}.parquet"
               atio.write(
                   file_info['data'],
                   file_path,
                   format="parquet",
                   show_progress=True
               )
               print(f"{file_info['name']} 파일 처리 완료")
               
           except Exception as e:
               print(f"{file_info['name']} 파일 처리 실패: {e}")
               continue

   # 배치 처리 실행
   process_multiple_files()

성능 최적화
---------

압축 및 최적화 설정
~~~~~~~~~~~~~~~~~

.. code-block:: python

   import atio
   import pandas as pd
   import numpy as np

   # 대용량 데이터 생성
   large_df = pd.DataFrame({
       "id": range(1, 100001),
       "value": np.random.randn(100000),
       "text": ["sample text"] * 100000
   })
   
   # 다양한 압축 설정으로 성능 비교
   compression_settings = [
       ("snappy", "fast_compression.parquet"),
       ("gzip", "balanced_compression.parquet"),
       ("brotli", "high_compression.parquet")
   ]
   
   for compression, filename in compression_settings:
       print(f"\n{compression} 압축으로 저장 중...")
       atio.write(
           large_df,
           filename,
           format="parquet",
           compression=compression,
           show_progress=True,
           verbose=True
       )
       
       # 파일 크기 확인
       file_size = os.path.getsize(filename) / (1024 * 1024)  # MB
       print(f"파일 크기: {file_size:.2f} MB")

실제 사용 사례
------------

데이터 파이프라인
~~~~~~~~~~~~~~~

.. code-block:: python

   import atio
   import pandas as pd
   from datetime import datetime, timedelta

   class DataPipeline:
       def __init__(self, base_path="data"):
           self.base_path = Path(base_path)
           self.base_path.mkdir(exist_ok=True)
       
       def extract_data(self):
           """데이터 추출 (시뮬레이션)"""
           # 실제로는 API나 데이터베이스에서 데이터를 가져옴
           data = {
               "timestamp": pd.date_range(datetime.now(), periods=1000, freq="H"),
               "value": np.random.randn(1000),
               "source": ["api"] * 1000
           }
           return pd.DataFrame(data)
       
       def transform_data(self, df):
           """데이터 변환"""
           # 시간별 집계
           df['hour'] = df['timestamp'].dt.hour
           df['day'] = df['timestamp'].dt.date
           
           # 일별 통계
           daily_stats = df.groupby('day').agg({
               'value': ['mean', 'std', 'min', 'max']
           }).round(2)
           
           return daily_stats
       
       def load_data(self, df, table_name):
           """데이터 로드"""
           # 스냅샷 생성
           snapshot_id = atio.write_snapshot(
               df,
               table_name,
               format="parquet"
           )
           
           # 최신 데이터도 별도 저장
           latest_path = self.base_path / f"{table_name}_latest.parquet"
           atio.write(df, str(latest_path), format="parquet")
           
           return snapshot_id
       
       def run_pipeline(self):
           """파이프라인 실행"""
           print("데이터 파이프라인 시작...")
           
           # 1. 데이터 추출
           raw_data = self.extract_data()
           print(f"추출된 데이터: {len(raw_data)} 행")
           
           # 2. 데이터 변환
           processed_data = self.transform_data(raw_data)
           print(f"처리된 데이터: {len(processed_data)} 행")
           
           # 3. 데이터 로드
           snapshot_id = self.load_data(processed_data, "daily_stats")
           print(f"스냅샷 생성 완료: {snapshot_id}")
           
           # 4. 오래된 스냅샷 정리
           deleted_count = atio.expire_snapshots("daily_stats", days=30)
           print(f"정리된 스냅샷: {deleted_count}개")
           
           print("파이프라인 완료!")

   # 파이프라인 실행
   pipeline = DataPipeline()
   pipeline.run_pipeline() 