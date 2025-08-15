지원하는 파일 형식
==================

Atio는 다양한 데이터 형식을 지원합니다. 각 형식별로 지원하는 라이브러리와 사용법을 설명합니다.

Pandas DataFrame 형식
--------------------

Pandas DataFrame은 가장 많은 형식을 지원합니다.

CSV (Comma-Separated Values)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import atio
   import pandas as pd

   df = pd.DataFrame({
       "name": ["Alice", "Bob", "Charlie"],
       "age": [25, 30, 35],
       "city": ["Seoul", "Busan", "Incheon"]
   })

   # 기본 CSV 저장
   atio.write(df, "users.csv", format="csv")
   
   # 추가 옵션과 함께 저장
   atio.write(df, "users.csv", format="csv", 
              index=False, 
              encoding='utf-8', 
              sep=';')

**지원 옵션:**
- `index`: 인덱스 포함 여부 (기본값: True)
- `encoding`: 인코딩 방식 (기본값: 'utf-8')
- `sep`: 구분자 (기본값: ',')
- `header`: 헤더 포함 여부 (기본값: True)

Parquet
~~~~~~~

.. code-block:: python

   # Parquet 형식으로 저장
   atio.write(df, "users.parquet", format="parquet")
   
   # 압축 옵션과 함께 저장
   atio.write(df, "users.parquet", format="parquet", 
              compression='snappy')

**지원 옵션:**
- `compression`: 압축 방식 ('snappy', 'gzip', 'brotli', None)
- `index`: 인덱스 포함 여부 (기본값: True)

Excel
~~~~~

.. code-block:: python

   # Excel 파일로 저장
   atio.write(df, "users.xlsx", format="excel")
   
   # 시트명과 옵션 지정
   atio.write(df, "users.xlsx", format="excel", 
              sheet_name="Users", 
              index=False)

**지원 옵션:**
- `sheet_name`: 시트 이름 (기본값: 'Sheet1')
- `index`: 인덱스 포함 여부 (기본값: True)
- `engine`: 엔진 ('openpyxl', 'xlsxwriter')

**필요 라이브러리:** `pip install openpyxl`

JSON
~~~~

.. code-block:: python

   # JSON 형식으로 저장
   atio.write(df, "users.json", format="json")
   
   # 들여쓰기와 함께 저장
   atio.write(df, "users.json", format="json", 
              indent=2, 
              orient='records')

**지원 옵션:**
- `orient`: JSON 구조 ('split', 'records', 'index', 'columns', 'values', 'table')
- `indent`: 들여쓰기 크기
- `date_format`: 날짜 형식

Pickle
~~~~~~

.. code-block:: python

   # Pickle 형식으로 저장
   atio.write(df, "users.pkl", format="pickle")
   
   # 압축과 함께 저장
   atio.write(df, "users.pkl", format="pickle", 
              compression='gzip')

**지원 옵션:**
- `compression`: 압축 방식 ('gzip', 'bz2', 'xz', None)

HTML
~~~~

.. code-block:: python

   # HTML 테이블로 저장
   atio.write(df, "users.html", format="html")
   
   # 스타일과 함께 저장
   atio.write(df, "users.html", format="html", 
              index=False, 
              classes='table table-striped')

**지원 옵션:**
- `classes`: CSS 클래스
- `index`: 인덱스 포함 여부 (기본값: True)

SQL
~~~

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
   atio.write(df, format="sql", 
              name="users", 
              con=engine, 
              if_exists='replace')

**지원 옵션:**
- `name`: 테이블 이름 (필수)
- `con`: 데이터베이스 연결 객체 (필수)
- `if_exists`: 테이블이 존재할 때 동작 ('fail', 'replace', 'append')
- `index`: 인덱스를 컬럼으로 저장 여부 (기본값: True)

**필요 라이브러리:** `pip install sqlalchemy`

Polars DataFrame 형식
--------------------

Polars는 빠른 데이터 처리를 위한 현대적인 DataFrame 라이브러리입니다.

CSV
~~~

.. code-block:: python

   import atio
   import polars as pl

   df = pl.DataFrame({
       "name": ["Alice", "Bob", "Charlie"],
       "age": [25, 30, 35],
       "city": ["Seoul", "Busan", "Incheon"]
   })

   # CSV 저장
   atio.write(df, "users.csv", format="csv")
   
   # 구분자와 함께 저장
   atio.write(df, "users.csv", format="csv", 
              separator=';')

**지원 옵션:**
- `separator`: 구분자 (기본값: ',')
- `include_header`: 헤더 포함 여부 (기본값: True)

Parquet
~~~~~~~

.. code-block:: python

   # Parquet 저장
   atio.write(df, "users.parquet", format="parquet")
   
   # 압축과 함께 저장
   atio.write(df, "users.parquet", format="parquet", 
              compression="snappy")

**지원 옵션:**
- `compression`: 압축 방식 ('snappy', 'gzip', 'brotli', 'lz4raw', 'zstd', None)

JSON
~~~~

.. code-block:: python

   # JSON 저장
   atio.write(df, "users.json", format="json")
   
   # 파일별 저장
   atio.write(df, "users.json", format="json", 
              file=True)

**지원 옵션:**
- `file`: 파일별 저장 여부 (기본값: False)

IPC (Arrow)
~~~~~~~~~~~

.. code-block:: python

   # IPC (Arrow) 형식으로 저장
   atio.write(df, "users.arrow", format="ipc")
   
   # 압축과 함께 저장
   atio.write(df, "users.arrow", format="ipc", 
              compression="lz4")

**지원 옵션:**
- `compression`: 압축 방식 ('lz4', 'zstd', None)

Avro
~~~~

.. code-block:: python

   # Avro 형식으로 저장
   atio.write(df, "users.avro", format="avro")

**필요 라이브러리:** `pip install fastavro`

Excel
~~~~~

.. code-block:: python

   # Excel 저장
   atio.write(df, "users.xlsx", format="excel")

**필요 라이브러리:** `pip install xlsx2csv openpyxl`

Database
~~~~~~~~

.. code-block:: python

   # 데이터베이스에 저장
   atio.write(df, format="database", 
              table_name="users", 
              connection_uri="postgresql://user:password@localhost/dbname")

**지원 옵션:**
- `table_name`: 테이블 이름 (필수)
- `connection_uri`: 데이터베이스 연결 URI (필수)

**필요 라이브러리:** `pip install connectorx`

NumPy 배열 형식
--------------

NumPy 배열은 수치 데이터 처리에 최적화된 형식을 지원합니다.

NPY (NumPy Binary)
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import atio
   import numpy as np

   arr = np.array([[1, 2, 3], [4, 5, 6]])

   # .npy 파일로 저장
   atio.write(arr, "array.npy", format="npy")

**특징:**
- 단일 배열을 효율적으로 저장
- 메타데이터와 함께 저장
- 빠른 읽기/쓰기 속도

NPZ (NumPy Compressed)
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # 여러 배열을 딕셔너리로 저장
   arrays = {
       "features": np.random.randn(1000, 10),
       "labels": np.random.randint(0, 2, 1000),
       "metadata": np.array([1, 2, 3, 4, 5])
   }
   
   # 압축되지 않은 .npz 파일로 저장
   atio.write(arrays, "data.npz", format="npz")
   
   # 압축된 .npz 파일로 저장
   atio.write(arrays, "data.npz", format="npz_compressed")

**특징:**
- 여러 배열을 하나의 파일에 저장
- 압축 옵션으로 저장 공간 절약
- 딕셔너리 형태로 데이터 구조화

CSV
~~~

.. code-block:: python

   # CSV로 저장
   atio.write(arr, "array.csv", format="csv")
   
   # 구분자와 함께 저장
   atio.write(arr, "array.csv", format="csv", 
              delimiter=';', 
              fmt='%.2f')

**지원 옵션:**
- `delimiter`: 구분자 (기본값: ',')
- `fmt`: 숫자 형식 (예: '%.2f', '%.4e')
- `header`: 헤더 포함 여부
- `comments`: 주석 문자

형식별 성능 비교
----------------

다양한 형식의 성능 특성을 비교해보겠습니다.

속도 비교
~~~~~~~~~

1. **가장 빠른 형식:**
   - NumPy: `.npy`, `.npz`
   - Polars: `.ipc` (Arrow)
   - Pandas: `.parquet` (snappy 압축)

2. **중간 속도:**
   - CSV (단순한 구조)
   - JSON (중간 복잡도)

3. **상대적으로 느린 형식:**
   - Excel (복잡한 구조)
   - Pickle (Python 특화)

용량 비교
~~~~~~~~~

1. **가장 작은 용량:**
   - `.parquet` (컬럼 기반 압축)
   - `.npz_compressed` (압축된 NumPy)
   - `.ipc` (Arrow 압축)

2. **중간 용량:**
   - `.npy` (단일 배열)
   - JSON (텍스트 기반)

3. **상대적으로 큰 용량:**
   - CSV (텍스트 기반)
   - Excel (복잡한 구조)

호환성 비교
~~~~~~~~~~~

1. **최고 호환성:**
   - CSV (모든 시스템에서 지원)
   - JSON (웹 표준)

2. **좋은 호환성:**
   - Excel (비즈니스 환경)
   - Parquet (빅데이터 생태계)

3. **제한적 호환성:**
   - `.npy`/`.npz` (Python/NumPy 특화)
   - `.ipc` (Arrow 생태계)

권장 사용 사례
-------------

데이터 분석 및 머신러닝
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # 학습 데이터: 빠른 읽기/쓰기를 위해 Parquet 사용
   atio.write(training_data, "train.parquet", format="parquet")
   
   # 모델 가중치: NumPy 배열로 저장
   atio.write(model_weights, "weights.npy", format="npy")
   
   # 실험 결과: JSON으로 저장 (가독성)
   atio.write(results, "experiment_results.json", format="json")

웹 애플리케이션
~~~~~~~~~~~~~~

.. code-block:: python

   # API 응답: JSON 형식
   atio.write(api_data, "response.json", format="json")
   
   # 대용량 데이터: Parquet 형식
   atio.write(large_dataset, "dataset.parquet", format="parquet")

데이터 파이프라인
~~~~~~~~~~~~~~~~

.. code-block:: python

   # 중간 결과: 빠른 처리를 위해 IPC 사용
   atio.write(intermediate_data, "step1.arrow", format="ipc")
   
   # 최종 결과: 호환성을 위해 CSV 사용
   atio.write(final_data, "output.csv", format="csv")

비즈니스 보고서
~~~~~~~~~~~~~~

.. code-block:: python

   # 엑셀 보고서
   atio.write(report_data, "monthly_report.xlsx", format="excel")
   
   # HTML 대시보드
   atio.write(dashboard_data, "dashboard.html", format="html")

형식 확장하기
------------

새로운 형식을 추가하려면 플러그인 시스템을 사용하세요.

.. code-block:: python

   from atio.plugins import register_writer
   import pandas as pd

   # 커스텀 형식 등록
   def yaml_writer(df, path, **kwargs):
       import yaml
       data = df.to_dict('records')
       with open(path, 'w') as f:
           yaml.dump(data, f, **kwargs)

   # 등록
   register_writer(pd.DataFrame, "yaml", yaml_writer)
   
   # 사용
   atio.write(df, "data.yaml", format="yaml") 