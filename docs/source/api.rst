API 참조
========

Atio의 모든 함수와 클래스에 대한 상세한 API 문서입니다.

주요 함수
--------

.. automodule:: atio
   :members:
   :undoc-members:
   :show-inheritance:

Core 모듈
--------

.. automodule:: atio.core
   :members:
   :undoc-members:
   :show-inheritance:

write()
-------

안전한 원자적 파일 쓰기를 수행하는 메인 함수입니다.

.. function:: atio.write(obj, target_path=None, format='parquet', **kwargs)

   :param obj: 저장할 데이터 객체 (pandas.DataFrame, polars.DataFrame, numpy.ndarray)
   :param target_path: 저장할 파일 경로 (파일 저장 시 필수)
   :param format: 저장 형식 ('csv', 'parquet', 'excel', 'json', 'sql', 'database')
   :param show_progress: 진행률 표시 여부 (기본값: False)
   :param verbose: 상세 성능 정보 출력 여부 (기본값: False)
   :param **kwargs: 형식별 추가 매개변수

   :returns: None

   :raises: ValueError, IOError, DatabaseError

   **사용 예제:**

   .. code-block:: python

      import atio
      import pandas as pd

      df = pd.DataFrame({"a": [1, 2, 3]})
      
      # 기본 사용법
      atio.write(df, "data.parquet", format="parquet")
      
      # 진행률 표시
      atio.write(df, "data.parquet", format="parquet", show_progress=True)
      
      # 성능 정보 출력
      atio.write(df, "data.parquet", format="parquet", verbose=True)

write_snapshot()
---------------

데이터 스냅샷을 생성하여 버전 관리를 수행합니다.

.. function:: atio.write_snapshot(obj, table_name, format='parquet', **kwargs)

   :param obj: 저장할 데이터 객체
   :param table_name: 테이블 이름 (스냅샷 디렉토리명)
   :param format: 저장 형식
   :param **kwargs: 추가 매개변수

   :returns: 생성된 스냅샷 ID

   **사용 예제:**

   .. code-block:: python

      # 스냅샷 생성
      snapshot_id = atio.write_snapshot(df, "users", format="parquet")
      print(f"생성된 스냅샷 ID: {snapshot_id}")

read_table()
-----------

스냅샷에서 데이터를 읽어옵니다.

.. function:: atio.read_table(table_name, snapshot_id='latest', format='parquet', **kwargs)

   :param table_name: 테이블 이름
   :param snapshot_id: 스냅샷 ID (기본값: 'latest')
   :param format: 읽을 형식
   :param **kwargs: 추가 매개변수

   :returns: 데이터 객체

   **사용 예제:**

   .. code-block:: python

      # 최신 스냅샷 읽기
      df = atio.read_table("users", format="parquet")
      
      # 특정 스냅샷 읽기
      df = atio.read_table("users", snapshot_id="20240101_120000", format="parquet")

expire_snapshots()
-----------------

오래된 스냅샷을 정리합니다.

.. function:: atio.expire_snapshots(table_name, days=30, format='parquet')

   :param table_name: 테이블 이름
   :param days: 보관할 일수 (기본값: 30)
   :param format: 형식

   :returns: 삭제된 스냅샷 수

   **사용 예제:**

   .. code-block:: python

      # 30일 이상 된 스냅샷 정리
      deleted_count = atio.expire_snapshots("users", days=30)
      print(f"삭제된 스냅샷 수: {deleted_count}")

Plugins 모듈
-----------

.. automodule:: atio.plugins
   :members:
   :undoc-members:
   :show-inheritance:

Utils 모듈
---------

.. automodule:: atio.utils
   :members:
   :undoc-members:
   :show-inheritance:

지원하는 형식
-----------

CSV 형식
~~~~~~~

.. code-block:: python

   atio.write(df, "data.csv", format="csv", index=False, encoding='utf-8')

**지원 매개변수:**
- `index`: 인덱스 포함 여부
- `encoding`: 인코딩 방식
- `sep`: 구분자
- `na_rep`: NA 값 표현

Parquet 형식
~~~~~~~~~~~

.. code-block:: python

   atio.write(df, "data.parquet", format="parquet", compression='snappy')

**지원 매개변수:**
- `compression`: 압축 방식 ('snappy', 'gzip', 'brotli')
- `engine`: 엔진 ('pyarrow', 'fastparquet')

Excel 형식
~~~~~~~~~

.. code-block:: python

   atio.write(df, "data.xlsx", format="excel", sheet_name="Sheet1")

**지원 매개변수:**
- `sheet_name`: 시트 이름
- `engine`: 엔진 ('openpyxl', 'xlsxwriter')

JSON 형식
~~~~~~~~~

.. code-block:: python

   atio.write(df, "data.json", format="json", orient="records")

**지원 매개변수:**
- `orient`: 방향 ('records', 'split', 'index', 'columns', 'values', 'table')

SQL 형식
~~~~~~~~

.. code-block:: python

   atio.write(df, format="sql", name="table_name", con=engine, if_exists="replace")

**지원 매개변수:**
- `name`: 테이블 이름
- `con`: 데이터베이스 연결
- `if_exists`: 테이블 존재 시 동작 ('fail', 'replace', 'append')

예외 처리
--------

Atio는 다음과 같은 예외를 발생시킬 수 있습니다:

- **ValueError**: 잘못된 매개변수나 형식
- **IOError**: 파일 시스템 오류
- **DatabaseError**: 데이터베이스 연결 오류
- **ImportError**: 필요한 패키지가 설치되지 않은 경우

**예외 처리 예제:**

.. code-block:: python

   try:
       atio.write(df, "data.parquet", format="parquet")
   except ValueError as e:
       print(f"매개변수 오류: {e}")
   except IOError as e:
       print(f"파일 시스템 오류: {e}")
   except Exception as e:
       print(f"예상치 못한 오류: {e}") 