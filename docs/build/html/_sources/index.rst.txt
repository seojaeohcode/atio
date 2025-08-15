Atio Documentation
==================

**Atio** 🛡️는 안전하고 원자적인 파일 쓰기를 지원하는 경량 Python 라이브러리입니다.

Pandas, Polars, NumPy 등 데이터 객체 저장 시 **파일 손상 없이**, **트랜잭션처럼 안전하게 처리**할 수 있습니다.

주요 기능
---------

🔒 **Atomic File Writing**
   Safe writing using temporary files

📊 **Multiple Format Support**
   CSV, Parquet, Excel, JSON, etc.

🗄️ **Database Support**
   Direct SQL and Database writing

📈 **Progress Display**
   Progress monitoring for large data processing

🔄 **Rollback Function**
   Automatic recovery when errors occur

🎯 **Simple API**
   Intuitive and easy-to-use interface

📋 **Version Management**
   Snapshot-based data version management

🧹 **Auto Cleanup**
   Automatic deletion of old data

빠른 시작
---------

.. code-block:: python

   import atio
   import pandas as pd

   # 간단한 DataFrame 생성
   df = pd.DataFrame({
       "name": ["Alice", "Bob", "Charlie"],
       "age": [25, 30, 35],
       "city": ["Seoul", "Busan", "Incheon"]
   })
   
   # 안전한 파일 쓰기
   atio.write(df, "users.parquet", format="parquet")
   
   # 진행도 표시와 함께 저장
   atio.write(df, "users.csv", format="csv", show_progress=True)

지원하는 형식
------------

.. list-table:: 지원하는 파일 형식
   :widths: 20 20 20 20
   :header-rows: 1

   * - 형식
     - Pandas
     - Polars
     - NumPy
   * - CSV
     - ✅
     - ✅
     - ✅
   * - Parquet
     - ✅
     - ✅
     - ❌
   * - Excel
     - ✅
     - ✅
     - ❌
   * - JSON
     - ✅
     - ✅
     - ❌
   * - Pickle
     - ✅
     - ❌
     - ❌
   * - HTML
     - ✅
     - ❌
     - ❌
   * - SQL
     - ✅
     - ❌
     - ❌
   * - Database
     - ❌
     - ✅
     - ❌
   * - NPY/NPZ
     - ❌
     - ❌
     - ✅

사용 사례
--------

🔹 **데이터 파이프라인**
   ETL 과정에서 중간 데이터 안전하게 저장

🔹 **실험 데이터 관리**
   머신러닝 실험 결과의 버전 관리

🔹 **대용량 데이터 처리**
   대용량 파일의 안전한 저장 및 진행도 모니터링

🔹 **데이터베이스 연동**
   Pandas/Polars 데이터를 SQL/NoSQL DB에 안전하게 저장

목차
----

.. toctree::
   :maxdepth: 2
   :caption: 사용자 가이드:

   installation
   quickstart
   examples
   advanced_usage

.. toctree::
   :maxdepth: 2
   :caption: API 참조:

   api
   configuration

API Reference
=============

핵심 함수들
-----------

.. automodule:: atio
   :members:
   :undoc-members:

.. automodule:: atio.core
   :members:
   :undoc-members:

.. automodule:: atio.plugins
   :members:
   :undoc-members:

.. automodule:: atio.utils
   :members:
   :undoc-members:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
