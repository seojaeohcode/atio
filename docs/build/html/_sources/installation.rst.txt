설치 가이드
==========

Atio를 설치하는 방법을 안내합니다.

PyPI를 통한 설치
----------------

가장 간단한 설치 방법은 PyPI를 사용하는 것입니다:

.. code-block:: bash

   pip install atio

특정 버전 설치
~~~~~~~~~~~~~

특정 버전을 설치하려면:

.. code-block:: bash

   pip install atio==2.0.0

개발 버전 설치
~~~~~~~~~~~~~

최신 개발 버전을 설치하려면:

.. code-block:: bash

   pip install git+https://github.com/seojaeohcode/atio.git

의존성
------

필수 의존성
~~~~~~~~~~

- Python 3.8+
- pandas
- numpy

선택적 의존성
~~~~~~~~~~~~

특정 형식을 사용하려면 추가 라이브러리가 필요합니다:

**Parquet 형식:**
.. code-block:: bash

   pip install pyarrow
   # 또는
   pip install fastparquet

**Excel 형식:**
.. code-block:: bash

   pip install openpyxl
   # 또는
   pip install xlsxwriter

**SQL 데이터베이스:**
.. code-block:: bash

   pip install sqlalchemy

**Polars 지원:**
.. code-block:: bash

   pip install polars

**Avro 형식 (Polars):**
.. code-block:: bash

   pip install fastavro

**Polars Excel 지원:**
.. code-block:: bash

   pip install xlsx2csv openpyxl

**Polars 데이터베이스 지원:**
.. code-block:: bash

   pip install connectorx

모든 의존성 설치
~~~~~~~~~~~~~~~

모든 기능을 사용하려면:

.. code-block:: bash

   pip install atio[all]

또는 개별적으로:

.. code-block:: bash

   pip install atio
   pip install pyarrow openpyxl sqlalchemy polars fastavro xlsx2csv connectorx

환경 확인
---------

설치가 완료되었는지 확인하려면:

.. code-block:: python

   import atio
   print(f"Atio 버전: {atio.__version__}")

   # 기본 기능 테스트
   import pandas as pd
   df = pd.DataFrame({"test": [1, 2, 3]})
   atio.write(df, "test.parquet", format="parquet")
   print("설치가 성공적으로 완료되었습니다!")

가상환경 사용 권장
-----------------

프로젝트별로 독립적인 환경을 유지하기 위해 가상환경 사용을 권장합니다:

.. code-block:: bash

   # 가상환경 생성
   python -m venv atio_env
   
   # 가상환경 활성화 (Windows)
   atio_env\Scripts\activate
   
   # 가상환경 활성화 (macOS/Linux)
   source atio_env/bin/activate
   
   # Atio 설치
   pip install atio

Conda 사용
---------

Conda를 사용하는 경우:

.. code-block:: bash

   # Conda 환경 생성
   conda create -n atio_env python=3.8
   conda activate atio_env
   
   # Atio 설치
   pip install atio

문제 해결
---------

설치 중 문제가 발생하는 경우:

**권한 오류:**
.. code-block:: bash

   pip install atio --user

**캐시 문제:**
.. code-block:: bash

   pip install atio --no-cache-dir

**의존성 충돌:**
.. code-block:: bash

   pip install atio --force-reinstall

**특정 Python 버전:**
.. code-block:: bash

   python3.8 -m pip install atio

업그레이드
----------

최신 버전으로 업그레이드:

.. code-block:: bash

   pip install --upgrade atio

특정 버전으로 다운그레이드:

.. code-block:: bash

   pip install atio==1.0.0

제거
----

Atio를 제거하려면:

.. code-block:: bash

   pip uninstall atio 