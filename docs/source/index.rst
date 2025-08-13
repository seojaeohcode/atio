Atio Documentation
==================

**Atio** 🛡️는 안전하고 원자적인 파일 쓰기를 지원하는 경량 Python 라이브러리입니다.

Pandas, Polars, NumPy 등 데이터 객체 저장 시 **파일 손상 없이**, **트랜잭션처럼 안전하게 처리**할 수 있습니다.

주요 기능
---------

- ✅ 임시 디렉토리 스테이징 후 **원자적 파일 교체**
- 📦 Pandas, Polars, NumPy 등 다양한 데이터 객체 지원
- 📍 `_SUCCESS` 플래그 파일 생성 — 저장 완료 여부 표시
- 🛠 실패 시 **원본 파일 보존**, 임시 파일 자동 정리
- 🧩 플러그인 아키텍처로 **확장성 좋음**
- 🔍 **성능 진단 로깅** — 각 단계별 실행 시간 측정 및 병목점 분석

빠른 시작
---------

.. code-block:: python

   import atio
   import pandas as pd

   df = pd.DataFrame({"a": [1, 2, 3]})
   
   # 안전한 파일 쓰기
   atio.write(df, target_path="data.parquet", format="parquet")

목차
----

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   quickstart
   api
   examples

API Reference
=============

.. automodule:: atio
   :members:

.. automodule:: atio.core
   :members:

.. automodule:: atio.plugins
   :members:

.. automodule:: atio.utils
   :members:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
