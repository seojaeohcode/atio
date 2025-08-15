설정 및 구성
============

Atio의 다양한 설정 옵션과 구성 방법을 설명합니다.

로깅 설정
--------

Atio는 상세한 로깅을 통해 작업 과정을 추적할 수 있습니다.

기본 로깅
~~~~~~~~~

.. code-block:: python

   import atio
   import pandas as pd

   df = pd.DataFrame({"a": [1, 2, 3]})

   # 기본 로깅 (INFO 레벨)
   atio.write(df, "data.parquet", format="parquet")

**출력 예시:**
```
[INFO] 임시 디렉토리 생성: /tmp/tmp12345
[INFO] 임시 파일 경로: /tmp/tmp12345/data.parquet
[INFO] 사용할 writer: to_parquet (format: parquet)
[INFO] 데이터 임시 파일에 저장 완료: /tmp/tmp12345/data.parquet
[INFO] 원자적 교체 완료: /tmp/tmp12345/data.parquet -> data.parquet
[INFO] _SUCCESS 플래그 파일 생성: .data.parquet._SUCCESS
[INFO] ✅ Atomic write completed successfully (took 0.1234s)
```

상세 로깅
~~~~~~~~~

.. code-block:: python

   # 상세한 성능 정보 출력 (DEBUG 레벨)
   atio.write(df, "data.parquet", format="parquet", verbose=True)

**출력 예시:**
```
[INFO] 임시 디렉토리 생성: /tmp/tmp12345
[INFO] 임시 파일 경로: /tmp/tmp12345/data.parquet
[INFO] 사용할 writer: to_parquet (format: parquet)
[INFO] 데이터 임시 파일에 저장 완료: /tmp/tmp12345/data.parquet
[INFO] 원자적 교체 완료: /tmp/tmp12345/data.parquet -> data.parquet
[INFO] _SUCCESS 플래그 파일 생성: .data.parquet._SUCCESS
[DEBUG] Atomic write step timings (SUCCESS): setup=0.0012s, write_call=0.0987s, replace=0.0001s, success_flag=0.0001s, total=0.1001s
[INFO] ✅ Atomic write completed successfully (took 0.1001s)
```

진행도 표시
----------

대용량 파일 처리 시 진행 상황을 실시간으로 확인할 수 있습니다.

기본 진행도 표시
~~~~~~~~~~~~~~~

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

**출력 예시:**
```
⠋ Writing large_data.parquet... [ 45.2 MB | 12.3 MB/s | 00:03 ]
⠙ Writing large_data.parquet... [ 67.8 MB | 11.9 MB/s | 00:05 ]
⠹ Writing large_data.parquet... [ 89.1 MB | 12.1 MB/s | 00:07 ]
✅ Writing completed successfully (89.1 MB in 7s)
```

진행도 표시 옵션
~~~~~~~~~~~~~~~

진행도 표시는 다음과 같은 정보를 제공합니다:

- **스피너**: 작업 진행 상태를 시각적으로 표시
- **파일 크기**: 현재까지 저장된 데이터 크기
- **처리 속도**: 초당 처리되는 데이터 양
- **경과 시간**: 작업 시작 후 경과한 시간

성능 최적화 설정
----------------

메모리 사용량 최적화
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # 대용량 데이터 처리 시 메모리 효율적인 설정
   atio.write(large_df, "data.parquet", format="parquet", 
              compression='snappy',  # 빠른 압축
              index=False)          # 인덱스 제외로 메모리 절약

압축 설정
~~~~~~~~~

.. code-block:: python

   # 속도 우선 (압축 없음)
   atio.write(df, "data.parquet", format="parquet", compression=None)
   
   # 균형 (snappy 압축)
   atio.write(df, "data.parquet", format="parquet", compression='snappy')
   
   # 용량 우선 (gzip 압축)
   atio.write(df, "data.parquet", format="parquet", compression='gzip')

임시 디렉토리 설정
-----------------

기본적으로 Atio는 시스템의 임시 디렉토리를 사용합니다.

사용자 정의 임시 디렉토리
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import os
   import tempfile

   # 임시 디렉토리 설정
   tempfile.tempdir = "/path/to/custom/temp"
   
   # 또는 환경 변수 설정
   os.environ['TMPDIR'] = "/path/to/custom/temp"

**주의사항:**
- 임시 디렉토리는 충분한 디스크 공간이 있어야 합니다
- 쓰기 권한이 있어야 합니다
- 빠른 I/O 성능을 위해 SSD를 권장합니다

에러 처리 설정
--------------

Atio는 다양한 에러 상황에 대해 안전하게 처리합니다.

롤백 동작
~~~~~~~~~

.. code-block:: python

   # 기본적으로 롤백이 자동으로 수행됩니다
   try:
       atio.write(df, "data.parquet", format="parquet")
   except Exception as e:
       # 에러 발생 시 원본 파일은 보존됩니다
       print(f"저장 실패: {e}")
       # 임시 파일은 자동으로 정리됩니다

백업 파일 관리
~~~~~~~~~~~~~

.. code-block:: python

   # 백업 파일은 작업 성공 시 자동으로 삭제됩니다
   # 실패 시에는 롤백 후 삭제됩니다
   
   # 백업 파일이 남아있는 경우 수동으로 확인
   import os
   backup_file = "data.parquet._backup"
   if os.path.exists(backup_file):
       print("백업 파일이 존재합니다. 수동 확인이 필요할 수 있습니다.")

완료 플래그 시스템
------------------

Atio는 작업 완료를 확인할 수 있는 플래그 파일을 생성합니다.

플래그 파일 확인
~~~~~~~~~~~~~~~

.. code-block:: python

   import os

   # 저장 완료 후 플래그 파일 확인
   atio.write(df, "data.parquet", format="parquet")
   
   # 플래그 파일 경로
   flag_file = ".data.parquet._SUCCESS"
   
   if os.path.exists(flag_file):
       print("저장이 성공적으로 완료되었습니다.")
   else:
       print("저장이 완료되지 않았거나 실패했습니다.")

플래그 파일 활용
~~~~~~~~~~~~~~~

.. code-block:: python

   # 배치 처리에서 완료 여부 확인
   files_to_process = ["data1.parquet", "data2.parquet", "data3.parquet"]
   
   for file in files_to_process:
       flag_file = f".{file}._SUCCESS"
       if not os.path.exists(flag_file):
           print(f"{file} 처리가 완료되지 않았습니다.")
           # 재처리 로직

스냅샷 설정
----------

스냅샷 시스템의 다양한 설정 옵션을 설명합니다.

스냅샷 모드
~~~~~~~~~~

.. code-block:: python

   # overwrite 모드 (기본값)
   atio.write_snapshot(df, "table_path", mode="overwrite", format="parquet")
   
   # append 모드 (기존 데이터에 추가)
   atio.write_snapshot(df, "table_path", mode="append", format="parquet")

스냅샷 정리 설정
~~~~~~~~~~~~~~~

.. code-block:: python

   from datetime import timedelta

   # 7일 이상 된 스냅샷 삭제
   atio.expire_snapshots("table_path", 
                        keep_for=timedelta(days=7), 
                        dry_run=True)  # 실제 삭제 전 확인
   
   # 30일 이상 된 스냅샷 삭제
   atio.expire_snapshots("table_path", 
                        keep_for=timedelta(days=30), 
                        dry_run=False)  # 실제 삭제

환경 변수 설정
-------------

Atio의 동작을 제어하는 환경 변수들을 설정할 수 있습니다.

로깅 레벨 설정
~~~~~~~~~~~~~

.. code-block:: python

   import os

   # DEBUG 레벨로 로깅 설정
   os.environ['ATIO_LOG_LEVEL'] = 'DEBUG'
   
   # INFO 레벨로 로깅 설정 (기본값)
   os.environ['ATIO_LOG_LEVEL'] = 'INFO'

임시 디렉토리 설정
~~~~~~~~~~~~~~~~~

.. code-block:: python

   # 임시 디렉토리 경로 설정
   os.environ['ATIO_TEMP_DIR'] = '/path/to/temp'
   
   # 또는 시스템 임시 디렉토리 설정
   os.environ['TMPDIR'] = '/path/to/temp'

성능 모니터링 설정
~~~~~~~~~~~~~~~~~

.. code-block:: python

   # 성능 모니터링 활성화
   os.environ['ATIO_PERFORMANCE_MONITORING'] = 'true'
   
   # 성능 모니터링 비활성화
   os.environ['ATIO_PERFORMANCE_MONITORING'] = 'false'

플러그인 설정
------------

커스텀 플러그인을 등록하여 새로운 형식을 지원할 수 있습니다.

플러그인 등록
~~~~~~~~~~~~

.. code-block:: python

   from atio.plugins import register_writer
   import pandas as pd

   # 커스텀 형식 등록
   def custom_writer(df, path, **kwargs):
       # 커스텀 저장 로직
       with open(path, 'w') as f:
           f.write("Custom format\n")
           f.write(df.to_string())

   # 등록
   register_writer(pd.DataFrame, "custom", custom_writer)

플러그인 확인
~~~~~~~~~~~~

.. code-block:: python

   from atio.plugins import WRITER_MAPPING

   # 등록된 플러그인 확인
   for obj_type, formats in WRITER_MAPPING.items():
       print(f"Object type: {obj_type.__name__}")
       for fmt, handler in formats.items():
           print(f"  - {fmt}: {handler}")

설정 파일 사용
-------------

설정을 파일로 관리하여 일관된 설정을 유지할 수 있습니다.

JSON 설정 파일
~~~~~~~~~~~~~~

.. code-block:: python

   import json

   # 설정 파일 생성
   config = {
       "default_format": "parquet",
       "compression": "snappy",
       "show_progress": True,
       "verbose": False,
       "temp_dir": "/path/to/temp"
   }

   with open("atio_config.json", "w") as f:
       json.dump(config, f, indent=2)

   # 설정 파일 읽기
   with open("atio_config.json", "r") as f:
       config = json.load(f)

   # 설정 적용
   atio.write(df, "data.parquet", 
              format=config.get("default_format", "parquet"),
              compression=config.get("compression", "snappy"),
              show_progress=config.get("show_progress", False),
              verbose=config.get("verbose", False))

YAML 설정 파일
~~~~~~~~~~~~~

.. code-block:: python

   import yaml

   # 설정 파일 생성
   config = {
       "default_format": "parquet",
       "compression": "snappy",
       "show_progress": True,
       "verbose": False,
       "temp_dir": "/path/to/temp"
   }

   with open("atio_config.yaml", "w") as f:
       yaml.dump(config, f)

   # 설정 파일 읽기
   with open("atio_config.yaml", "r") as f:
       config = yaml.safe_load(f)

모범 사례
--------

프로덕션 환경 설정
~~~~~~~~~~~~~~~~~

.. code-block:: python

   # 프로덕션 환경을 위한 설정
   import os
   import tempfile

   # 1. 전용 임시 디렉토리 설정
   tempfile.tempdir = "/var/tmp/atio"
   os.makedirs(tempfile.tempdir, exist_ok=True)

   # 2. 로깅 레벨 설정
   os.environ['ATIO_LOG_LEVEL'] = 'INFO'

   # 3. 성능 최적화 설정
   def safe_write(df, path, **kwargs):
       return atio.write(df, path,
                        format="parquet",
                        compression="snappy",
                        show_progress=True,
                        verbose=False,
                        **kwargs)

개발 환경 설정
~~~~~~~~~~~~~

.. code-block:: python

   # 개발 환경을 위한 설정
   import os

   # 1. 상세 로깅 활성화
   os.environ['ATIO_LOG_LEVEL'] = 'DEBUG'

   # 2. 성능 모니터링 활성화
   os.environ['ATIO_PERFORMANCE_MONITORING'] = 'true'

   # 3. 개발용 설정
   def dev_write(df, path, **kwargs):
       return atio.write(df, path,
                        format="parquet",
                        compression=None,  # 압축 없음으로 빠른 처리
                        show_progress=True,
                        verbose=True,  # 상세 정보 출력
                        **kwargs)

설정 검증
--------

설정이 올바르게 적용되었는지 확인하는 방법을 설명합니다.

기본 검증
~~~~~~~~~

.. code-block:: python

   import atio
   import pandas as pd
   import tempfile

   # 테스트 데이터 생성
   df = pd.DataFrame({"test": [1, 2, 3]})

   # 설정 테스트
   def test_config():
       # 임시 디렉토리 확인
       print(f"임시 디렉토리: {tempfile.gettempdir()}")
       
       # 로깅 테스트
       atio.write(df, "test.parquet", format="parquet", verbose=True)
       
       # 플래그 파일 확인
       import os
       if os.path.exists(".test.parquet._SUCCESS"):
           print("설정이 올바르게 작동합니다.")
       else:
           print("설정에 문제가 있을 수 있습니다.")

성능 테스트
~~~~~~~~~~

.. code-block:: python

   import time
   import pandas as pd
   import numpy as np

   # 성능 테스트
   def performance_test():
       # 대용량 데이터 생성
       large_df = pd.DataFrame(np.random.randn(100000, 10))
       
       # 성능 측정
       start_time = time.time()
       atio.write(large_df, "performance_test.parquet", 
                  format="parquet", 
                  show_progress=True)
       end_time = time.time()
       
       print(f"처리 시간: {end_time - start_time:.2f}초")
       
       # 파일 크기 확인
       import os
       file_size = os.path.getsize("performance_test.parquet")
       print(f"파일 크기: {file_size / 1024 / 1024:.2f} MB") 