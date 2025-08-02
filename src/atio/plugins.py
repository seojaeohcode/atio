from .utils import setup_logger
# 추후 Pandas, Polars, Numpy 등 확장 지원을 위한 플러그인 구조

logger = setup_logger()

# { 객체 타입: { 포맷: 핸들러 함수 } }
WRITER_MAPPING = {}

def register_writer(obj_type, fmt, handler):
    """(객체 타입, 포맷) 쌍으로 쓰기 핸들러를 등록"""
    if obj_type not in WRITER_MAPPING:
        WRITER_MAPPING[obj_type] = {}
    WRITER_MAPPING[obj_type][fmt] = handler

def get_writer(obj, fmt):
    """객체의 타입과 포맷에 맞는 핸들러를 조회"""
    obj_type = type(obj)
    return WRITER_MAPPING.get(obj_type, {}).get(fmt)

# ---------------------------------------------------------------------------
# 1. Pandas 쓰기 방법 등록
# ---------------------------------------------------------------------------
try:
    import pandas as pd

    PANDAS_DF_TYPE = pd.DataFrame
    
    WRITER_MAPPING[PANDAS_DF_TYPE] = {
        "csv": "to_csv",
        "excel": "to_excel",
        "parquet": "to_parquet",
        "json": "to_json",
        "sql": "to_sql",         # 특별 케이스, core.py에서 별도 처리 필요
        "pickle": "to_pickle",
        "html": "to_html",
    }
    logger.info("Pandas writers registered successfully.")

except ImportError:
    logger.info("Pandas not found. Skipping pandas writer registration.")
    pass

# ---------------------------------------------------------------------------
# 2. Polars 쓰기 방법 등록
# ---------------------------------------------------------------------------
try:
    import polars as pl

    POLARS_DF_TYPE = pl.DataFrame

    WRITER_MAPPING[POLARS_DF_TYPE] = {
        "csv": "write_csv",
        "excel": "write_excel",
        "parquet": "write_parquet",
        "json": "write_json",
        "ipc": "write_ipc",
        "avro": "write_avro",
        "delta": "write_delta",     # 특별 케이스
        "database": "write_database", # 특별 케이스
    }
    logger.info("Polars writers registered successfully.")

except ImportError:
    logger.info("Polars not found. Skipping polars writer registration.")
    pass

# ---------------------------------------------------------------------------
# 3. NumPy 쓰기 방법 등록
# ---------------------------------------------------------------------------
try:
    import numpy as np
    
    NUMPY_NDARRAY_TYPE = np.ndarray

    # NumPy는 저장 방식이 메소드와 함수가 섞여있어 구분이 중요합니다.
    WRITER_MAPPING[NUMPY_NDARRAY_TYPE] = {
        # 값: 실제 '함수 객체' (호출 방식: np.save(arr, path))
        "npy": np.save,
        "npz": np.savez,
        "npz_compressed": np.savez_compressed,
        "csv": np.savetxt,
        
        # 값: '메소드 이름(문자열)' (호출 방식: arr.tofile(path))
        "bin": "tofile",
    }
    WRITER_MAPPING[dict] = {
        "npz": np.savez,  # 딕셔너리 저장을 위해 np.savez 사용
        "npz_compressed": np.savez_compressed,  # 압축 저장
    }
    logger.info("NumPy writers registered successfully.")

except ImportError:
    logger.info("NumPy not found. Skipping numpy writer registration.")
    pass