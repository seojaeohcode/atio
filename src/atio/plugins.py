from .utils import setup_logger
# 추후 Pandas, Polars, Numpy 등 확장 지원을 위한 플러그인 구조

logger = setup_logger()

# { 객체 타입: { 포맷: 핸들러 } }
# 핸들러는 '메소드 이름(str)' 또는 '호출 가능한 함수'가 될 수 있습니다.
WRITER_MAPPING = {}

def register_writer(obj_type, fmt, handler):
    """(객체 타입, 포맷) 쌍으로 쓰기 핸들러를 등록"""
    if obj_type not in WRITER_MAPPING:
        WRITER_MAPPING[obj_type] = {}
    WRITER_MAPPING[obj_type][fmt] = handler
    logger.debug(f"Writer registered: type={obj_type.__name__}, format={fmt}, handler={handler}")

def get_writer(obj, fmt):
    """객체의 타입과 포맷에 맞는 핸들러를 조회"""
    obj_type = type(obj)
    handler = WRITER_MAPPING.get(obj_type, {}).get(fmt)
    if handler is None:
        logger.warning(f"No writer found for type {obj_type.__name__} and format '{fmt}'")
    return handler

# ---------------------------------------------------------------------------
# 1. Pandas 쓰기 방법 등록
# ---------------------------------------------------------------------------
try:
    import pandas as pd

    PANDAS_DF_TYPE = pd.DataFrame
    
    # Pandas DataFrame에 대한 쓰기 핸들러 등록
    # 값은 DataFrame 객체의 메소드 이름(문자열)입니다.
    # 예: format='csv' -> df.to_csv(...) 호출
    register_writer(PANDAS_DF_TYPE, "csv", "to_csv")
    register_writer(PANDAS_DF_TYPE, "parquet", "to_parquet")
    register_writer(PANDAS_DF_TYPE, "json", "to_json")
    register_writer(PANDAS_DF_TYPE, "pickle", "to_pickle")
    register_writer(PANDAS_DF_TYPE, "html", "to_html")
    
    # Excel 쓰기. `openpyxl` 라이브러리가 필요합니다.
    # pip install openpyxl
    register_writer(PANDAS_DF_TYPE, "excel", "to_excel")

    # SQL 쓰기. `sqlalchemy` 라이브러리가 필요합니다.
    # 이 핸들러는 core.py에서 특별 처리됩니다 (파일 시스템을 사용하지 않음).
    # pip install sqlalchemy
    register_writer(PANDAS_DF_TYPE, "sql", "to_sql")
    
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

    # Polars DataFrame에 대한 쓰기 핸들러 등록
    register_writer(POLARS_DF_TYPE, "csv", "write_csv")
    register_writer(POLARS_DF_TYPE, "parquet", "write_parquet")
    register_writer(POLARS_DF_TYPE, "json", "write_json")
    register_writer(POLARS_DF_TYPE, "ipc", "write_ipc")
    register_writer(POLARS_DF_TYPE, "avro", "write_avro")
    
    # Polars Excel 쓰기. `xlsx2csv`와 `openpyxl`이 필요할 수 있습니다.
    # pip install xlsx2csv openpyxl
    register_writer(POLARS_DF_TYPE, "excel", "write_excel")
    
    # Polars 데이터베이스 쓰기. connector-x 가 필요합니다.
    # pip install connectorx
    # 이 핸들러는 core.py에서 특별 처리됩니다.
    register_writer(POLARS_DF_TYPE, "database", "write_database")

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
    # 값: 실제 '함수 객체' (호출 방식: np.save(path, arr))
    register_writer(NUMPY_NDARRAY_TYPE, "npy", np.save)
    register_writer(NUMPY_NDARRAY_TYPE, "npz", np.savez)
    register_writer(NUMPY_NDARRAY_TYPE, "npz_compressed", np.savez_compressed)
    register_writer(NUMPY_NDARRAY_TYPE, "csv", np.savetxt)
    
    # 값: '메소드 이름(문자열)' (호출 방식: arr.tofile(path))
    register_writer(NUMPY_NDARRAY_TYPE, "bin", "tofile")

    # 여러 배열을 한 번에 저장하기 위해 dict 타입도 지원
    register_writer(dict, "npz", np.savez)
    register_writer(dict, "npz_compressed", np.savez_compressed)

    logger.info("NumPy writers registered successfully.")

except ImportError:
    logger.info("NumPy not found. Skipping numpy writer registration.")
    pass
