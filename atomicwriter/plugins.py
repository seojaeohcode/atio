# 추후 Pandas, Polars, Numpy 등 확장 지원을 위한 플러그인 구조

_WRITERS = {}


def register_writer(fmt, handler):
    """포맷별(혹은 객체별) 쓰기 핸들러 등록"""
    _WRITERS[fmt] = handler


def get_writer(fmt):
    return _WRITERS.get(fmt)


# 기본 pandas DataFrame 핸들러 등록 예시
def _pandas_parquet(obj, path, **kwargs):
    obj.to_parquet(path, **kwargs)


def _pandas_csv(obj, path, **kwargs):
    obj.to_csv(path, **kwargs)


try:
    import pandas as pd

    register_writer("parquet", _pandas_parquet)
    register_writer("csv", _pandas_csv)
except ImportError:
    pass
