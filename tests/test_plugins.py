import pytest
from src.atomicwriter.plugins import register_writer, get_writer


def dummy_writer(obj, path, **kwargs):
    with open(path, "w") as f:
        f.write("dummy")


def test_register_and_get_writer():
    register_writer("dummy", dummy_writer)
    writer = get_writer("dummy")
    assert writer is dummy_writer


# polars 지원 예시 (실제 polars 설치 필요)
try:
    import polars as pl

    def polars_parquet(obj, path, **kwargs):
        obj.write_parquet(path, **kwargs)

    register_writer("polars_parquet", polars_parquet)

    def test_polars_writer(tmp_path):
        df = pl.DataFrame({"a": [1, 2, 3]})
        out_path = tmp_path / "test_polars.parquet"
        writer = get_writer("polars_parquet")
        writer(df, str(out_path))
        assert out_path.exists()

except ImportError:
    pass
