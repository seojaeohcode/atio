import os
import pandas as pd
import pytest
from atio.core import write


def test_write_parquet(tmp_path):
    df = pd.DataFrame({"a": [1, 2, 3]})
    out_path = tmp_path / "test.parquet"
    write(df, str(out_path), format="parquet")
    assert out_path.exists()
    success_path = str(out_path) + "._SUCCESS"
    assert os.path.exists(success_path)
    with open(success_path) as f:
        assert f.read().strip() == "OK"


def test_write_csv(tmp_path):
    df = pd.DataFrame({"a": [1, 2, 3]})
    out_path = tmp_path / "test.csv"
    write(df, str(out_path), format="csv")
    assert out_path.exists()
    success_path = str(out_path) + "._SUCCESS"
    assert os.path.exists(success_path)
    with open(success_path) as f:
        assert f.read().strip() == "OK"


def test_unsupported_format(tmp_path):
    df = pd.DataFrame({"a": [1, 2, 3]})
    out_path = tmp_path / "test.txt"
    with pytest.raises(ValueError):
        write(df, str(out_path), format="txt")