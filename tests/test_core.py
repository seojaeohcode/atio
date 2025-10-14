# Copyright 2025 Atio Developers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import pandas as pd
import pytest
from atio import write


def test_write_parquet(tmp_path):
    df = pd.DataFrame({"a": [1, 2, 3]})
    out_path = tmp_path / "test.parquet"
    write(df, str(out_path), format="parquet")
    assert out_path.exists()
    success_path = out_path.parent / f".{out_path.name}._SUCCESS"
    assert os.path.exists(success_path)
    with open(success_path) as f:
        assert f.read().strip() == "OK"


def test_write_csv(tmp_path):
    df = pd.DataFrame({"a": [1, 2, 3]})
    out_path = tmp_path / "test.csv"
    write(df, str(out_path), format="csv")
    assert out_path.exists()
    success_path = out_path.parent / f".{out_path.name}._SUCCESS"
    assert os.path.exists(success_path)
    with open(success_path) as f:
        assert f.read().strip() == "OK"


def test_unsupported_format(tmp_path):
    df = pd.DataFrame({"a": [1, 2, 3]})
    out_path = tmp_path / "test.txt"
    with pytest.raises(ValueError):
        write(df, str(out_path), format="txt")