import os
import pandas as pd
from atomicwriter import write


def print_file_status(path):
    print(f"[파일 상태] {path}: {'존재함' if os.path.exists(path) else '없음'}")
    print(
        f"[파일 상태] {path}._SUCCESS: {'존재함' if os.path.exists(path + '._SUCCESS') else '없음'}"
    )


# print("=== [성공 케이스: parquet] ===")
# df = pd.DataFrame({"a": [1, 2, 3]})
# out_path = "demo.parquet"
# write(df, out_path, format="parquet")
# print_file_status(out_path)

# print("\n=== [성공 케이스: csv] ===")
# out_path2 = "demo.csv"
# write(df, out_path2, format="csv")
# print_file_status(out_path2)

# print("\n=== [실패 케이스: 지원하지 않는 포맷] ===")
# out_path3 = "demo.txt"
# try:
#     write(df, out_path3, format="txt")
# except Exception as e:
#     print(f"[예외 발생] {e}")
#     pass
# print_file_status(out_path3)


"""
테스트 코드
엑셀, numpy 제외 잘 작동
"""

import os
import pandas as pd
from atomicwriter.core import write

def print_file_status(path):
    print(f"[파일 상태] {path}: {'존재함' if os.path.exists(path) else '없음'}")
    print(f"[파일 상태] {path}._SUCCESS: {'존재함' if os.path.exists(path + '._SUCCESS') else '없음'}")

df = pd.DataFrame({"a": [1, 2, 3]})

# Pandas 테스트 (특별 케이스 제외)
for fmt, ext in [("csv", "csv"), ("excel", "xlsx"), ("parquet", "parquet"), ("json", "json"), ("pickle", "pkl"), ("html", "html")]:
    out_path = f"./tests/output_data/demo_pandas.{ext}"
    print(f"\n=== [Pandas: {fmt}] ===")
    try:
        write(df, out_path, format=fmt)
    except Exception as e:
        print(f"[예외 발생] {e}")
    print_file_status(out_path)

# Polars 테스트 (특별 케이스 제외)
try:
    import polars as pl
    pl_df = pl.DataFrame({"a": [1, 2, 3]})
    for fmt, ext in [("csv", "csv"), ("excel", "xlsx"), ("parquet", "parquet"), ("json", "json"), ("ipc", "ipc"), ("avro", "avro")]:
        out_path = f"./tests/output_data/demo_polars.{ext}"
        print(f"\n=== [Polars: {fmt}] ===")
        try:
            write(pl_df, out_path, format=fmt)
        except Exception as e:
            print(f"[예외 발생] {e}")
        print_file_status(out_path)
except ImportError:
    print("\n[Polars 미설치: 테스트 건너뜀]")

# NumPy 테스트 (특별 케이스 제외)
try:
    import numpy as np
    arr = np.array([[1, 2, 3], [4, 5, 6]])
    for fmt, ext in [("npy", "npy"), ("npz", "npz"), ("csv", "csv")]:
        out_path = f"./tests/output_data/demo_numpy.{ext}"
        print(f"\n=== [NumPy: {fmt}] ===")
        try:
            write(arr, out_path, format=fmt)
        except Exception as e:
            print(f"[예외 발생] {e}")
        print_file_status(out_path)
    # bin은 메소드 호출이므로 별도 처리 필요
except ImportError:
    print("\n[NumPy 미설치: 테스트 건너뜀]")