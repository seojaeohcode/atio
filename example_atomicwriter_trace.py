import os
import pandas as pd
from atomicwriter.core import write


def print_file_status(path):
    print(f"[파일 상태] {path}: {'존재함' if os.path.exists(path) else '없음'}")
    print(
        f"[파일 상태] {path}._SUCCESS: {'존재함' if os.path.exists(path + '._SUCCESS') else '없음'}"
    )


print("=== [성공 케이스: parquet] ===")
df = pd.DataFrame({"a": [1, 2, 3]})
out_path = "demo.parquet"
write(df, out_path, format="parquet")
print_file_status(out_path)

print("\n=== [성공 케이스: csv] ===")
out_path2 = "demo.csv"
write(df, out_path2, format="csv")
print_file_status(out_path2)

print("\n=== [실패 케이스: 지원하지 않는 포맷] ===")
out_path3 = "demo.txt"
try:
    write(df, out_path3, format="txt")
except Exception as e:
    print(f"[예외 발생] {e}")
print_file_status(out_path3)
