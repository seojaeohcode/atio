import pandas as pd
import numpy as np
import atomicwriter as aw
import os

print("[INFO] 대용량 DataFrame 생성 중...")
df = pd.DataFrame(np.random.rand(10_000_000, 1000))

os.makedirs("data", exist_ok=True)
print("[INFO] atomicwriter 사용하여 저장 시작: data/oom_output.parquet")

# ✅ atomicwriter 사용: 실패 시 원본 보존, 깨진 파일 방지
aw.write(df, "data/oom_output.parquet", format="parquet")

print("[SUCCESS] atomicwriter 저장 완료")