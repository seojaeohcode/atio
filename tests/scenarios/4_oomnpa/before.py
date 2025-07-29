import pandas as pd
import numpy as np
import os

print("[INFO] 대용량 DataFrame 생성 중...")
df = pd.DataFrame(np.random.rand(10_000_000, 1000))

os.makedirs("data", exist_ok=True)
print("[INFO] CSV 저장 시작: data/oom_broken.csv")

# ❌ atomicwriter 미사용
with open("data/oom_broken.csv", "w", encoding="utf-8") as f:
    for chunk in np.array_split(df, 100):
        chunk.to_csv(f, header=False, index=False)
        raise MemoryError("💥 인위적 OOM 발생 (CSV 저장 중)")

print("[SUCCESS] 저장 완료")