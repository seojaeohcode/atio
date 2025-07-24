import pandas as pd
import numpy as np
import os

print("[INFO] 대용량 DataFrame 생성 중...")
# 약 8~10GB짜리 DataFrame (환경에 따라 조정 필요)
df = pd.DataFrame(np.random.rand(10_000_000, 1000))

os.makedirs("data", exist_ok=True)
print("[INFO] 저장 시작: data/oom_output.parquet")

# 💥 메모리 부족 시 실패 → 깨진 파일 남음
df.to_parquet("data/oom_output.parquet")

print("[SUCCESS] 저장 완료")