import pandas as pd
import numpy as np
from atomicwriter.core import write

# 대용량 더미 데이터 생성
print("Creating a large dummy DataFrame...")
size_mb = 2048
df = pd.DataFrame(np.random.rand(int(size_mb * 131072), 1), columns=['value'])
print(f"Dummy DataFrame created (approx. {size_mb}MB in memory).")

# 새로운 기능 테스트
write(
    df,
    "./tests/output_data/large_data.parquet",
    format="parquet",
    show_progress=True  # 이 옵션 하나로 진행도 표시 기능 활성화
)