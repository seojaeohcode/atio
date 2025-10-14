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