import pandas as pd
import time
import signal
import sys
import atomicwriter as aw

# Ctrl+C 감지 시 사용자 메시지 출력
def handler(signum, frame):
    print("\n[!] KeyboardInterrupt 감지됨. 안전하게 종료 중...")
    sys.exit(1)

signal.signal(signal.SIGINT, handler)

# 데이터 준비
df = pd.DataFrame({"a": range(10000000)})

print("[INFO] 5초 후 AtomicWriter를 사용한 저장 시작")
time.sleep(5)

# 핵심: atomicwriter 사용
print("[INFO] Atomic 저장 시작 / 지금 Ctrl+C로 중단해 보세요...")
aw.write(df, "data/output.parquet", format="parquet")
print("[SUCCESS] Atomic 저장 완료")
