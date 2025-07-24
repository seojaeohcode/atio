import pandas as pd
import time
import signal
import sys

# 시그널 핸들러 설정 (테스트 편의를 위해)
def handler(signum, frame):
    print("\n[!] KeyboardInterrupt (Ctrl+C) 감지됨. 종료 중...")
    sys.exit(1)

signal.signal(signal.SIGINT, handler)

# 예시 데이터 생성
df = pd.DataFrame({"a": range(100000000)})

print("[INFO] 5초 후 Parquet 파일 저장 시작.")
time.sleep(5)

# 파일 저장 (문제 코드)
print("[INFO] 저장 시작: data/output.parquet / 지금 Ctrl+C로 중단해 보세요...")
df.to_parquet("data/output.parquet")
print("[SUCCESS] 저장 완료")