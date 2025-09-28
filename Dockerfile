# Atio 벤치마크 테스트 환경
FROM python:3.11-slim

# 작업 디렉토리 설정
WORKDIR /app

# Python 의존성 파일 복사 및 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 소스 코드 및 벤치마크 스크립트 복사
COPY src/ ./src/
COPY benchmark_write_speed.py .

# 벤치마크 실행
CMD ["python", "benchmark_write_speed.py"]
