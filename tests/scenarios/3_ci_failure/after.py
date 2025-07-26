import os
import atomicwriter as aw
import sys

def safe_ci_cd_pipeline():
    try:
        # Step 1: 원자적 저장 방식으로 중간 산출물 생성
        aw.write("CI/CD Step 1: Preparing build artifacts...\n", "ci_artifact.txt")

        # Step 2: 오류 발생 (하지만 임시 파일로 인해 후속 작업 영향 없음)
        raise RuntimeError("CI/CD Step 2: Deployment failed due to config mismatch")

    except Exception as e:
        print(f"[ERROR] CI/CD 실패: {e}", file=sys.stderr)
        print("[INFO] atomicwriter가 중간 파일 생성을 방지했기 때문에 후속 작업에 영향 없음.")

if __name__ == "__main__":
    safe_ci_cd_pipeline()