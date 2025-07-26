import os
import sys

def broken_ci_cd_pipeline():
    try:
        # Step 1: CI/CD 중간 산출물 생성
        with open("ci_artifact.txt", "w") as f:
            f.write("CI/CD Step 1: Preparing build artifacts...\n")
        
        # Step 2: 중간에 오류 발생
        raise RuntimeError("CI/CD Step 2: Deployment failed due to config mismatch")

    except Exception as e:
        print(f"[ERROR] CI/CD 실패: {e}", file=sys.stderr)
        print("[WARNING] 불완전한 파일이 저장되었을 수 있습니다. 후속 작업에 영향을 줄 수 있습니다.")

if __name__ == "__main__":
    broken_ci_cd_pipeline()