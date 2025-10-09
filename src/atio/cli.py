import os
from pytz import timezone
import typer
from rich.console import Console
from rich.table import Table

from .core import (
    list_snapshots,
    tag_version,
    revert,
    delete_version
)

# Typer 앱과 Rich 콘솔 객체를 생성합니다.
app = typer.Typer(help="Atio: A simple, efficient, and robust data versioning tool.")
console = Console()

@app.command()
def list(
    table_path: str = typer.Argument(..., help="스냅샷 목록을 조회할 테이블의 경로")
):
    """
    테이블에 저장된 모든 스냅샷의 로그를 조회합니다.
    """
    try:
        snapshots = list_snapshots(table_path)
        if not snapshots:
            console.print(f"'{table_path}'에서 스냅샷을 찾을 수 없습니다.")
            raise typer.Exit()

        table = Table(title=f"Snapshot History for '{os.path.basename(table_path)}'")
        table.add_column("Version", style="cyan", no_wrap=True)
        table.add_column("Latest", style="magenta")
        table.add_column("Tags", style="green")
        table.add_column("Timestamp (UTC)", style="yellow")
        table.add_column("Message")

        for s in snapshots:
            latest_marker = "✅" if s['is_latest'] else ""
            tags_str = ", ".join(s['tags'])
            
            # timestamp를 사람이 읽기 좋은 형태로 변환
            from datetime import datetime
            ts_utc = datetime.fromtimestamp(s.get('timestamp', 0), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            
            table.add_row(
                str(s['version_id']),
                latest_marker,
                tags_str,
                ts_utc,
                s['message']
            )
        
        console.print(table)

    except Exception as e:
        console.print(f"[bold red]오류 발생:[/bold red] {e}")
        raise typer.Exit(code=1)

@app.command()
def tag(
    table_path: str = typer.Argument(..., help="태그를 지정할 테이블의 경로"),
    version_id: int = typer.Argument(..., help="태그를 붙일 버전 ID"),
    tag_name: str = typer.Argument(..., help="지정할 태그 이름"),
):
    """
    특정 버전에 태그를 지정하거나 업데이트합니다.
    """
    try:
        success = tag_version(table_path, version_id, tag_name)
        if success:
            console.print(f"✅ [green]성공:[/green] 버전 {version_id}에 '{tag_name}' 태그를 지정했습니다.")
        else:
            # tag_version 함수 내부에서 이미 에러 로그를 출력했을 수 있습니다.
            console.print(f"❌ [red]실패:[/red] 태그 지정에 실패했습니다. 자세한 내용은 로그를 확인하세요.")
            raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[bold red]오류 발생:[/bold red] {e}")
        raise typer.Exit(code=1)

@app.command()
def revert(
    table_path: str = typer.Argument(..., help="리버트할 테이블의 경로"),
    version_id: int = typer.Argument(..., help="상태를 되돌릴 목표 버전 ID"),
    message: str = typer.Option(None, "-m", "--message", help="새로운 리버트 버전에 대한 커밋 메시지")
):
    """
    과거 버전의 상태를 가져와 새로운 버전으로 생성합니다.
    """
    try:
        success = revert(table_path, version_id, message=message)
        if success:
            console.print(f"✅ [green]성공:[/green] v{version_id}의 상태로 리버트하여 새로운 버전을 생성했습니다.")
        else:
            console.print(f"❌ [red]실패:[/red] 리버트 작업에 실패했습니다.")
            raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[bold red]오류 발생:[/bold red] {e}")
        raise typer.Exit(code=1)

@app.command()
def delete(
    table_path: str = typer.Argument(..., help="버전을 삭제할 테이블의 경로"),
    version_id: int = typer.Argument(..., help="삭제할 버전 ID"),
    dry_run: bool = typer.Option(False, "--dry-run", help="실제로 삭제하지 않고, 삭제될 파일 목록만 출력합니다."),
):
    """
    특정 버전을 삭제하고 사용되지 않는 데이터를 정리합니다. (가비지 컬렉션)
    """
    try:
        # delete_version 함수가 dry_run을 이미 지원하므로 그대로 전달
        success = delete_version(table_path, version_id, dry_run=dry_run)
        if success and not dry_run:
            console.print(f"✅ [green]성공:[/green] 버전 {version_id}을(를) 삭제하고 관련 데이터를 정리했습니다.")
        elif success and dry_run:
             console.print(f"✅ [yellow]Dry Run 완료:[/yellow] 위의 목록이 삭제될 예정입니다.")
        else:
            console.print(f"❌ [red]실패:[/red] 버전 삭제에 실패했습니다.")
    except Exception as e:
        console.print(f"[bold red]오류 발생:[/bold red] {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()