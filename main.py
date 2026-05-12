import asyncio

from File_Report import startReport
from infura_monitor import monitor_new_blocks




if __name__ == "__main__":
    try:
        asyncio.run(monitor_new_blocks())
    except KeyboardInterrupt:
        startReport()
        pass
        print("\n[bold red]Monitor stopped by user.[/bold red]")
