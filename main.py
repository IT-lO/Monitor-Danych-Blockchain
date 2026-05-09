import asyncio
from infura_monitor import monitor_new_blocks
from block_state_manager import BlockchainState
from console_dashboard import display_ui


async def main():
    state = BlockchainState()
    monitor_task = asyncio.create_task(monitor_new_blocks(state))
    ui_task = asyncio.create_task(display_ui(state))

    try:
        await asyncio.gather(monitor_task, ui_task)
    except KeyboardInterrupt:
        state.is_running = False
    finally:
        monitor_task.cancel()
        ui_task.cancel()
        await asyncio.gather(monitor_task, ui_task, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[bold red]Monitor stopped by user.[/bold red]")
