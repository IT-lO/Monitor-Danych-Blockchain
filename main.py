import asyncio
from infura_monitor import monitor_new_blocks

if __name__ == "__main__":
    try:
        asyncio.run(monitor_new_blocks())
    except KeyboardInterrupt:
        pass