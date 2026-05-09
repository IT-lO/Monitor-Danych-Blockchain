import asyncio
from pathlib import Path
from web3 import AsyncWeb3
from web3.providers import WebSocketProvider
from data_filter import filtered_block_to_json
from node_provider_info import node_provider_endpoint
from data_aggregator import save_aggregated_stats, aggregate_filtered_blocks


async def monitor_new_blocks(state):
    output_dir = Path("filtered_blocks")
    output_dir.mkdir(exist_ok=True)

    while True:
        try:
            state.add_log("Trying to connect...")
            async with AsyncWeb3(WebSocketProvider(node_provider_endpoint)) as web3:
                if await web3.is_connected():
                    state.add_log("Connected to Sepolia")

                    # Syncing 100 blocks using the limiter
                    await get_x_last_blocks(web3, 100, state)

                    subscription_id = await web3.eth.subscribe("newHeads")
                    async for response in web3.socket.process_subscriptions():
                        number = response["result"].get("number")

                        block = await web3.eth.get_block(number, full_transactions=True)
                        block_json = filtered_block_to_json(block)

                        output_path = output_dir / f"block_{number}.json"
                        output_path.write_text(block_json, encoding="utf-8")

                        save_aggregated_stats(output_dir, "aggregated_stats.json")
                        state.update_stats(aggregate_filtered_blocks(output_dir))

                        state.add_log(f"New block processed: {number}")

        except Exception as e:
            state.add_log(f"Connection lost: {e}")
            await asyncio.sleep(5)


async def get_x_last_blocks(web3: AsyncWeb3, amount: int, state):
    output_dir = Path("filtered_blocks")
    output_dir.mkdir(exist_ok=True)

    latest_block_number = await web3.eth.block_number
    block_numbers = range(latest_block_number - amount + 1, latest_block_number + 1)

    semaphore = asyncio.Semaphore(10)

    async def fetch_block_with_limit(bn):
        """Fetches a full block while respecting the API rate limits."""
        async with semaphore:
            await asyncio.sleep(0.1)
            return await web3.eth.get_block(bn, full_transactions=True)

    tasks = [fetch_block_with_limit(bn) for bn in block_numbers]

    state.add_log(f"Starting download of {amount} blocks...")
    blocks = await asyncio.gather(*tasks, return_exceptions=True)

    for block in blocks:
        if block is None or isinstance(block, Exception):
            if isinstance(block, Exception):
                state.add_log(f"Error retrieving block: {block}")
            continue

        try:
            block_number = block.get("number")
            block_json = filtered_block_to_json(block)
            output_path = output_dir / f"block_{block_number}.json"
            output_path.write_text(block_json, encoding="utf-8")

            state.add_log(f"Saved block to file: {output_path}")
        except Exception as e:
            state.add_log(f"Error while processing block: {e}")

    save_aggregated_stats(output_dir, "aggregated_stats.json")
    state.update_stats(aggregate_filtered_blocks(output_dir))
    state.add_log("Initial sync complete.")
