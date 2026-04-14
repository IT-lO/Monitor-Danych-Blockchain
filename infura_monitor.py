import asyncio
from pathlib import Path

from web3 import AsyncWeb3
from web3.providers import WebSocketProvider
from data_filter import filtered_block_to_json
from node_provider_info import node_provider_endpoint
from data_aggregator import save_aggregated_stats

# IMPORTANT - Requires a node_provider_info.py file with variable node_provider_endpoint
# containing URL to the test network.


async def monitor_new_blocks():
    output_dir = Path("filtered_blocks")
    output_dir.mkdir(exist_ok=True)

    async with AsyncWeb3(WebSocketProvider(node_provider_endpoint)) as web3:
        if await web3.is_connected():
            print("Connected")

            subscription_id = await web3.eth.subscribe("newHeads")
            print(f"Subscription ID: {subscription_id}")

            await get_100_last_blocks(web3)

            async for response in web3.socket.process_subscriptions():
                number = response["result"].get("number")

                block = await web3.eth.get_block(number, full_transactions=True)
                block_json = filtered_block_to_json(block)
                output_path = output_dir / f"block_{number}.json"
                output_path.write_text(block_json, encoding="utf-8")
                stats_path = save_aggregated_stats(output_dir, "aggregated_stats.json")
                print(f"Saved block to file: {output_path}")
                print(f"Updated aggregated stats: {stats_path}")


async def get_100_last_blocks(web3: AsyncWeb3):
    output_dir = Path("filtered_blocks")
    output_dir.mkdir(exist_ok=True)

    latest_block_number = await web3.eth.block_number
    block_numbers = range(latest_block_number - 99, latest_block_number + 1)

    tasks = [web3.eth.get_block(block_number, full_transactions=True) for block_number in block_numbers]
    blocks = await asyncio.gather(*tasks)

    for block in blocks:
        block_number = block.get("number")
        block_json = filtered_block_to_json(block)

        output_path = output_dir / f"block_{block_number}.json"
        output_path.write_text(block_json, encoding="utf-8")
        print(f"Saved block to file: {output_path}")

    stats_path = save_aggregated_stats(output_dir, "aggregated_stats.json")
    print(f"Updated aggregated stats: {stats_path}")