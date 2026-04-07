from web3 import AsyncWeb3
from web3.providers import WebSocketProvider
from node_provider_info import node_provider_endpoint

# IMPORTANT - Requires a node_provider_info.py file with variable node_provider_endpoint
# containing URL to the test network.

async def monitor_new_blocks():
    async with AsyncWeb3(WebSocketProvider(node_provider_endpoint)) as web3:
        if await web3.is_connected():
            print("Connected")

            subscription_id = await web3.eth.subscribe("newHeads")
            print(f"Subscription ID: {subscription_id}")

            await web3.eth.get_block("latest")

            async for response in web3.socket.process_subscriptions():
                print(f"Response: {response}")

                number = response['result'].get('number')
                print(f"Block number: {number}")

                print()

                block = await web3.eth.get_block(number)
                print(f"Block details: {block}")

                print()

                block = await web3.eth.get_block(number, full_transactions=True)
                print(f"Block details with full transactions: {block}")