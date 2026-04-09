# Blockchain Data Monitor

Project for monitoring new Ethereum Sepolia blocks over WebSocket.

The program:
- connects to an RPC node using `web3`
- listens for new blocks
- fetches full block data together with transactions
- filters only the selected fields
- saves each filtered block as a separate file in the `filtered_blocks` directory
- updates aggregated statistics from filtered blocks in a separate JSON file

## Saved data

For each block:
- `block_number`
- `block_hash`
- `transaction_count`

For each transaction:
- `transaction_hash`
- `from_address`
- `to_address`
- `value_eth`
- `gas`
- `gas_price`

Aggregated statistics:

- `processed_blocks`
- `total_transactions`
- `avg_transactions_per_block`
- `min_transactions_in_block`
- `block_with_min_transactions`
- `max_transactions_in_block`
- `block_with_max_transactions`
- `total_value_eth`
- `avg_value_eth_per_transaction`
- `zero_value_transactions`
- `zero_value_transaction_ratio`
- `total_gas`
- `avg_gas_per_transaction`
- `avg_gas_price`
- `min_gas_price`
- `max_gas_price`
- `unique_senders`
- `unique_receivers`
- `top_senders`
- `top_receivers`

## Requirements

- Python 3.10+
- an active WebSocket endpoint for Ethereum Sepolia or another test network

Set the endpoint in [node_provider_info.py](.\node_provider_info.py).

## Install dependencies

```powershell
pip install -r requirements.txt
```

## Run the program

```powershell
python main.py
```

After startup, the program saves new blocks into the `filtered_blocks` directory as:

```text
filtered_blocks/block_<block_number>.json
```

During monitoring, the program also updates:

```text
aggregated_stats.json
```

## Run tests

```powershell
pytest
```

## Project structure

- [main.py](.\main.py) - program entry point
- [infura_monitor.py](.\infura_monitor.py) - WebSocket connection, block saving, and stats update
- [data_filter.py](.\data_filter.py) - data filtering and JSON preparation
- [data_aggregator.py](.\data_aggregator.py) - aggregation of statistics from filtered blocks
- [tests/test_data_filter.py](.\tests\test_data_filter.py) - unit tests for filtering
- [tests/test_data_aggregator.py](.\tests\test_statistics_aggregator.py) - unit tests for aggregation


