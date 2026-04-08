# Blockchain Data Monitor

Project for monitoring new Ethereum Sepolia blocks over WebSocket.

The program:
- connects to an RPC node using `web3`
- listens for new blocks
- fetches full block data together with transactions
- filters only the selected fields
- saves each filtered block as a separate file in the `filtered_blocks` directory

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

## Run tests

```powershell
pytest
```

## Project structure

- [main.py](.\main.py) - program entry point
- [infura_monitor.py](.\infura_monitor.py) - WebSocket connection and block saving
- [data_filter.py](.\data_filter.py) - data filtering and JSON preparation
- [tests/test_data_filter.py](.\tests\test_data_filter.py) - unit tests for filtering
