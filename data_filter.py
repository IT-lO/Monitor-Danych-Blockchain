import json
from typing import Any

from web3 import Web3


def normalize_hex(value: Any) -> Any:
    """Converts HexBytes values to a plain hex string."""
    if value is None:
        return None

    if hasattr(value, "hex"):
        return value.hex()

    return value


def wei_to_eth(value: Any) -> float:
    """Converts a value from wei to ETH."""
    if value is None:
        return 0.0

    return float(Web3.from_wei(int(value), "ether"))


def filter_block_data(block: dict[str, Any]) -> dict[str, Any]:
    """Returns the basic filtered fields for a block."""
    transactions = block.get("transactions", [])

    return {
        "block_number": block.get("number"),
        "block_hash": normalize_hex(block.get("hash")),
        "transaction_count": len(transactions),
    }


def filter_transaction_data(transaction: dict[str, Any]) -> dict[str, Any]:
    """Returns the selected fields for a single transaction."""
    return {
        "transaction_hash": normalize_hex(transaction.get("hash")),
        "from_address": transaction.get("from"),
        "to_address": transaction.get("to"),
        "value_eth": wei_to_eth(transaction.get("value")),
        "gas": transaction.get("gas"),
        "gas_price": transaction.get("gasPrice"),
    }


def filter_block_with_transactions(block: dict[str, Any]) -> dict[str, Any]:
    """Combines filtered block data and transactions into one structure."""
    return {
        "block": filter_block_data(block),
        "transactions": [
            filter_transaction_data(transaction)
            for transaction in block.get("transactions", [])
        ],
    }


def filtered_block_to_json(block: dict[str, Any]) -> str:
    """Serializes a filtered block to JSON."""
    filtered_block = filter_block_with_transactions(block)
    return json.dumps(filtered_block, indent=2, ensure_ascii=False)
