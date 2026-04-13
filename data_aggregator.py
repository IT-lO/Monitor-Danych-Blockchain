import json
from collections import Counter
from pathlib import Path
from typing import Any


def round_value(value: float, digits: int = 8) -> float:
    """Rounds numeric values"""
    return round(value, digits)


def safe_average(total: float, count: int) -> float:
    """Returns an average or zero when there is no data."""
    if count == 0:
        return 0.0

    return total / count


def load_filtered_block(path: Path) -> dict[str, Any]:
    """Loads one filtered block JSON file."""
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def top_addresses(counter: Counter[str], limit: int = 5) -> list[dict[str, Any]]:
    """Returns the most active addresses with transaction counts."""
    return [
        {"address": address, "transaction_count": count}
        for address, count in counter.most_common(limit)
    ]


def aggregate_filtered_blocks(filtered_blocks_dir: Path | str = "filtered_blocks") -> dict[str, Any]:
    """Aggregates statistics from all filtered block JSON files."""
    input_dir = Path(filtered_blocks_dir)
    # Only block files, sorted for deterministic results (for tests).
    block_files = sorted(input_dir.glob("block_*.json"))

    sender_counter = Counter()
    receiver_counter = Counter()
    total_transactions = 0
    total_value_eth = 0.0
    total_gas = 0
    total_gas_price = 0
    zero_value_transactions = 0
    min_transactions_in_block = None
    max_transactions_in_block = None
    block_with_min_transactions = None
    block_with_max_transactions = None
    min_gas_price = None
    max_gas_price = None
    latest_block_number = None
    latest_gas_price = 0
    per_block_stats = []

    for block_file in block_files:
        filtered_block = load_filtered_block(block_file)
        block_data = filtered_block["block"]
        transactions = filtered_block["transactions"]
        block_number = block_data["block_number"]
        block_hash = block_data["block_hash"]
        transaction_count = block_data["transaction_count"]
        block_total_value_eth = 0.0
        block_total_gas = 0
        block_total_gas_price = 0
        block_zero_value_transactions = 0
        block_min_gas_price = None
        block_max_gas_price = None

        total_transactions += transaction_count

        if min_transactions_in_block is None or transaction_count < min_transactions_in_block:
            min_transactions_in_block = transaction_count
            block_with_min_transactions = block_number

        if max_transactions_in_block is None or transaction_count > max_transactions_in_block:
            max_transactions_in_block = transaction_count
            block_with_max_transactions = block_number

        if latest_block_number is None or block_number > latest_block_number:
            latest_block_number = block_number
            if transactions:
                latest_gas_price = transactions[-1]["gas_price"]

        for transaction in transactions:
            value_eth = transaction["value_eth"]
            gas = transaction["gas"]
            gas_price = transaction["gas_price"]
            from_address = transaction["from_address"]
            to_address = transaction["to_address"]

            total_value_eth += value_eth
            total_gas += gas
            total_gas_price += gas_price
            block_total_value_eth += value_eth
            block_total_gas += gas
            block_total_gas_price += gas_price

            if value_eth == 0.0:
                zero_value_transactions += 1
                block_zero_value_transactions += 1

            if from_address:
                sender_counter[from_address] += 1

            if to_address:
                receiver_counter[to_address] += 1

            if min_gas_price is None or gas_price < min_gas_price:
                min_gas_price = gas_price

            if max_gas_price is None or gas_price > max_gas_price:
                max_gas_price = gas_price

            if block_min_gas_price is None or gas_price < block_min_gas_price:
                block_min_gas_price = gas_price

            if block_max_gas_price is None or gas_price > block_max_gas_price:
                block_max_gas_price = gas_price

        per_block_stats.append({
            "block_number": block_number,
            "block_hash": block_hash,
            "transaction_count": transaction_count,
            "total_value_eth": round_value(block_total_value_eth),
            "avg_value_eth_per_transaction": round_value(safe_average(block_total_value_eth, transaction_count)),
            "zero_value_transactions": block_zero_value_transactions,
            "zero_value_transaction_ratio": round_value(safe_average(block_zero_value_transactions, transaction_count)),
            "total_gas": block_total_gas,
            "avg_gas_per_transaction": round_value(safe_average(block_total_gas, transaction_count)),
            "avg_gas_price": round_value(safe_average(block_total_gas_price, transaction_count)),
            "min_gas_price": block_min_gas_price if block_min_gas_price is not None else 0,
            "max_gas_price": block_max_gas_price if block_max_gas_price is not None else 0,
        })

    processed_blocks = len(block_files)

    return {
        "processed_blocks": processed_blocks,
        "total_transactions": total_transactions,
        "avg_transactions_per_block": round_value(safe_average(total_transactions, processed_blocks)),
        "min_transactions_in_block": min_transactions_in_block if min_transactions_in_block is not None else 0,
        "block_with_min_transactions": block_with_min_transactions,
        "max_transactions_in_block": max_transactions_in_block if max_transactions_in_block is not None else 0,
        "block_with_max_transactions": block_with_max_transactions,
        "total_value_eth": round_value(total_value_eth),
        "avg_value_eth_per_transaction": round_value(safe_average(total_value_eth, total_transactions)),
        "zero_value_transactions": zero_value_transactions,
        "zero_value_transaction_ratio": round_value(safe_average(zero_value_transactions, total_transactions)),
        "total_gas": total_gas,
        "avg_gas_per_transaction": round_value(safe_average(total_gas, total_transactions)),
        "avg_gas_price": round_value(safe_average(total_gas_price, total_transactions)),
        "latest_gas_price": latest_gas_price,
        "min_gas_price": min_gas_price if min_gas_price is not None else 0,
        "max_gas_price": max_gas_price if max_gas_price is not None else 0,
        "unique_senders": len(sender_counter),
        "unique_receivers": len(receiver_counter),
        "top_senders": top_addresses(sender_counter),
        "top_receivers": top_addresses(receiver_counter),
        "per_block_stats": per_block_stats,
    }


def aggregate_to_json(filtered_blocks_dir: Path | str = "filtered_blocks") -> str:
    """Serializes aggregated statistics to JSON."""
    aggregated_stats = aggregate_filtered_blocks(filtered_blocks_dir)
    return json.dumps(aggregated_stats, indent=2, ensure_ascii=False)


def save_aggregated_stats(
    filtered_blocks_dir: Path | str = "filtered_blocks",
    output_path: Path | str = "aggregated_stats.json",
) -> Path:
    """Saves aggregated statistics to a JSON file."""
    output_file = Path(output_path)
    output_file.write_text(aggregate_to_json(filtered_blocks_dir), encoding="utf-8")
    return output_file
