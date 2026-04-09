import json

from data_aggregator import aggregate_filtered_blocks

# tmp_path is a pytest thing for temporary test files and directories.
def test_aggregate_filtered_blocks_returns_expected_statistics(tmp_path):
    filtered_blocks_dir = tmp_path / "filtered_blocks"
    filtered_blocks_dir.mkdir()

    first_block = {
        "block": {
            "block_number": 100,
            "block_hash": "aaa",
            "transaction_count": 2,
        },
        "transactions": [
            {
                "transaction_hash": "tx1",
                "from_address": "0xA",
                "to_address": "0xB",
                "value_eth": 1.5,
                "gas": 21_000,
                "gas_price": 100,
            },
            {
                "transaction_hash": "tx2",
                "from_address": "0xA",
                "to_address": "0xC",
                "value_eth": 0.0,
                "gas": 50_000,
                "gas_price": 120,
            },
        ],
    }

    second_block = {
        "block": {
            "block_number": 101,
            "block_hash": "bbb",
            "transaction_count": 1,
        },
        "transactions": [
            {
                "transaction_hash": "tx3",
                "from_address": "0xD",
                "to_address": "0xB",
                "value_eth": 0.25,
                "gas": 30_000,
                "gas_price": 90,
            }
        ],
    }

    (filtered_blocks_dir / "block_100.json").write_text(json.dumps(first_block), encoding="utf-8")
    (filtered_blocks_dir / "block_101.json").write_text(json.dumps(second_block), encoding="utf-8")

    result = aggregate_filtered_blocks(filtered_blocks_dir)

    assert result == {
        "processed_blocks": 2,
        "total_transactions": 3,
        "avg_transactions_per_block": 1.5,
        "min_transactions_in_block": 1,
        "block_with_min_transactions": 101,
        "max_transactions_in_block": 2,
        "block_with_max_transactions": 100,
        "total_value_eth": 1.75,
        "avg_value_eth_per_transaction": 0.58333333,
        "zero_value_transactions": 1,
        "zero_value_transaction_ratio": 0.33333333,
        "total_gas": 101000,
        "avg_gas_per_transaction": 33666.66666667,
        "avg_gas_price": 103.33333333,
        "min_gas_price": 90,
        "max_gas_price": 120,
        "unique_senders": 2,
        "unique_receivers": 2,
        "top_senders": [
            {"address": "0xA", "transaction_count": 2},
            {"address": "0xD", "transaction_count": 1},
        ],
        "top_receivers": [
            {"address": "0xB", "transaction_count": 2},
            {"address": "0xC", "transaction_count": 1},
        ],
    }


def test_aggregate_filtered_blocks_handles_empty_directory(tmp_path):
    filtered_blocks_dir = tmp_path / "filtered_blocks"
    filtered_blocks_dir.mkdir()

    result = aggregate_filtered_blocks(filtered_blocks_dir)

    assert result == {
        "processed_blocks": 0,
        "total_transactions": 0,
        "avg_transactions_per_block": 0.0,
        "min_transactions_in_block": 0,
        "block_with_min_transactions": None,
        "max_transactions_in_block": 0,
        "block_with_max_transactions": None,
        "total_value_eth": 0.0,
        "avg_value_eth_per_transaction": 0.0,
        "zero_value_transactions": 0,
        "zero_value_transaction_ratio": 0.0,
        "total_gas": 0,
        "avg_gas_per_transaction": 0.0,
        "avg_gas_price": 0.0,
        "min_gas_price": 0,
        "max_gas_price": 0,
        "unique_senders": 0,
        "unique_receivers": 0,
        "top_senders": [],
        "top_receivers": [],
    }
