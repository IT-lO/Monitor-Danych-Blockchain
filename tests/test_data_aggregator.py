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
        "latest_gas_price": 90,
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
        "per_block_stats": [
            {
                "block_number": 100,
                "block_hash": "aaa",
                "transaction_count": 2,
                "total_value_eth": 1.5,
                "avg_value_eth_per_transaction": 0.75,
                "zero_value_transactions": 1,
                "zero_value_transaction_ratio": 0.5,
                "total_gas": 71000,
                "avg_gas_per_transaction": 35500.0,
                "avg_gas_price": 110.0,
                "min_gas_price": 100,
                "max_gas_price": 120,
            },
            {
                "block_number": 101,
                "block_hash": "bbb",
                "transaction_count": 1,
                "total_value_eth": 0.25,
                "avg_value_eth_per_transaction": 0.25,
                "zero_value_transactions": 0,
                "zero_value_transaction_ratio": 0.0,
                "total_gas": 30000,
                "avg_gas_per_transaction": 30000.0,
                "avg_gas_price": 90.0,
                "min_gas_price": 90,
                "max_gas_price": 90,
            },
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
        "latest_gas_price": 0,
        "min_gas_price": 0,
        "max_gas_price": 0,
        "unique_senders": 0,
        "unique_receivers": 0,
        "top_senders": [],
        "top_receivers": [],
        "per_block_stats": [],
    }


def test_aggregate_filtered_blocks_uses_latest_gas_price_from_last_transaction(tmp_path):
    filtered_blocks_dir = tmp_path / "filtered_blocks"
    filtered_blocks_dir.mkdir()

    block = {
        "block": {
            "block_number": 300,
            "block_hash": "ccc",
            "transaction_count": 3,
        },
        "transactions": [
            {
                "transaction_hash": "tx1",
                "from_address": "0xA",
                "to_address": "0xB",
                "value_eth": 1.0,
                "gas": 21_000,
                "gas_price": 100,
            },
            {
                "transaction_hash": "tx2",
                "from_address": "0xC",
                "to_address": "0xD",
                "value_eth": 0.5,
                "gas": 30_000,
                "gas_price": 200,
            },
            {
                "transaction_hash": "tx3",
                "from_address": "0xE",
                "to_address": "0xF",
                "value_eth": 0.25,
                "gas": 40_000,
                "gas_price": 300,
            },
        ],
    }

    (filtered_blocks_dir / "block_300.json").write_text(json.dumps(block), encoding="utf-8")

    result = aggregate_filtered_blocks(filtered_blocks_dir)

    assert result["latest_gas_price"] == 300


def test_aggregate_filtered_blocks_uses_latest_gas_price_from_highest_block_number(tmp_path):
    filtered_blocks_dir = tmp_path / "filtered_blocks"
    filtered_blocks_dir.mkdir()

    older_block = {
        "block": {
            "block_number": 9,
            "block_hash": "old",
            "transaction_count": 1,
        },
        "transactions": [
            {
                "transaction_hash": "tx-old",
                "from_address": "0xA",
                "to_address": "0xB",
                "value_eth": 1.0,
                "gas": 21_000,
                "gas_price": 999,
            }
        ],
    }

    newer_block = {
        "block": {
            "block_number": 10,
            "block_hash": "new",
            "transaction_count": 1,
        },
        "transactions": [
            {
                "transaction_hash": "tx-new",
                "from_address": "0xC",
                "to_address": "0xD",
                "value_eth": 2.0,
                "gas": 30_000,
                "gas_price": 111,
            }
        ],
    }

    (filtered_blocks_dir / "block_9.json").write_text(json.dumps(older_block), encoding="utf-8")
    (filtered_blocks_dir / "block_10.json").write_text(json.dumps(newer_block), encoding="utf-8")

    result = aggregate_filtered_blocks(filtered_blocks_dir)

    assert result["latest_gas_price"] == 111


def test_aggregate_filtered_blocks_keeps_latest_gas_price_when_newest_block_is_empty(tmp_path):
    filtered_blocks_dir = tmp_path / "filtered_blocks"
    filtered_blocks_dir.mkdir()

    older_block = {
        "block": {
            "block_number": 20,
            "block_hash": "older",
            "transaction_count": 1,
        },
        "transactions": [
            {
                "transaction_hash": "tx-older",
                "from_address": "0xA",
                "to_address": "0xB",
                "value_eth": 1.0,
                "gas": 21_000,
                "gas_price": 555,
            }
        ],
    }

    newer_empty_block = {
        "block": {
            "block_number": 21,
            "block_hash": "newer",
            "transaction_count": 0,
        },
        "transactions": [],
    }

    (filtered_blocks_dir / "block_20.json").write_text(json.dumps(older_block), encoding="utf-8")
    (filtered_blocks_dir / "block_21.json").write_text(json.dumps(newer_empty_block), encoding="utf-8")

    result = aggregate_filtered_blocks(filtered_blocks_dir)

    assert result["latest_gas_price"] == 555


def test_aggregate_filtered_blocks_returns_per_block_stats_for_each_block(tmp_path):
    filtered_blocks_dir = tmp_path / "filtered_blocks"
    filtered_blocks_dir.mkdir()

    first_block = {
        "block": {
            "block_number": 400,
            "block_hash": "ddd",
            "transaction_count": 1,
        },
        "transactions": [
            {
                "transaction_hash": "tx1",
                "from_address": "0xA",
                "to_address": "0xB",
                "value_eth": 2.0,
                "gas": 25_000,
                "gas_price": 150,
            }
        ],
    }

    second_block = {
        "block": {
            "block_number": 401,
            "block_hash": "eee",
            "transaction_count": 2,
        },
        "transactions": [
            {
                "transaction_hash": "tx2",
                "from_address": "0xC",
                "to_address": "0xD",
                "value_eth": 0.0,
                "gas": 20_000,
                "gas_price": 90,
            },
            {
                "transaction_hash": "tx3",
                "from_address": "0xE",
                "to_address": "0xF",
                "value_eth": 1.0,
                "gas": 50_000,
                "gas_price": 110,
            },
        ],
    }

    (filtered_blocks_dir / "block_400.json").write_text(json.dumps(first_block), encoding="utf-8")
    (filtered_blocks_dir / "block_401.json").write_text(json.dumps(second_block), encoding="utf-8")

    result = aggregate_filtered_blocks(filtered_blocks_dir)

    assert result["per_block_stats"] == [
        {
            "block_number": 400,
            "block_hash": "ddd",
            "transaction_count": 1,
            "total_value_eth": 2.0,
            "avg_value_eth_per_transaction": 2.0,
            "zero_value_transactions": 0,
            "zero_value_transaction_ratio": 0.0,
            "total_gas": 25000,
            "avg_gas_per_transaction": 25000.0,
            "avg_gas_price": 150.0,
            "min_gas_price": 150,
            "max_gas_price": 150,
        },
        {
            "block_number": 401,
            "block_hash": "eee",
            "transaction_count": 2,
            "total_value_eth": 1.0,
            "avg_value_eth_per_transaction": 0.5,
            "zero_value_transactions": 1,
            "zero_value_transaction_ratio": 0.5,
            "total_gas": 70000,
            "avg_gas_per_transaction": 35000.0,
            "avg_gas_price": 100.0,
            "min_gas_price": 90,
            "max_gas_price": 110,
        },
    ]


def test_aggregate_filtered_blocks_ignores_empty_addresses_in_counters(tmp_path):
    filtered_blocks_dir = tmp_path / "filtered_blocks"
    filtered_blocks_dir.mkdir()

    block = {
        "block": {
            "block_number": 500,
            "block_hash": "fff",
            "transaction_count": 2,
        },
        "transactions": [
            {
                "transaction_hash": "tx1",
                "from_address": None,
                "to_address": "0xB",
                "value_eth": 0.0,
                "gas": 21_000,
                "gas_price": 100,
            },
            {
                "transaction_hash": "tx2",
                "from_address": "0xA",
                "to_address": None,
                "value_eth": 1.0,
                "gas": 30_000,
                "gas_price": 120,
            },
        ],
    }

    (filtered_blocks_dir / "block_500.json").write_text(json.dumps(block), encoding="utf-8")

    result = aggregate_filtered_blocks(filtered_blocks_dir)

    assert result["unique_senders"] == 1
    assert result["unique_receivers"] == 1
    assert result["top_senders"] == [{"address": "0xA", "transaction_count": 1}]
    assert result["top_receivers"] == [{"address": "0xB", "transaction_count": 1}]


def test_aggregate_filtered_blocks_handles_block_without_transactions(tmp_path):
    filtered_blocks_dir = tmp_path / "filtered_blocks"
    filtered_blocks_dir.mkdir()

    empty_block = {
        "block": {
            "block_number": 600,
            "block_hash": "ggg",
            "transaction_count": 0,
        },
        "transactions": [],
    }

    (filtered_blocks_dir / "block_600.json").write_text(json.dumps(empty_block), encoding="utf-8")

    result = aggregate_filtered_blocks(filtered_blocks_dir)

    assert result["processed_blocks"] == 1
    assert result["total_transactions"] == 0
    assert result["avg_transactions_per_block"] == 0.0
    assert result["latest_gas_price"] == 0
    assert result["per_block_stats"] == [
        {
            "block_number": 600,
            "block_hash": "ggg",
            "transaction_count": 0,
            "total_value_eth": 0.0,
            "avg_value_eth_per_transaction": 0.0,
            "zero_value_transactions": 0,
            "zero_value_transaction_ratio": 0.0,
            "total_gas": 0,
            "avg_gas_per_transaction": 0.0,
            "avg_gas_price": 0.0,
            "min_gas_price": 0,
            "max_gas_price": 0,
        }
    ]
