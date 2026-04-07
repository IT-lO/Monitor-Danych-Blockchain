import json

from hexbytes import HexBytes

from data_filter import (
    filter_block_data,
    filter_block_with_transactions,
    filter_transaction_data,
    filtered_block_to_json,
    normalize_hex,
    wei_to_eth,
)


def test_normalize_hex_converts_hexbytes_to_string():
    value = HexBytes("0x1234abcd")

    result = normalize_hex(value)

    assert result == "1234abcd"


def test_normalize_hex_keeps_none():
    assert normalize_hex(None) is None


def test_wei_to_eth_converts_value_to_eth():
    result = wei_to_eth(5_000_000_000_000_000)

    assert result == 0.005


def test_filter_block_data_returns_expected_fields():
    block = {
        "number": 123,
        "hash": HexBytes("0xabc123"),
        "transactions": [HexBytes("0x01"), HexBytes("0x02")],
        "gasUsed": 999,
    }

    result = filter_block_data(block)

    assert result == {
        "block_number": 123,
        "block_hash": "abc123",
        "transaction_count": 2,
    }


def test_filter_transaction_data_returns_expected_fields():
    transaction = {
        "hash": HexBytes("0xdeadbeef"),
        "from": "0xSender",
        "to": "0xReceiver",
        "value": 2_000_000_000_000_000,
        "gas": 21_000,
        "gasPrice": 1_500_000_018,
        "input": "ignored",
    }

    result = filter_transaction_data(transaction)

    assert result == {
        "transaction_hash": "deadbeef",
        "from_address": "0xSender",
        "to_address": "0xReceiver",
        "value_eth": 0.002,
        "gas": 21_000,
        "gas_price": 1_500_000_018,
    }


def test_filter_block_with_transactions_builds_full_filtered_structure():
    block = {
        "number": 456,
        "hash": HexBytes("0xbeef"),
        "transactions": [
            {
                "hash": HexBytes("0x01"),
                "from": "0xA",
                "to": "0xB",
                "value": 0,
                "gas": 50_000,
                "gasPrice": 123,
            }
        ],
    }

    result = filter_block_with_transactions(block)

    assert result == {
        "block": {
            "block_number": 456,
            "block_hash": "beef",
            "transaction_count": 1,
        },
        "transactions": [
            {
                "transaction_hash": "01",
                "from_address": "0xA",
                "to_address": "0xB",
                "value_eth": 0.0,
                "gas": 50_000,
                "gas_price": 123,
            }
        ],
    }


def test_filtered_block_to_json_returns_valid_json():
    block = {
        "number": 789,
        "hash": HexBytes("0xcafe"),
        "transactions": [
            {
                "hash": HexBytes("0x10"),
                "from": "0xFrom",
                "to": "0xTo",
                "value": 1_000_000_000_000_000_000,
                "gas": 21_000,
                "gasPrice": 2_000_000_000,
            }
        ],
    }

    result = filtered_block_to_json(block)
    parsed = json.loads(result)

    assert parsed["block"]["block_number"] == 789
    assert parsed["block"]["block_hash"] == "cafe"
    assert parsed["block"]["transaction_count"] == 1
    assert parsed["transactions"][0]["transaction_hash"] == "10"
    assert parsed["transactions"][0]["value_eth"] == 1.0
