import json
import datetime
from unittest.mock import patch, mock_open, MagicMock
from reportlab.platypus import Paragraph, Table
from reportlab.lib.styles import ParagraphStyle

import pytest

from file_report import (
    short_addr,
    format_num,
    create_title,
    create_section_header,
    create_basic_stats_table,
    build_findings,
    generate_blockchain_report_pdf,
    startReport
)


@pytest.fixture
def sample_report_data():
    """Fixture dostarczający przykładowe dane do testów."""
    return {
        "processed_blocks": 100,
        "total_transactions": 5000,
        "avg_transactions_per_block": 50.0,
        "min_transactions_in_block": 1,
        "block_with_min_transactions": 10,
        "max_transactions_in_block": 200,
        "block_with_max_transactions": 99,
        "total_value_eth": 1250.555,
        "avg_value_eth_per_transaction": 0.25,
        "zero_value_transactions": 1000,
        "zero_value_transaction_ratio": 0.20,
        "total_gas": 15000000,
        "avg_gas_per_transaction": 3000.0,
        "avg_gas_price": 50.5,
        "min_gas_price": 10,
        "max_gas_price": 100,
        "latest_gas_price": 45,
        "unique_senders": 150,
        "unique_receivers": 200,
        "top_senders": [
            {"address": "0x1234567890abcdef1234567890abcdef12345678", "transaction_count": 150}
        ],
        "top_receivers": [
            {"address": "0xabcdef1234567890abcdef1234567890abcdef12", "transaction_count": 80}
        ],
        "per_block_stats": [
            {
                "block_number": 10,
                "transaction_count": 1,
                "total_value_eth": 0.05,
                "zero_value_transaction_ratio": 0.0,
                "avg_gas_per_transaction": 21000,
                "avg_gas_price": 50,
                "min_gas_price": 50,
                "max_gas_price": 50
            }
        ]
    }


def test_short_addr_truncates_long_string():
    addr = "0x1234567890abcdef1234567890abcdef"
    assert short_addr(addr) == "0x123456...abcdef"


def test_short_addr_custom_lengths():
    addr = "0x1234567890abcdef"
    assert short_addr(addr, left=4, right=4) == "0x12...cdef"


def test_short_addr_returns_full_if_short():
    addr = "0x123456"
    assert short_addr(addr, left=5, right=5) == "0x123456"


def test_format_num_formats_correctly():
    assert format_num(1234567.891, decimals=2) == "1 234 567.89"
    assert format_num(1000, decimals=0) == "1 000"


def test_build_findings_returns_correct_strings(sample_report_data):
    findings = build_findings(sample_report_data)

    assert len(findings) == 6
    assert "100 blocks" in findings[0]
    assert "5000 transactions" in findings[0]
    assert "least active block was 10" in findings[1]
    assert "20.00% of all operations" in findings[3]
    assert "150 unique senders" in findings[4]


def test_create_basic_stats_table_generates_table(sample_report_data):
    table = create_basic_stats_table(sample_report_data)

    assert isinstance(table, Table)
    assert table._cellvalues[0][0] == "Metric"
    assert table._cellvalues[1][0] == "Processed blocks"
    assert table._cellvalues[1][1] == "100"


def test_create_section_header():
    real_style = ParagraphStyle(name="SectionHeader", fontName="Helvetica", fontSize=12)
    styles = {"SectionHeader": real_style}

    paragraph = create_section_header("Test Header", styles)

    assert isinstance(paragraph, Paragraph)
    assert paragraph.text == "Test Header"


@patch('file_report.SimpleDocTemplate')
def test_generate_blockchain_report_pdf_builds_document(mock_doc_template, sample_report_data):
    mock_doc_instance = MagicMock()
    mock_doc_template.return_value = mock_doc_instance

    output_path = "test_output.pdf"

    result_path = generate_blockchain_report_pdf(sample_report_data, output_path)

    mock_doc_template.assert_called_once()
    assert mock_doc_template.call_args[0][0] == output_path

    mock_doc_instance.build.assert_called_once()
    elements = mock_doc_instance.build.call_args[0][0]
    assert isinstance(elements, list)
    assert len(elements) > 10
    assert result_path == output_path


@patch('file_report.datetime')
@patch('file_report.Path')
@patch('file_report.generate_blockchain_report_pdf')
@patch('builtins.open', new_callable=mock_open, read_data='{"processed_blocks": 10}')
@patch('json.load')
def test_startReport_executes_flow(mock_json_load, mock_file, mock_generate, mock_path, mock_datetime):
    mock_data = {"processed_blocks": 10}
    mock_json_load.return_value = mock_data

    mock_now = datetime.datetime(2026, 5, 17, 15, 30, 0)
    mock_datetime.datetime.now.return_value = mock_now

    startReport()

    mock_file.assert_called_once_with("aggregated_stats.json", "r", encoding="utf-8")
    mock_path.return_value.mkdir.assert_called_once_with(parents=True, exist_ok=True)

    expected_filename = "Reports/raport_blockchain_2026-05-17_15-30-00.pdf"
    mock_generate.assert_called_once_with(mock_data, expected_filename)