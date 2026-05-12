import datetime
import json

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    PageBreak
)
from reportlab.graphics.shapes import Drawing, String
from reportlab.graphics.charts.barcharts import VerticalBarChart, HorizontalBarChart
from reportlab.graphics.charts.lineplots import LinePlot
from reportlab.graphics.charts.piecharts import Pie


def short_addr(addr: str, left=8, right=6) -> str:
    if len(addr) <= left + right:
        return addr
    return f"{addr[:left]}...{addr[-right:]}"


def format_num(value, decimals=2):
    return f"{value:,.{decimals}f}".replace(",", " ")


def create_title(text, styles):
    return Paragraph(text, styles["TitleCenter"])


def create_section_header(text, styles):
    return Paragraph(text, styles["SectionHeader"])


def create_basic_stats_table(data):
    rows = [
        ["Metric", "Value"],
        ["Processed blocks", str(data["processed_blocks"])],
        ["Total transactions", str(data["total_transactions"])],
        ["Average transactions per block", format_num(data["avg_transactions_per_block"])],
        ["Minimum transactions in a block", f'{data["min_transactions_in_block"]} (block {data["block_with_min_transactions"]})'],
        ["Maximum transactions in a block", f'{data["max_transactions_in_block"]} (block {data["block_with_max_transactions"]})'],
        ["Total ETH value", format_num(data["total_value_eth"], 6)],
        ["Average ETH value per transaction", format_num(data["avg_value_eth_per_transaction"], 6)],
        ["Zero-value transactions", str(data["zero_value_transactions"])],
        ["Zero-value transaction ratio", f'{data["zero_value_transaction_ratio"] * 100:.2f}%'],
        ["Total gas", format_num(data["total_gas"], 0)],
        ["Average gas per transaction", format_num(data["avg_gas_per_transaction"], 2)],
        ["Average gas price", format_num(data["avg_gas_price"], 2)],
        ["Minimum gas price", str(data["min_gas_price"])],
        ["Maximum gas price", str(data["max_gas_price"])],
        ["Latest gas price", str(data["latest_gas_price"])],
        ["Unique senders", str(data["unique_senders"])],
        ["Unique receivers", str(data["unique_receivers"])],
    ]

    table = Table(rows, colWidths=[8.2 * cm, 8.3 * cm], repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1F4E78")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.HexColor("#EAF2F8")]),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    return table


def create_top_addresses_table(items):
    rows = [["Address", "Transaction count"]]
    for item in items:
        rows.append([item["address"], str(item["transaction_count"])])

    table = Table(rows, colWidths=[12.5 * cm, 4 * cm], repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2E75B6")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F7FBFF")]),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    return table


def build_tx_per_block_chart(per_block_stats):
    labels = [str(x["block_number"]) for x in per_block_stats]
    values = [x["transaction_count"] for x in per_block_stats]

    drawing = Drawing(520, 260)
    drawing.add(String(150, 235, "Number of transactions by block", fontSize=13))

    chart = VerticalBarChart()
    chart.x = 45
    chart.y = 55
    chart.width = 430
    chart.height = 150
    chart.data = [values]
    chart.categoryAxis.categoryNames = labels
    chart.categoryAxis.labels.angle = 35
    chart.categoryAxis.labels.dy = -12
    chart.categoryAxis.labels.fontSize = 7
    chart.valueAxis.valueMin = 0
    chart.valueAxis.valueMax = max(values) + 20
    chart.valueAxis.valueStep = 20
    chart.valueAxis.labels.fontSize = 8
    chart.bars[0].fillColor = colors.HexColor("#4F81BD")

    drawing.add(chart)
    return drawing


def build_eth_per_block_chart(per_block_stats):
    labels = [str(x["block_number"]) for x in per_block_stats]
    values = [x["total_value_eth"] for x in per_block_stats]

    points = list(enumerate(values))
    drawing = Drawing(520, 260)
    drawing.add(String(170, 235, "Total ETH value by block", fontSize=13))

    chart = LinePlot()
    chart.x = 45
    chart.y = 55
    chart.width = 430
    chart.height = 150
    chart.data = [points]
    chart.lines[0].strokeColor = colors.HexColor("#70AD47")
    chart.lines[0].strokeWidth = 2
    chart.xValueAxis.valueMin = 0
    chart.xValueAxis.valueMax = len(values) - 1
    chart.xValueAxis.valueStep = 1
    chart.yValueAxis.valueMin = 0
    chart.yValueAxis.valueMax = max(values) + 0.5
    chart.yValueAxis.valueStep = max(0.5, round(max(values) / 5, 2))
    chart.xValueAxis.labels.fontSize = 7
    chart.yValueAxis.labels.fontSize = 8

    drawing.add(chart)

    x_start = 45
    step = 430 / max(1, len(labels) - 1)
    for i, label in enumerate(labels):
        drawing.add(String(x_start + i * step - 8, 40, label[-4:], fontSize=6, angle=35))

    return drawing


def build_zero_value_pie(data):
    zero_pct = round(data["zero_value_transaction_ratio"] * 100, 2)
    non_zero_pct = round(100 - zero_pct, 2)

    drawing = Drawing(420, 250)
    drawing.add(String(130, 225, "Share of zero-value transactions", fontSize=13))

    pie = Pie()
    pie.x = 110
    pie.y = 45
    pie.width = 170
    pie.height = 170
    pie.data = [zero_pct, non_zero_pct]
    pie.labels = [f"Zero-value: {zero_pct}%", f"Non-zero: {non_zero_pct}%"]
    pie.sideLabels = True
    pie.slices[0].fillColor = colors.HexColor("#C0504D")
    pie.slices[1].fillColor = colors.HexColor("#9BBB59")

    drawing.add(pie)
    return drawing


def build_horizontal_bar_chart(items, chart_title, color_hex):
    labels = [short_addr(x["address"]) for x in items]
    values = [x["transaction_count"] for x in items]

    drawing = Drawing(520, 250)
    drawing.add(String(155, 225, chart_title, fontSize=13))

    chart = HorizontalBarChart()
    chart.x = 140
    chart.y = 45
    chart.width = 300
    chart.height = 135
    chart.data = [values]
    chart.categoryAxis.categoryNames = labels
    chart.categoryAxis.labels.fontSize = 8
    chart.valueAxis.valueMin = 0
    chart.valueAxis.valueMax = max(values) + 20
    chart.valueAxis.valueStep = max(5, int((max(values) + 20) / 5))
    chart.valueAxis.labels.fontSize = 8
    chart.bars[0].fillColor = colors.HexColor(color_hex)

    drawing.add(chart)
    return drawing


def create_block_stats_table(per_block_stats):
    rows = [[
        "Block",
        "Tx",
        "ETH",
        "Zero %",
        "Avg gas",
        "Avg gas price",
        "Min gas",
        "Max gas"
    ]]

    for b in per_block_stats:
        rows.append([
            str(b["block_number"]),
            str(b["transaction_count"]),
            format_num(b["total_value_eth"], 4),
            f'{b["zero_value_transaction_ratio"] * 100:.2f}%',
            format_num(b["avg_gas_per_transaction"], 2),
            format_num(b["avg_gas_price"], 2),
            str(b["min_gas_price"]),
            str(b["max_gas_price"]),
        ])

    table = Table(
        rows,
        colWidths=[2.3 * cm, 1.2 * cm, 1.8 * cm, 1.8 * cm, 2.4 * cm, 2.7 * cm, 2.1 * cm, 2.1 * cm],
        repeatRows=1
    )
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1F4E78")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 7.3),
        ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.HexColor("#F9F9F9")]),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    return table


def build_findings(data):
    findings = []
    findings.append(
        f"The analysis covered {data['processed_blocks']} blocks containing a total of {data['total_transactions']} transactions, "
        f"with an average of {format_num(data['avg_transactions_per_block'])} transactions per block."
    )
    findings.append(
        f"The least active block was {data['block_with_min_transactions']} with {data['min_transactions_in_block']} transactions, "
        f"while the most active block was {data['block_with_max_transactions']} with {data['max_transactions_in_block']} transactions."
    )
    findings.append(
        f"The total transferred value reached {format_num(data['total_value_eth'], 6)} ETH, "
        f"and the average transaction value was {format_num(data['avg_value_eth_per_transaction'], 6)} ETH."
    )
    findings.append(
        f"Zero-value transactions accounted for {data['zero_value_transaction_ratio'] * 100:.2f}% of all operations "
        f"({data['zero_value_transactions']} transactions), indicating a strong dominance of non-value ETH transfers."
    )
    findings.append(
        f"The dataset included {data['unique_senders']} unique senders and {data['unique_receivers']} unique receivers."
    )
    findings.append(
        f"The average gas price was {format_num(data['avg_gas_price'], 2)}, "
        f"with a minimum of {data['min_gas_price']} and a maximum of {data['max_gas_price']}."
    )
    return findings


def generate_blockchain_report_pdf(report_data: dict, output_path: str = "blockchain_report.pdf"):
    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        name="TitleCenter",
        parent=styles["Title"],
        alignment=TA_CENTER,
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=22,
        textColor=colors.HexColor("#17375E"),
        spaceAfter=10
    ))

    styles.add(ParagraphStyle(
        name="SectionHeader",
        parent=styles["Heading2"],
        alignment=TA_LEFT,
        fontName="Helvetica-Bold",
        fontSize=13,
        leading=16,
        textColor=colors.HexColor("#1F4E78"),
        spaceBefore=8,
        spaceAfter=8
    ))

    styles.add(ParagraphStyle(
        name="BodyTextCustom",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=10,
        leading=14,
        spaceAfter=5
    ))

    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        rightMargin=1.5 * cm,
        leftMargin=1.5 * cm,
        topMargin=1.5 * cm,
        bottomMargin=1.5 * cm
    )

    elements = []

    elements.append(create_title("Blockchain Transaction Analysis Report", styles))
    elements.append(Paragraph(
        "This document presents key statistics, main findings and charts describing transaction activity in the analyzed blocks.",
        styles["BodyTextCustom"]
    ))
    elements.append(Spacer(1, 0.3 * cm))

    elements.append(create_section_header("1. Basic statistics", styles))
    elements.append(create_basic_stats_table(report_data))
    elements.append(Spacer(1, 0.4 * cm))

    elements.append(create_section_header("2. Key findings", styles))
    for idx, finding in enumerate(build_findings(report_data), start=1):
        elements.append(Paragraph(f"{idx}. {finding}", styles["BodyTextCustom"]))

    elements.append(Spacer(1, 0.25 * cm))
    elements.append(create_section_header("3. Top senders", styles))
    elements.append(create_top_addresses_table(report_data["top_senders"]))
    elements.append(Spacer(1, 0.25 * cm))

    elements.append(create_section_header("4. Top receivers", styles))
    elements.append(create_top_addresses_table(report_data["top_receivers"]))

    elements.append(PageBreak())
    elements.append(create_section_header("5. Charts", styles))

    elements.append(Paragraph(
        "The following charts present the distribution of transaction counts, ETH values, the share of zero-value transactions and the activity of the most frequent addresses.",
        styles["BodyTextCustom"]
    ))
    elements.append(Spacer(1, 0.2 * cm))

    elements.append(Paragraph("5.1. Number of transactions by block", styles["BodyTextCustom"]))
    elements.append(build_tx_per_block_chart(report_data["per_block_stats"]))
    elements.append(Spacer(1, 0.2 * cm))

    elements.append(Paragraph("5.2. Total ETH value by block", styles["BodyTextCustom"]))
    elements.append(build_eth_per_block_chart(report_data["per_block_stats"]))
    elements.append(PageBreak())

    elements.append(create_section_header("6. Additional charts", styles))
    elements.append(Paragraph("6.1. Share of zero-value transactions", styles["BodyTextCustom"]))
    elements.append(build_zero_value_pie(report_data))
    elements.append(Spacer(1, 0.2 * cm))

    elements.append(Paragraph("6.2. Top 5 senders", styles["BodyTextCustom"]))
    elements.append(build_horizontal_bar_chart(
        report_data["top_senders"],
        "Top 5 senders by transaction count",
        "#ED7D31"
    ))
    elements.append(Spacer(1, 0.2 * cm))

    elements.append(Paragraph("6.3. Top 5 receivers", styles["BodyTextCustom"]))
    elements.append(build_horizontal_bar_chart(
        report_data["top_receivers"],
        "Top 5 receivers by transaction count",
        "#5B9BD5"
    ))

    elements.append(PageBreak())
    elements.append(create_section_header("7. Detailed per-block statistics", styles))
    elements.append(Paragraph(
        "The table below contains a detailed summary of parameters for each analyzed block.",
        styles["BodyTextCustom"]
    ))
    elements.append(create_block_stats_table(report_data["per_block_stats"]))

    doc.build(elements)
    return output_path
def startReport():
    with open("aggregated_stats.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    currentDate = datetime.datetime.now()
    correctedDate = currentDate.strftime("%Y-%m-%d_%H-%M-%S")

    pdf_file = generate_blockchain_report_pdf(data, "Reports/raport_blockchain_"+correctedDate+".pdf")
    print(f"Utworzono plik: {pdf_file}")