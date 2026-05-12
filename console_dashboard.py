import asyncio
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.console import Console, Group

console = Console()


def create_overview_table(stats):
    """Summarizes network activity and costs."""
    table = Table(
        expand=True, border_style="cyan", title="[bold]Network Summary[/bold]"
    )
    table.add_column("Blocks Processed", justify="center", style="bold green")
    table.add_column("Total Transactions (TX)", justify="center", style="bold magenta")
    table.add_column("Avg Gas Price (Gwei)", justify="center", style="bold yellow")

    avg_gas = stats.get("avg_gas_price", 0)
    display_gas = f"{avg_gas / 1e9:.2f}" if avg_gas > 1000000 else f"{avg_gas:.2f}"

    table.add_row(
        str(stats["processed_blocks"]), str(stats["total_transactions"]), display_gas
    )
    return table


def create_financial_table(stats):
    """Details the flow of ETH and transaction types."""
    table = Table(
        expand=True, border_style="green", title="[bold]Financial Analysis[/bold]"
    )
    table.add_column("Total ETH Volume", justify="center", style="bold blue")
    table.add_column("Avg TX Value", justify="center")
    table.add_column("Biggest TX", justify="center", style="bold blue")
    table.add_column("Zero-Value TXs", justify="center", style="dim")
    table.add_column("Zero-Value Ratio", justify="center", style="dim")

    ratio = stats.get("zero_value_transaction_ratio", 0) * 100
    table.add_row(
        f"{stats['total_value_eth']:.4f} ETH",
        f"{stats['avg_value_eth_per_transaction']:.6f} ETH",
        f"{stats.get('max_tx_value', 0):.4f} ETH",
        str(stats["zero_value_transactions"]),
        f"{ratio:.1f}%",
    )
    return table


def create_process_block_table(stats):
    """Shows the dashboard of the most recent blocks."""
    table = Table(
        expand=True, border_style="magenta", title="[bold]Recent Block Pulse[/bold]"
    )
    table.add_column("Block #", justify="center")
    table.add_column("TXs", justify="center")
    table.add_column("Value (ETH)", justify="center")
    table.add_column("Zero-Value TXs", justify="center", style="dim")

    for b_stats in stats.get("per_block_stats", [])[-3:]:
        table.add_row(
            str(b_stats["block_number"]),
            str(b_stats["transaction_count"]),
            f"{b_stats['total_value_eth']:.4f}",
            str(b_stats["zero_value_transactions"]),
        )
    return table


def generate_dashboard(state):
    """Assembles all components into the final layout."""
    if not state.latest_stats:
        return Panel(
            "Initializing Monitor & Aggregating Data...",
            border_style="yellow",
            title="Status",
        )

    return Group(
        create_overview_table(state.latest_stats),
        create_financial_table(state.latest_stats),
        create_process_block_table(state.latest_stats),
        Panel(
            "\n".join(state.logs),
            title="[bold]Live Activity Feed[/bold]",
            border_style="blue",
            height=20,
        ),
    )


async def display_ui(state):
    """The main UI loop using Rich Live."""
    with Live(generate_dashboard(state), refresh_per_second=4, screen=True) as live:
        while state.is_running:
            live.update(generate_dashboard(state))
            await asyncio.sleep(0.1)
