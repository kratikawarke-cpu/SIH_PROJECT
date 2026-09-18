import json
import time
import uuid
from collections import defaultdict, deque

from kafka import KafkaConsumer, KafkaProducer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from datetime import datetime
from zoneinfo import ZoneInfo


# ============================================================
# CONFIG
# ============================================================

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "bank_transactions"
KAFKA_GROUP = "cyber-sih-detector-working-ui"
OUTPUT_TOPIC = "detected_scenarios"

MAX_ACCOUNTS = 14

# A scenario only gets surfaced once real chaining has happened —
# not the moment money leaves the source. This keeps the Scenarios
# feed from lighting up on every single transaction; it waits for
# the chain to actually grow into something worth flagging.
MIN_SCENARIO_ACCOUNTS = 7
MIN_SCENARIO_HOPS = 6

console = Console()


# ============================================================
# IN-MEMORY DATA
# ============================================================

# source account -> transactions going out
outgoing = defaultdict(list)

# destination account -> transactions coming in
incoming = defaultdict(list)

# transaction_id -> transaction
transactions = {}

# transaction_id -> confirmation telemetry
confirmations = {}

# Already displayed scenarios
detected_signatures = set()

# Statistics
total_transactions = 0
total_confirmations = 0
total_detections = 0


# ============================================================
# KAFKA
# ============================================================

def create_consumer():

    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        group_id=KAFKA_GROUP,

        # Read new events from the beginning if this
        # consumer group does not have an offset yet.
        auto_offset_reset="earliest",

        enable_auto_commit=True,

        value_deserializer=lambda value:
            json.loads(value.decode("utf-8"))
    )


# ============================================================
# KAFKA OUTPUT
# ============================================================

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value:
            json.dumps(value).encode("utf-8")
    )


# ============================================================
# DISPLAY TIME
# ============================================================

IST = ZoneInfo("Asia/Kolkata")

def display_time(value):
    if not value:
        return "--"

    try:
        dt = datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))

        return dt.astimezone(IST).strftime("%H:%M:%S")

    except Exception:
        return str(value)[:8]


# ============================================================
# STORE TRANSACTION
# ============================================================

def store_transaction(event):

    global total_transactions

    tx_id = event.get("transaction_id")
    source = event.get("source_account")
    destination = event.get("destination_account")

    if not tx_id or not source or not destination:
        return

    transactions[tx_id] = event

    outgoing[source].append(event)
    incoming[destination].append(event)

    total_transactions += 1


# ============================================================
# STORE CONFIRMATION
# ============================================================

def store_confirmation(event):

    global total_confirmations

    tx_id = event.get("transaction_id")

    if not tx_id:
        return

    confirmations[tx_id] = event

    total_confirmations += 1


# ============================================================
# BUILD GRAPH FROM SOURCE
# ============================================================

def build_graph(source):

    visited = set()
    edges = []

    queue = deque([source])

    while queue:

        account = queue.popleft()

        if account in visited:
            continue

        visited.add(account)

        if len(visited) > MAX_ACCOUNTS:
            return set(), []

        for tx in outgoing.get(account, []):

            destination = tx.get("destination_account")

            if not destination:
                continue

            edges.append(tx)

            if destination not in visited:
                queue.append(destination)

    return visited, edges


# ============================================================
# FIND SOURCE'S DIRECT DESTINATIONS
# ============================================================

def get_direct_destinations(source):

    destinations = set()

    for tx in outgoing.get(source, []):

        destination = tx.get("destination_account")

        if destination:
            destinations.add(destination)

    return destinations


# ============================================================
# FIND FINAL ACCOUNTS
# ============================================================

def find_final_accounts(accounts, edges):

    accounts_with_outgoing = set()

    for tx in edges:

        source = tx.get("source_account")

        if source:
            accounts_with_outgoing.add(source)

    finals = []

    for account in accounts:

        if account not in accounts_with_outgoing:
            finals.append(account)

    return finals


# ============================================================
# FIND MULE
# ============================================================

def find_mule(source):

    destinations = get_direct_destinations(source)

    # IMPORTANT RULE:
    #
    # SOURCE must have exactly ONE immediate destination.
    #
    # SOURCE -> MULE

    if len(destinations) != 1:
        return None

    mule = next(iter(destinations))

    return mule


# ============================================================
# CHECK FOR REAL CHAINING AFTER MULE
# ============================================================

def has_forwarding_after_mule(mule, edges):

    mule_destinations = set()

    for tx in edges:

        if tx.get("source_account") == mule:

            destination = tx.get("destination_account")

            if destination:
                mule_destinations.add(destination)

    return len(mule_destinations) > 0


# ============================================================
# CLASSIFY PATTERN
# ============================================================

def classify_pattern(source, edges):

    outgoing_count = defaultdict(int)
    incoming_count = defaultdict(int)

    for tx in edges:

        src = tx.get("source_account")
        dst = tx.get("destination_account")

        if src:
            outgoing_count[src] += 1

        if dst:
            incoming_count[dst] += 1

    branching = any(
        count > 1
        for account, count in outgoing_count.items()
        if account != source
    )

    convergence = any(
        count > 1
        for count in incoming_count.values()
    )

    if branching and convergence:
        return "MIXED"

    if branching:
        return "BRANCH"

    if convergence:
        return "FAN-IN"

    return "LINEAR"


# ============================================================
# VALIDATE SCENARIO
# ============================================================

def detect_scenario(source):

    accounts, edges = build_graph(source)

    if not accounts:
        return None

    # Require real chaining before this counts as a scenario — a
    # couple of hops isn't a mule chain yet. Only alert once the
    # money has actually moved through a sizeable network of
    # accounts, matching the scale of chains we expect to see
    # (roughly 10-12 accounts once fully formed).
    if len(edges) < MIN_SCENARIO_HOPS:
        return None

    if len(accounts) < MIN_SCENARIO_ACCOUNTS:
        return None

    if len(accounts) > MAX_ACCOUNTS:
        return None

    # Source must have exactly one direct destination.
    mule = find_mule(source)

    if mule is None:
        return None

    # Mule must actually forward money.
    if not has_forwarding_after_mule(mule, edges):
        return None

    # Find final account(s).
    final_accounts = find_final_accounts(
        accounts,
        edges
    )

    if not final_accounts:
        return None

    # Mule cannot be a final account.
    if mule in final_accounts:
        return None

    # Make sure there is a real path beyond mule.
    downstream_accounts = set()

    for tx in edges:

        if tx.get("source_account") == mule:

            dst = tx.get("destination_account")

            if dst:
                downstream_accounts.add(dst)

    if not downstream_accounts:
        return None

    pattern = classify_pattern(
        source,
        edges
    )

    return {
        "alert_id": "SCN-" + uuid.uuid4().hex[:8].upper(),

        "source": source,

        "mule": mule,

        "final_accounts": final_accounts,

        "accounts": accounts,

        "edges": edges,

        "pattern": pattern
    }


# ============================================================
# CREATE SIGNATURE
# ============================================================

def scenario_signature(scenario):

    ids = sorted(
        tx.get("transaction_id")
        for tx in scenario["edges"]
        if tx.get("transaction_id")
    )

    return "|".join(ids)


# ============================================================
# CALCULATE TOTAL AMOUNT
# ============================================================

def total_amount(edges):

    total = 0.0

    for tx in edges:

        try:
            total += float(
                tx.get("amount", 0)
            )

        except (TypeError, ValueError):
            pass

    return total


# ============================================================
# SHOW CHAIN
# ============================================================

def create_chain_text(scenario):

    edges = scenario["edges"]

    adjacency = defaultdict(list)

    for tx in edges:

        src = tx.get("source_account")
        dst = tx.get("destination_account")

        if src and dst:

            adjacency[src].append(dst)

    source = scenario["source"]

    lines = []

    queue = deque([
        (source, 0)
    ])

    visited = set()

    while queue:

        account, level = queue.popleft()

        if account in visited:
            continue

        visited.add(account)

        indent = "    " * level

        if account == scenario["source"]:

            label = (
                f"[bold red]SOURCE[/bold red] "
                f"{account}"
            )

        elif account == scenario["mule"]:

            label = (
                f"[bold yellow]MULE[/bold yellow] "
                f"{account}"
            )

        elif account in scenario["final_accounts"]:

            label = (
                f"[bold green]FINAL[/bold green] "
                f"{account}"
            )

        else:

            label = (
                f"[cyan]{account}[/cyan]"
            )

        lines.append(
            f"{indent}└── {label}"
        )

        for destination in adjacency.get(
            account,
            []
        ):

            queue.append(
                (
                    destination,
                    level + 1
                )
            )

    return "\n".join(lines)


# ============================================================
# TELEMETRY TABLE
# ============================================================

def create_telemetry_table(edges):

    table = Table(
        title="Confirmation / Network Telemetry",
        border_style="cyan"
    )

    table.add_column(
        "Receiver",
        style="cyan"
    )

    table.add_column(
        "Mode",
        style="magenta"
    )

    table.add_column(
        "Mobile → Bank",
        justify="right"
    )

    table.add_column(
        "Server",
        style="yellow"
    )

    table.add_column(
        "Latency",
        justify="right"
    )

    table.add_column(
        "Latitude",
        justify="right"
    )

    table.add_column(
        "Longitude",
        justify="right"
    )

    found = False

    for tx in edges:

        tx_id = tx.get(
            "transaction_id"
        )

        telemetry = confirmations.get(
            tx_id
        )

        if not telemetry:
            continue

        found = True

        receiver = telemetry.get(
            "receiver_account",
            "-"
        )

        mobile_latency = telemetry.get(
            "mobile_to_bank_latency_ms",
            "-"
        )

        mode = tx.get(
            "mode",
            "-"
        )

        servers = telemetry.get(
            "servers",
            []
        )

        for server in servers:

            table.add_row(

                str(receiver),

                str(mode),

                f"{mobile_latency} ms",

                str(
                    server.get(
                        "server_id",
                        "-"
                    )
                ),

                f"{server.get('latency_ms', '-')} ms",

                str(
                    server.get(
                        "latitude",
                        "-"
                    )
                ),

                str(
                    server.get(
                        "longitude",
                        "-"
                    )
                )
            )

    if not found:

        table.add_row(
            "-",
            "-",
            "Waiting...",
            "-",
            "-",
            "-",
            "-"
        )

    return table


# ============================================================
# SHOW DETECTION
# ============================================================

def show_detection(scenario):

    global total_detections

    total_detections += 1

    edges = scenario["edges"]

    amount = total_amount(edges)

    console.print()

    # --------------------------------------------------------
    # BIG RED ALERT
    # --------------------------------------------------------

    alert = Text()

    alert.append(
        "🔴 CHAINING DETECTED\n\n",
        style="bold white"
    )

    alert.append(
        f"Alert ID : {scenario['alert_id']}\n",
        style="bold"
    )

    alert.append(
        f"Pattern  : {scenario['pattern']}\n",
        style="bold magenta"
    )

    alert.append(
        f"Accounts : {len(scenario['accounts'])}\n"
    )

    alert.append(
        f"Transfers: {len(edges)}\n"
    )

    alert.append(
        f"Total    : ₹{amount:,.2f}\n"
    )

    alert.append(
        f"Source   : {scenario['source']}\n",
        style="bold red"
    )

    alert.append(
        f"Mule     : {scenario['mule']}\n",
        style="bold yellow"
    )

    alert.append(
        "Final    : "
        + ", ".join(
            scenario["final_accounts"]
        )
        + "\n",
        style="bold green"
    )

    console.print(
        Panel(
            alert,
            title="[bold white] 🚨 CHAINING ALERT 🚨 [/bold white]",
            border_style="red"
        )
    )

    # --------------------------------------------------------
    # GRAPH OVERVIEW
    # --------------------------------------------------------

    chain_text = create_chain_text(
        scenario
    )

    console.print(
        Panel(
            chain_text,
            title="[bold yellow]Reconstructed Money Flow[/bold yellow]",
            border_style="yellow"
        )
    )

    # --------------------------------------------------------
    # TRANSACTION TABLE
    # --------------------------------------------------------

    tx_table = Table(
        title="Transactions In Detected Scenario",
        border_style="green"
    )

    tx_table.add_column("Transaction ID")
    tx_table.add_column("Source")
    tx_table.add_column("Destination")
    tx_table.add_column(
        "Amount",
        justify="right"
    )
    tx_table.add_column("Mode")

    for tx in edges:

        tx_table.add_row(

            str(
                tx.get(
                    "transaction_id",
                    "-"
                )
            ),

            str(
                tx.get(
                    "source_account",
                    "-"
                )
            ),

            str(
                tx.get(
                    "destination_account",
                    "-"
                )
            ),

            f"₹{float(tx.get('amount', 0)):,.2f}",

            str(
                tx.get(
                    "mode",
                    "-"
                )
            )
        )

    console.print(tx_table)

    # --------------------------------------------------------
    # TELEMETRY
    # --------------------------------------------------------

    console.print(
        create_telemetry_table(
            edges
        )
    )

    console.print(
        Panel(
            "[dim]Detection was inferred from transaction "
            "relationships. No ground-truth suspicion label "
            "was consumed.[/dim]",
            border_style="dim"
        )
    )

    console.print()


# ============================================================
# STATUS
# ============================================================

def show_status():

    table = Table(
        title="🛡 CYBER SIH — LIVE DETECTION",
        border_style="blue"
    )

    table.add_column("Metric")
    table.add_column(
        "Value",
        justify="right"
    )

    table.add_row(
        "Transactions",
        str(total_transactions)
    )

    table.add_row(
        "Confirmations",
        str(total_confirmations)
    )

    table.add_row(
        "Tracked Accounts",
        str(
            len(
                set(outgoing.keys())
                | set(incoming.keys())
            )
        )
    )

    table.add_row(
        "Detections",
        str(total_detections)
    )

    console.print(table)


# ============================================================
# PROCESS EVENT
# ============================================================

def process_event(event, producer=None):

    event_type = event.get(
        "event_type"
    )

    if event_type == "TRANSACTION":

        store_transaction(event)

        # Immediately try to detect newly completed
        # chains.
        detections = []

        # Only accounts with outgoing activity
        # can potentially be sources.
        for source in list(outgoing.keys()):

            scenario = detect_scenario(
                source
            )

            if scenario is None:
                continue

            signature = scenario_signature(
                scenario
            )

            if signature in detected_signatures:
                continue

            detected_signatures.add(
                signature
            )

            detections.append(
                scenario
            )

        for scenario in detections:

            show_detection(
                scenario
            )

            if producer is not None:
                telemetry = []
                for tx in scenario["edges"]:
                    tx_id = tx.get("transaction_id")
                    if tx_id and tx_id in confirmations:
                        telemetry.append(confirmations[tx_id])

                detection_event = {
                    "alert_id": scenario["alert_id"],
                    "pattern": scenario["pattern"],
                    "source_account": scenario["source"],
                    "mule_account": scenario["mule"],
                    "final_accounts": list(scenario["final_accounts"]),
                    "detected_at": datetime.now(IST).isoformat(),
                    "accounts": list(scenario["accounts"]),
                    "transactions": scenario["edges"],
                    "confirmation_telemetry": telemetry,
                }
                try:
                    producer.send(OUTPUT_TOPIC, detection_event)
                    producer.flush()
                except Exception as error:
                    console.print(
                        f"[red]Failed to publish detection:[/red] {error}"
                    )

    elif event_type == "MONEY_RECEIVED_CONFIRMATION":

        store_confirmation(event)


# ============================================================
# MAIN
# ============================================================

def main():

    console.clear()

    console.print(
        Panel(
            "[bold cyan]"
            "🛡 CYBER SIH LIVE DETECTION ENGINE"
            "[/bold cyan]\n\n"
            "Kafka → Graph Reconstruction → "
            "Chaining Detection",
            border_style="cyan"
        )
    )

    console.print(
        f"[yellow]Kafka:[/yellow] {KAFKA_SERVER}"
    )

    console.print(
        f"[yellow]Topic:[/yellow] {KAFKA_TOPIC}"
    )

    console.print(
        "[yellow]Status:[/yellow] Connecting..."
    )

    try:

        consumer = create_consumer()
        producer = create_producer()

    except Exception as error:

        console.print(
            Panel(
                "[bold red]Kafka connection failed[/bold red]\n\n"
                f"{error}\n\n"
                "Check that Docker Kafka is running.",
                border_style="red"
            )
        )

        return

    console.print(
        "[bold green]✓ Kafka connected[/bold green]"
    )

    console.print(
        "[green]✓ Detection engine is LIVE[/green]\n"
    )

    console.print(
        "[dim]Waiting for transactions...[/dim]\n"
    )

    try:

        for message in consumer:

            event = message.value

            process_event(event, producer)

            # Compact status after normal events.
            if event.get("event_type") == "TRANSACTION":

                console.print(
                    f"[dim]{display_time(event.get('timestamp'))}[/dim] "
                    f"[green]TX[/green] "
                    f"{event.get('source_account')} → "
                    f"{event.get('destination_account')} "
                    f"| ₹{event.get('amount', 0)} "
                    f"| {event.get('mode', '-')}"
                )

    except KeyboardInterrupt:

        console.print(
            "\n[yellow]Detection engine stopped.[/yellow]"
        )

    finally:

        consumer.close()
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    main()