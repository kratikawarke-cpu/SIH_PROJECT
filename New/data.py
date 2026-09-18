import json
import random
import time
import uuid
import math
from collections import defaultdict, deque
from datetime import datetime, timezone

from kafka import KafkaProducer
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich import box

from geo_india import infer_state


# ============================================================
# CONFIG
# ============================================================

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "bank_transactions"

MAX_CHAIN_ACCOUNTS = 13
MIN_CHAIN_ACCOUNTS = 9
TARGET_CHAIN_ACCOUNTS = (10, 12)

# A chain always starts with one lump sum at the source and only ever
# splits or shrinks as it moves downstream — money laundering chains
# don't manufacture new money mid-flight. Each hop's operator skims a
# small cut before forwarding the rest; when several routes fan back
# into one account, that account receives the sum of what arrived
# (still capped by what actually came in, never more).
CHAIN_PRINCIPAL_RANGE = (150000.0, 900000.0)
CHAIN_FEE_RATE_RANGE = (0.02, 0.08)
MIN_LEG_AMOUNT = 100.0

console = Console()


# ============================================================
# SYNTHETIC BANK SERVERS
# ============================================================

BANK_SERVERS = [
    {
        "server_id": "BANK_SERVER_01",
        "latitude": 19.0760,
        "longitude": 72.8777,
    },
    {
        "server_id": "BANK_SERVER_02",
        "latitude": 23.2599,
        "longitude": 77.4126,
    },
    {
        "server_id": "BANK_SERVER_03",
        "latitude": 28.6139,
        "longitude": 77.2090,
    },
]


# ============================================================
# ACCOUNT GENERATION
# ============================================================

def account_id():
    return "ACCT-" + str(random.randint(1000, 9999))


def unique_account_id(used_ids):
    # account_id() only draws from 9,000 possible 4-digit numbers, and
    # a single chain has 10-12 nodes — collisions are rare but real,
    # and a collision inside one chain silently merges two accounts
    # into one, turning the intended DAG into a graph with a cycle
    # (breaks topological ordering, and can leave the chain with zero
    # or multiple "final" accounts). Force uniqueness per chain.
    while True:
        candidate = account_id()
        if candidate not in used_ids:
            used_ids.add(candidate)
            return candidate


# ============================================================
# LAT/LON + LATENCY
# ============================================================

def random_device_location():

    latitude = random.uniform(8.0, 30.0)
    longitude = random.uniform(68.0, 88.0)

    return latitude, longitude


def haversine(lat1, lon1, lat2, lon2):

    radius = 6371

    lat1 = math.radians(lat1)
    lat2 = math.radians(lat2)

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1)
        * math.cos(lat2)
        * math.sin(dlon / 2) ** 2
    )

    return 2 * radius * math.asin(
        math.sqrt(a)
    )


def generate_confirmation(transaction_id, receiver):

    device_lat, device_lon = random_device_location()

    mobile_latency = max(
        15,
        min(
            90,
            round(
                random.gauss(42, 7)
            )
        )
    )

    servers = []

    common_network_jitter = random.uniform(
        2,
        8
    )

    for server in BANK_SERVERS:

        distance = haversine(
            device_lat,
            device_lon,
            server["latitude"],
            server["longitude"]
        )

        propagation = distance * 0.004

        routing = random.uniform(
            8,
            18
        )

        jitter = random.uniform(
            -2,
            3
        )

        latency = max(
            8,
            round(
                propagation
                + routing
                + common_network_jitter
                + jitter
            )
        )

        processing = random.randint(
            4,
            12
        )

        confirmation_latency = (
            mobile_latency
            + latency
            + processing
        )

        servers.append(
            {
                "server_id": server["server_id"],
                "latitude": server["latitude"],
                "longitude": server["longitude"],
                "latency_ms": latency,
                "processing_latency_ms": processing,
                "confirmation_latency_ms": confirmation_latency,
            }
        )

    return {
        "event_type": "MONEY_RECEIVED_CONFIRMATION",
        "event_id": (
            "EVT-"
            + uuid.uuid4().hex[:10].upper()
        ),
        "transaction_id": transaction_id,
        "receiver_account": receiver,
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "mobile_to_bank_latency_ms": mobile_latency,
        "device_latitude": round(device_lat, 4),
        "device_longitude": round(device_lon, 4),
        "state": infer_state(device_lat, device_lon),
        "servers": servers,
    }


# ============================================================
# TRANSACTION
# ============================================================

def generate_transaction(source, destination, amount=None):

    modes = [
        "UPI",
        "IMPS",
        "NEFT",
        "RTGS",
    ]

    weights = [
        55,
        25,
        15,
        5,
    ]

    transaction_id = (
        "TX-"
        + uuid.uuid4().hex[:10].upper()
    )

    # Chain legs carry a precomputed, conserved amount (see
    # compute_chain_amounts). Only standalone "noise" transactions
    # (not part of a mule chain) fall back to an independent random
    # amount, since there's no upstream flow to conserve.
    if amount is None:
        amount = random.uniform(500, 50000)

    return {
        "event_type": "TRANSACTION",
        "event_id": (
            "EVT-"
            + uuid.uuid4().hex[:10].upper()
        ),
        "transaction_id": transaction_id,
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_account": source,
        "destination_account": destination,
        "amount": round(amount, 2),
        "currency": "INR",
        "mode": random.choices(
            modes,
            weights=weights,
            k=1
        )[0],
        "status": "SUCCESS",
    }


# ============================================================
# CHAIN GENERATOR
# ============================================================

def create_chain(_depth=0):

    # Every chain targets a realistic, "big enough to matter" mule
    # network: 10-12 unique accounts. Small 3-4 account toy chains
    # were what made scenarios fire on almost every transaction —
    # real chaining takes real length.
    target_accounts = random.randint(*TARGET_CHAIN_ACCOUNTS)

    used_ids = set()

    source = unique_account_id(used_ids)
    mule = unique_account_id(used_ids)

    pattern = random.choice(
        [
            "LINEAR",
            "BRANCH",
            "FAN-IN",
            "MIXED",
        ]
    )

    edges = []
    final = mule

    # SOURCE → MULE
    edges.append(
        {"source": source, "dest": mule}
    )

    # --------------------------------------------------------
    # LINEAR — one long single-file chain of hops.
    # source -> mule -> hop -> hop -> ... -> final
    # --------------------------------------------------------

    if pattern == "LINEAR":

        middle_count = max(
            4,
            target_accounts - 3
        )

        previous = mule

        for _ in range(middle_count):

            next_account = unique_account_id(used_ids)

            edges.append(
                {"source": previous, "dest": next_account}
            )

            previous = next_account

        final = unique_account_id(used_ids)

        edges.append(
            {"source": previous, "dest": final}
        )

    # --------------------------------------------------------
    # BRANCH — true multi-chain: the mule fans out into several
    # *independent* multi-hop chains (not a single hop each), so
    # it reads as parallel laundering routes — but real chains
    # still cash out at exactly one final account, so every
    # branch reconverges there at the end.
    # --------------------------------------------------------

    elif pattern == "BRANCH":

        # Reserve one extra account for the shared final sink that
        # every branch converges into.
        remaining = max(
            6,
            target_accounts - 3
        )

        branch_count = random.randint(2, 3)
        base = remaining // branch_count
        extra = remaining % branch_count

        branch_finals = []

        for i in range(branch_count):

            branch_len = max(2, base + (1 if i < extra else 0))
            previous = mule

            for _ in range(branch_len - 1):

                next_account = unique_account_id(used_ids)

                edges.append(
                    {"source": previous, "dest": next_account}
                )

                previous = next_account

            branch_end = unique_account_id(used_ids)

            edges.append(
                {"source": previous, "dest": branch_end}
            )

            branch_finals.append(branch_end)

        # Every parallel branch cashes out into the same single
        # final account — one true endpoint, not several.
        final = unique_account_id(used_ids)

        for branch_end in branch_finals:

            edges.append(
                {"source": branch_end, "dest": final}
            )

    # --------------------------------------------------------
    # FAN-IN — several entry accounts converge into one funnel,
    # then the money keeps moving several more hops downstream.
    # --------------------------------------------------------

    elif pattern == "FAN-IN":

        remaining = max(
            6,
            target_accounts - 2
        )

        fan_count = random.randint(3, 4)
        fan_accounts = []

        for _ in range(fan_count):

            a = unique_account_id(used_ids)
            fan_accounts.append(a)
            edges.append({"source": mule, "dest": a})

        convergence = unique_account_id(used_ids)

        for a in fan_accounts:
            edges.append({"source": a, "dest": convergence})

        extra_hops = max(
            0,
            remaining - fan_count - 2
        )

        previous = convergence

        for _ in range(extra_hops):

            next_account = unique_account_id(used_ids)

            edges.append(
                {"source": previous, "dest": next_account}
            )

            previous = next_account

        final = unique_account_id(used_ids)

        edges.append(
            {"source": previous, "dest": final}
        )

    # --------------------------------------------------------
    # MIXED — branches out, each branch chains a couple of hops,
    # then everything converges again before the final payout.
    # --------------------------------------------------------

    else:

        remaining = max(
            8,
            target_accounts - 2
        )

        a = unique_account_id(used_ids)
        b = unique_account_id(used_ids)

        edges.append({"source": mule, "dest": a})
        edges.append({"source": mule, "dest": b})

        branch_hops = max(1, (remaining - 4) // 2)

        prev_a, prev_b = a, b

        for _ in range(branch_hops):

            next_a = unique_account_id(used_ids)
            edges.append({"source": prev_a, "dest": next_a})
            prev_a = next_a

            next_b = unique_account_id(used_ids)
            edges.append({"source": prev_b, "dest": next_b})
            prev_b = next_b

        convergence = unique_account_id(used_ids)

        edges.append({"source": prev_a, "dest": convergence})
        edges.append({"source": prev_b, "dest": convergence})

        final = unique_account_id(used_ids)

        edges.append(
            {"source": convergence, "dest": final}
        )

    # --------------------------------------------------------
    # ACCOUNT COUNT SAFETY NET
    # --------------------------------------------------------

    accounts = set()

    for edge in edges:

        accounts.add(edge["source"])
        accounts.add(edge["dest"])

    account_count = len(accounts)

    if (
        account_count > MAX_CHAIN_ACCOUNTS
        or account_count < MIN_CHAIN_ACCOUNTS
    ):

        if _depth >= 20:
            # Extremely unlucky run of ID collisions — fall back to
            # whatever we built rather than recurse forever.
            compute_chain_amounts(edges, source)

            return {
                "source": source,
                "mule": mule,
                "final": final,
                "pattern": pattern,
                "edges": edges,
            }

        return create_chain(_depth=_depth + 1)

    # Assign every leg's amount only once the final topology is
    # locked in, so splits/merges are computed over the real graph.
    compute_chain_amounts(edges, source)

    return {
        "source": source,
        "mule": mule,
        "final": final,
        "pattern": pattern,
        "edges": edges,
    }


# ============================================================
# AMOUNT CONSERVATION
# ============================================================

def compute_chain_amounts(edges, source):
    """
    Assign every edge in a chain an amount such that money only ever
    splits or shrinks as it flows downstream from `source` — never
    increases. Walks the chain in topological order (Kahn's
    algorithm, since a chain is always a DAG): each account forwards
    a random fraction of what it received, minus a small skimmed
    "fee" (the operator's cut), split across however many outgoing
    legs it has. Accounts with several incoming legs (convergence
    points) simply receive the sum of what arrived on each.
    """

    out_adj = defaultdict(list)
    indegree = defaultdict(int)
    nodes = set()

    for edge in edges:
        out_adj[edge["source"]].append(edge)
        indegree[edge["dest"]] += 1
        nodes.add(edge["source"])
        nodes.add(edge["dest"])

    indegree.setdefault(source, 0)

    inflow = defaultdict(float)
    inflow[source] = round(random.uniform(*CHAIN_PRINCIPAL_RANGE), 2)

    queue = deque(
        n for n in nodes if indegree.get(n, 0) == 0
    )
    processed = set()

    while queue:

        node = queue.popleft()

        if node in processed:
            continue

        processed.add(node)

        outgoing = out_adj.get(node, [])
        available = inflow.get(node, 0.0)

        if outgoing:

            fee_rate = random.uniform(*CHAIN_FEE_RATE_RANGE)
            forwardable = max(available * (1 - fee_rate), 0.0)

            if len(outgoing) == 1:
                shares = [1.0]
            else:
                raw = [random.uniform(0.6, 1.4) for _ in outgoing]
                total_raw = sum(raw)
                shares = [r / total_raw for r in raw]

            leg_amounts = [
                round(forwardable * share, 2)
                for share in shares
            ]

            # Floor tiny legs to something realistic, then rescale
            # the whole set down if that floor would ever push the
            # total above what's actually available — the sum handed
            # out must never exceed what came in.
            leg_amounts = [
                max(amount, MIN_LEG_AMOUNT)
                for amount in leg_amounts
            ]

            total_legs = sum(leg_amounts)

            if total_legs > forwardable and total_legs > 0:
                scale = forwardable / total_legs
                leg_amounts = [
                    round(amount * scale, 2)
                    for amount in leg_amounts
                ]

            for edge, leg_amount in zip(outgoing, leg_amounts):
                edge["amount"] = leg_amount
                inflow[edge["dest"]] += leg_amount

        for edge in outgoing:

            dst = edge["dest"]
            indegree[dst] -= 1

            if indegree[dst] <= 0 and dst not in processed:
                queue.append(dst)

    return edges


# ============================================================
# RICH TABLES
# ============================================================

def create_transaction_table(rows):

    table = Table(
        title="💸 LIVE TRANSACTION STREAM",
        box=box.ROUNDED,
        expand=True,
        show_lines=False,
    )

    table.add_column(
        "Time",
        style="dim",
        no_wrap=True
    )

    table.add_column(
        "Transaction ID",
        style="cyan",
        no_wrap=True
    )

    table.add_column(
        "Source Account",
        style="yellow",
        no_wrap=True
    )

    table.add_column(
        "Destination",
        style="magenta",
        no_wrap=True
    )

    table.add_column(
        "Amount",
        justify="right",
        no_wrap=True
    )

    table.add_column(
        "Mode",
        style="blue",
        no_wrap=True
    )

    table.add_column(
        "Status",
        style="green",
        no_wrap=True
    )

    for row in rows:

        table.add_row(
            row["time"],
            row["transaction_id"],
            row["source"],
            row["destination"],
            row["amount"],
            row["mode"],
            "[green]SUCCESS[/green]",
        )

    return table


def create_confirmation_table(rows):

    table = Table(
        title="📱 MONEY RECEIVED — CONFIRMATION TELEMETRY",
        box=box.ROUNDED,
        expand=True,
        show_lines=False,
    )

    table.add_column(
        "Time",
        style="dim",
        no_wrap=True
    )

    table.add_column(
        "Transaction ID",
        style="cyan",
        no_wrap=True
    )

    table.add_column(
        "Receiver",
        style="magenta",
        no_wrap=True
    )

    table.add_column(
        "Mobile → Bank",
        style="yellow",
        justify="right",
        no_wrap=True
    )

    table.add_column(
        "Server",
        style="blue",
        no_wrap=True
    )

    table.add_column(
        "Server Latency",
        justify="right",
        no_wrap=True
    )

    table.add_column(
        "Latitude",
        justify="right",
        no_wrap=True
    )

    table.add_column(
        "Longitude",
        justify="right",
        no_wrap=True
    )

    for row in rows:

        table.add_row(
            row["time"],
            row["transaction_id"],
            row["receiver"],
            f"{row['mobile_latency']} ms",
            row["server"],
            f"{row['server_latency']} ms",
            str(row["latitude"]),
            str(row["longitude"]),
        )

    return table


def create_header():

    table = Table(
        box=box.HEAVY,
        expand=True,
    )

    table.add_column(
        "SYSTEM",
        style="bold cyan"
    )

    table.add_column(
        "KAFKA",
        style="bold green"
    )

    table.add_column(
        "TOPIC",
        style="bold yellow"
    )

    table.add_column(
        "STREAM",
        style="bold green"
    )

    table.add_row(
        "CYBER SIH",
        KAFKA_SERVER,
        KAFKA_TOPIC,
        "● LIVE",
    )

    return table


# ============================================================
# KAFKA
# ============================================================

def create_producer():

    console.print(
        "[yellow]Connecting to Kafka...[/yellow]"
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,

        value_serializer=lambda value:
            json.dumps(value).encode("utf-8"),

        acks="all",

        retries=5,
    )

    producer.bootstrap_connected()

    console.print(
        "[bold green]✓ Kafka connected[/bold green]\n"
    )

    return producer


def publish(producer, event):

    producer.send(
        KAFKA_TOPIC,
        value=event
    )

    producer.flush()


# ============================================================
# MAIN
# ============================================================

def main():

    console.clear()

    producer = create_producer()

    # Keep recent rows only so terminal doesn't grow forever.
    transaction_rows = []
    confirmation_rows = []

    chains = [
        create_chain()
        for _ in range(5)
    ]

    chain_indexes = [
        0
        for _ in chains
    ]

    transaction_counter = 0

    console.print(create_header())
    console.print(
        "\n[dim]Generating synthetic live banking data..."
        " Press CTRL+C to stop.[/dim]\n"
    )

    try:

        with Live(
            refresh_per_second=4,
            console=console,
            screen=False,
        ) as live:

            while True:

                # ------------------------------------------------
                # Choose chain or normal transaction
                # ------------------------------------------------

                generate_chain_transaction = (
                    random.random() < 0.40
                )

                if generate_chain_transaction:

                    chain_index = random.randrange(
                        len(chains)
                    )

                    chain = chains[
                        chain_index
                    ]

                    edge_index = chain_indexes[
                        chain_index
                    ]

                    edges = chain["edges"]

                    if edge_index >= len(edges):

                        chains[chain_index] = (
                            create_chain()
                        )

                        chain_indexes[
                            chain_index
                        ] = 0

                        chain = chains[
                            chain_index
                        ]

                        edges = chain["edges"]

                        edge_index = 0

                    edge = edges[
                        edge_index
                    ]

                    source = edge["source"]
                    destination = edge["dest"]
                    chain_amount = edge.get("amount")

                    chain_indexes[
                        chain_index
                    ] += 1

                else:

                    source = account_id()
                    destination = account_id()
                    chain_amount = None

                # ------------------------------------------------
                # Generate transaction
                # ------------------------------------------------

                tx = generate_transaction(
                    source,
                    destination,
                    amount=chain_amount
                )

                publish(
                    producer,
                    tx
                )

                transaction_counter += 1

                tx_time = datetime.now().strftime(
                    "%H:%M:%S"
                )

                transaction_rows.append(
                    {
                        "time": tx_time,
                        "transaction_id":
                            tx["transaction_id"],
                        "source":
                            tx["source_account"],
                        "destination":
                            tx["destination_account"],
                        "amount":
                            f"₹{tx['amount']:,.2f}",
                        "mode":
                            tx["mode"],
                    }
                )

                # Keep latest 12.
                transaction_rows = (
                    transaction_rows[-12:]
                )

                # ------------------------------------------------
                # Confirmation
                # ------------------------------------------------

                confirmation = generate_confirmation(
                    tx["transaction_id"],
                    destination
                )

                publish(
                    producer,
                    confirmation
                )

                confirmation_time = datetime.now().strftime(
                    "%H:%M:%S"
                )

                for server in confirmation["servers"]:

                    confirmation_rows.append(
                        {
                            "time":
                                confirmation_time,

                            "transaction_id":
                                confirmation[
                                    "transaction_id"
                                ],

                            "receiver":
                                confirmation[
                                    "receiver_account"
                                ],

                            "mobile_latency":
                                confirmation[
                                    "mobile_to_bank_latency_ms"
                                ],

                            "server":
                                server[
                                    "server_id"
                                ],

                            "server_latency":
                                server[
                                    "latency_ms"
                                ],

                            "latitude":
                                server[
                                    "latitude"
                                ],

                            "longitude":
                                server[
                                    "longitude"
                                ],
                        }
                    )

                # Keep latest 12 server rows.
                confirmation_rows = (
                    confirmation_rows[-12:]
                )

                # ------------------------------------------------
                # Update Rich display
                # ------------------------------------------------

                display = Table.grid(
                    expand=True
                )

                display.add_row(
                    create_header()
                )

                display.add_row(
                    create_transaction_table(
                        transaction_rows
                    )
                )

                display.add_row(
                    create_confirmation_table(
                        confirmation_rows
                    )
                )

                display.add_row(
                    f"[dim]Transactions published: "
                    f"{transaction_counter}    "
                    f"Kafka topic: {KAFKA_TOPIC}    "
                    f"Press CTRL+C to stop[/dim]"
                )

                live.update(display)

                time.sleep(
                    random.uniform(
                        1.0,
                        2.5
                    )
                )

    except KeyboardInterrupt:

        console.print(
            "\n[yellow]Generator stopped.[/yellow]"
        )

    finally:

        producer.close()

        console.print(
            "[dim]Kafka producer closed.[/dim]"
        )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()