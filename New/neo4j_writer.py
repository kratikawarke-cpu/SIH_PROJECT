import json
from kafka import KafkaConsumer
from neo4j import GraphDatabase
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich import box

# ============================================================
# CONFIG
# ============================================================

KAFKA_SERVER = "localhost:9092"
DETECTION_TOPIC = "detected_scenarios"
CONSUMER_GROUP = "cyber-sih-neo4j-writer-fixed"

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password123"

console = Console()


# ============================================================
# NEO4J
# ============================================================

class Neo4jWriter:

    def __init__(self):
        self.driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD)
        )

    def verify_connection(self):
        self.driver.verify_connectivity()

    def close(self):
        self.driver.close()

    def write_scenario(self, event):
        with self.driver.session() as session:
            session.execute_write(
                self._write_scenario_tx,
                event
            )

    @staticmethod
    def _write_scenario_tx(tx, event):

        alert_id = event.get("alert_id")
        pattern = event.get("pattern")
        source = event.get("source_account")
        mule = event.get("mule_account")
        detected_at = event.get("detected_at")

        final_accounts = event.get("final_accounts", [])

        # Backward compatibility if an older detector sends final_account.
        if not final_accounts:
            final_account = event.get("final_account")
            if final_account:
                final_accounts = (
                    final_account
                    if isinstance(final_account, list)
                    else [final_account]
                )

        if not alert_id or not source or not mule:
            raise ValueError("Invalid detection event: missing alert/source/mule")

        # ====================================================
        # SCENARIO NODE
        # ====================================================

        tx.run(
            """
            MERGE (s:Scenario {alert_id: $alert_id})
            SET
                s.pattern = $pattern,
                s.detected_at = $detected_at,
                s.source_account = $source,
                s.mule_account = $mule,
                s.final_accounts = $final_accounts,
                s.updated_at = datetime()
            """,
            alert_id=alert_id,
            pattern=pattern,
            detected_at=detected_at,
            source=source,
            mule=mule,
            final_accounts=final_accounts
        )

        # ====================================================
        # ACCOUNT NODES
        # ====================================================

        accounts = event.get("accounts", [])

        # Defensive normalization: only strings become Account IDs.
        accounts = [
            account for account in accounts
            if isinstance(account, str)
        ]

        for account in accounts:
            if account == source:
                role = "SOURCE"
            elif account == mule:
                role = "MULE"
            elif account in final_accounts:
                role = "FINAL"
            else:
                role = "CHAIN_ACCOUNT"

            tx.run(
                """
                MERGE (a:Account {account_id: $account_id})
                WITH a
                MATCH (s:Scenario {alert_id: $alert_id})
                MERGE (s)-[r:INVOLVES]->(a)
                SET r.role = $role
                """,
                account_id=account,
                alert_id=alert_id,
                role=role
            )

        # ====================================================
        # TRANSACTIONS
        # ====================================================

        transactions = event.get("transactions", [])

        for transaction in transactions:

            # Current detector sends complete transaction dictionaries.
            # Ignore old string-only transaction IDs rather than crashing.
            if not isinstance(transaction, dict):
                continue

            transaction_id = transaction.get("transaction_id")
            source_account = transaction.get("source_account")
            destination_account = transaction.get("destination_account")

            if not transaction_id or not source_account or not destination_account:
                continue

            tx.run(
                """
                MERGE (t:Transaction {
                    transaction_id: $transaction_id
                })
                SET
                    t.amount = $amount,
                    t.currency = $currency,
                    t.mode = $mode,
                    t.timestamp = $timestamp,
                    t.status = $status

                WITH t
                MATCH (src:Account {account_id: $source_account})
                MATCH (dst:Account {account_id: $destination_account})

                MERGE (t)-[:FROM]->(src)
                MERGE (t)-[:TO]->(dst)

                MERGE (src)-[tr:TRANSFERRED {
                    transaction_id: $transaction_id
                }]->(dst)

                SET
                    tr.amount = $amount,
                    tr.currency = $currency,
                    tr.mode = $mode,
                    tr.timestamp = $timestamp,
                    tr.status = $status
                """,
                transaction_id=transaction_id,
                source_account=source_account,
                destination_account=destination_account,
                amount=transaction.get("amount"),
                currency=transaction.get("currency"),
                mode=transaction.get("mode"),
                timestamp=transaction.get("timestamp"),
                status=transaction.get("status")
            )

            # Scenario -> Transaction is valid because both endpoints
            # are nodes.
            tx.run(
                """
                MATCH (s:Scenario {alert_id: $alert_id})
                MATCH (t:Transaction {transaction_id: $transaction_id})
                MERGE (s)-[:CONTAINS_TRANSACTION]->(t)
                """,
                alert_id=alert_id,
                transaction_id=transaction_id
            )

        # ====================================================
        # CONFIRMATION TELEMETRY
        # ====================================================

        for confirmation in event.get("confirmation_telemetry", []):

            if not isinstance(confirmation, dict):
                continue

            transaction_id = confirmation.get("transaction_id")
            receiver = confirmation.get("receiver_account")

            if not transaction_id:
                continue

            tx.run(
                """
                MERGE (c:Confirmation {
                    transaction_id: $transaction_id
                })
                SET
                    c.receiver_account = $receiver,
                    c.timestamp = $timestamp,
                    c.mobile_to_bank_latency_ms = $mobile_latency,
                    c.device_latitude = $device_latitude,
                    c.device_longitude = $device_longitude,
                    c.state = $state

                WITH c
                MATCH (t:Transaction {
                    transaction_id: $transaction_id
                })
                MERGE (t)-[:HAS_CONFIRMATION]->(c)
                """,
                transaction_id=transaction_id,
                receiver=receiver,
                timestamp=confirmation.get("timestamp"),
                mobile_latency=confirmation.get(
                    "mobile_to_bank_latency_ms"
                ),
                device_latitude=confirmation.get("device_latitude"),
                device_longitude=confirmation.get("device_longitude"),
                state=confirmation.get("state"),
            )

            # =================================================
            # SERVER TELEMETRY
            # =================================================

            for server in confirmation.get("servers", []):

                if not isinstance(server, dict):
                    continue

                server_id = server.get("server_id")
                if not server_id:
                    continue

                tx.run(
                    """
                    MERGE (bs:BankServer {
                        server_id: $server_id
                    })
                    SET
                        bs.latitude = $latitude,
                        bs.longitude = $longitude
                    """,
                    server_id=server_id,
                    latitude=server.get("latitude"),
                    longitude=server.get("longitude")
                )

                tx.run(
                    """
                    MATCH (c:Confirmation {
                        transaction_id: $transaction_id
                    })
                    MATCH (bs:BankServer {
                        server_id: $server_id
                    })
                    MERGE (c)-[r:PROCESSED_BY]->(bs)
                    SET
                        r.server_latency_ms = $server_latency,
                        r.processing_latency_ms = $processing_latency,
                        r.confirmation_latency_ms = $confirmation_latency
                    """,
                    transaction_id=transaction_id,
                    server_id=server_id,
                    server_latency=server.get("latency_ms"),
                    processing_latency=server.get(
                        "processing_latency_ms"
                    ),
                    confirmation_latency=server.get(
                        "confirmation_latency_ms"
                    )
                )


# ============================================================
# RICH DISPLAY
# ============================================================

def create_table(processed, created, updated, last_event):

    table = Table(
        title="🟢 NEO4J SCENARIO WRITER",
        box=box.ROUNDED,
        expand=True
    )

    table.add_column("Metric")
    table.add_column("Value")

    table.add_row("Kafka topic", DETECTION_TOPIC)
    table.add_row("Neo4j", NEO4J_URI)
    table.add_row("Events processed", str(processed))
    table.add_row("Scenarios created", str(created))
    table.add_row("Scenario updates", str(updated))

    if last_event:
        table.add_row(
            "Last alert",
            str(last_event.get("alert_id", ""))
        )
        table.add_row(
            "Pattern",
            str(last_event.get("pattern", ""))
        )
        table.add_row(
            "Source",
            str(last_event.get("source_account", ""))
        )
        table.add_row(
            "Mule",
            str(last_event.get("mule_account", ""))
        )

        finals = last_event.get("final_accounts")
        if finals is None:
            finals = last_event.get("final_account", "")

        table.add_row("Final", str(finals))

        table.add_row(
            "Telemetry",
            str(len(last_event.get(
                "confirmation_telemetry",
                []
            )))
        )

    else:
        table.add_row("Last alert", "Waiting...")

    return table


# ============================================================
# MAIN
# ============================================================

def main():

    console.print("Connecting to Kafka and Neo4j...")

    try:
        consumer = KafkaConsumer(
            DETECTION_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda value:
                json.loads(value.decode("utf-8"))
        )

        writer = Neo4jWriter()
        writer.verify_connection()

    except Exception as error:
        console.print(
            f"[red]Connection failed: {error}[/red]"
        )
        return

    console.print("[green]✓ Kafka connected[/green]")
    console.print("[green]✓ Neo4j connected[/green]\n")

    processed = 0
    created = 0
    updated = 0
    last_event = None

    try:
        with Live(
            create_table(
                processed,
                created,
                updated,
                last_event
            ),
            console=console,
            refresh_per_second=5
        ) as live:

            for message in consumer:

                event = message.value

                try:
                    if not isinstance(event, dict):
                        raise ValueError(
                            f"Detection event must be an object, "
                            f"got {type(event).__name__}"
                        )

                    writer.write_scenario(event)

                    processed += 1
                    created += 1
                    last_event = event

                except Exception as error:
                    console.print(
                        f"\n[red]Writer error:[/red]\n"
                        f"{error}"
                    )

                live.update(
                    create_table(
                        processed,
                        created,
                        updated,
                        last_event
                    )
                )

    except KeyboardInterrupt:
        console.print(
            "\n[yellow]Writer stopped.[/yellow]"
        )

    finally:
        consumer.close()
        writer.close()
        console.print(
            "\n[yellow]Kafka and Neo4j connections closed.[/yellow]"
        )


if __name__ == "__main__":
    main()
