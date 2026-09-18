"""
CYBER SIH - Neo4j Backend API
Read-only API for the fraud-chain graph.

Run:
    pip install -r requirements_backend.txt
    uvicorn backend:app --host 0.0.0.0 --port 8000 --reload
"""

from typing import Any, Dict, List, Optional
import hashlib

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase

from geo_india import ALL_STATES, infer_state

# ============================================================
# CONFIG
# ============================================================

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password123"

app = FastAPI(
    title="Cyber SIH Fraud Detection API",
    description="Read-only API over the Neo4j transaction/fraud graph.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD),
)


# ============================================================
# HELPERS
# ============================================================

def json_safe(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "iso_format"):
        return value.iso_format()
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if hasattr(value, "items") and not isinstance(value, (str, bytes)):
        try:
            return {key: json_safe(item) for key, item in dict(value).items()}
        except Exception:
            return str(value)
    return value


def record_to_dict(record: Any) -> Dict[str, Any]:
    result = {}
    for key in record.keys():
        result[key] = json_safe(record[key])
    return result


def node_to_dict(node: Any) -> Dict[str, Any]:
    return json_safe(dict(node))


def derived_state(explicit: Optional[str], key: Optional[str]) -> Optional[str]:
    if explicit:
        return explicit
    if not key:
        return None
    digest = hashlib.md5(str(key).encode("utf-8")).hexdigest()
    return ALL_STATES[int(digest, 16) % len(ALL_STATES)]


def unique_states(pairs: List[Dict[str, Any]]) -> List[str]:
    values = []
    seen = set()
    for pair in pairs:
        state = derived_state(pair.get("state"), pair.get("key"))
        if state and state not in seen:
            seen.add(state)
            values.append(state)
    return values


def scenario_states_query() -> str:
    return """
    OPTIONAL MATCH (s)-[:CONTAINS_TRANSACTION]->(:Transaction)
                  -[:HAS_CONFIRMATION]->(c:Confirmation)
    WITH s, collect(DISTINCT {
        state: c.state,
        key: coalesce(c.receiver_account, c.transaction_id)
    }) AS state_pairs
    """


@app.on_event("shutdown")
def shutdown() -> None:
    driver.close()


# ============================================================
# HEALTH
# ============================================================

@app.get("/")
def root():
    return {
        "service": "Cyber SIH Fraud Detection API",
        "status": "running",
        "docs": "/docs",
    }


@app.get("/api/health")
def health():
    try:
        with driver.session() as session:
            session.run("RETURN 1 AS ok").single()
        return {
            "status": "ok",
            "neo4j": "connected",
        }
    except Exception as exc:
        return {
            "status": "error",
            "neo4j": "disconnected",
            "error": str(exc),
        }


# ============================================================
# DASHBOARD STATS
# ============================================================

@app.get("/api/stats")
def stats():
    with driver.session() as session:
        result = session.run(
            """
            MATCH (s:Scenario)
            WITH count(s) AS scenarios
            OPTIONAL MATCH (a:Account)
            WITH scenarios, count(a) AS accounts
            OPTIONAL MATCH (t:Transaction)
            WITH scenarios, accounts, count(t) AS transactions
            OPTIONAL MATCH (c:Confirmation)
            RETURN
                scenarios,
                accounts,
                transactions,
                count(c) AS confirmations
            """
        ).single()

    return dict(result)


# ============================================================
# STATES
# ============================================================

@app.get("/api/states")
def get_states():
    counts: Dict[str, int] = {name: 0 for name in ALL_STATES}

    with driver.session() as session:
        result = session.run(
            """
            MATCH (s:Scenario)-[:CONTAINS_TRANSACTION]->(:Transaction)
                  -[:HAS_CONFIRMATION]->(c:Confirmation)
            RETURN s.alert_id AS alert_id,
                   c.state AS state,
                   coalesce(c.receiver_account, c.transaction_id) AS key
            """
        )
        scenario_states: Dict[str, set] = {}
        for record in result:
            name = derived_state(record["state"], record["key"])
            if not name:
                continue
            scenario_states.setdefault(record["alert_id"], set()).add(name)
        for names in scenario_states.values():
            for name in names:
                counts[name] = counts.get(name, 0) + 1

    return {
        "states": [
            {"name": name, "count": counts.get(name, 0)}
            for name in ALL_STATES
        ]
    }


# ============================================================
# SCENARIOS
# ============================================================

@app.get("/api/scenarios")
def get_scenarios(
    limit: int = Query(50, ge=1, le=500),
    pattern: Optional[str] = None,
    q: Optional[str] = None,
    states: Optional[str] = Query(
        None,
        description="Comma-separated Indian states to filter by.",
    ),
):
    state_list = [
        item.strip()
        for item in (states or "").split(",")
        if item.strip()
    ]
    query_text = (q or "").strip()

    cypher = f"""
        MATCH (s:Scenario)
        {scenario_states_query()}
        WHERE ($pattern IS NULL OR toUpper(s.pattern) = toUpper($pattern))
          AND (
            $q = '' OR
            toLower(s.alert_id) CONTAINS toLower($q) OR
            toLower(coalesce(s.source_account, '')) CONTAINS toLower($q) OR
            toLower(coalesce(s.mule_account, '')) CONTAINS toLower($q) OR
            any(final IN coalesce(s.final_accounts, []) WHERE toLower(final) CONTAINS toLower($q))
          )
        RETURN
            s.alert_id AS alert_id,
            s.pattern AS pattern,
            s.detected_at AS detected_at,
            s.source_account AS source_account,
            s.mule_account AS mule_account,
            s.final_accounts AS final_accounts,
            s.updated_at AS updated_at,
            state_pairs
        ORDER BY s.detected_at DESC
        LIMIT $fetch_limit
    """

    fetch_limit = 500 if state_list else limit

    with driver.session() as session:
        result = session.run(
            cypher,
            pattern=pattern,
            q=query_text,
            fetch_limit=fetch_limit,
        )
        scenarios = []
        for record in result:
            item = record_to_dict(record)
            pairs = item.pop("state_pairs", []) or []
            item["states"] = unique_states(
                [pair for pair in pairs if isinstance(pair, dict)]
            )
            if state_list and not any(state in state_list for state in item["states"]):
                continue
            scenarios.append(item)
            if len(scenarios) >= limit:
                break

    return {
        "count": len(scenarios),
        "scenarios": scenarios,
    }


@app.get("/api/scenarios/{alert_id}")
def get_scenario(alert_id: str):
    with driver.session() as session:
        scenario_record = session.run(
            """
            MATCH (s:Scenario {alert_id: $alert_id})
            RETURN s
            """,
            alert_id=alert_id,
        ).single()

        if not scenario_record:
            raise HTTPException(status_code=404, detail="Scenario not found")

        scenario = node_to_dict(scenario_record["s"])

        account_records = session.run(
            """
            MATCH (s:Scenario {alert_id: $alert_id})
                  -[rel:INVOLVES]->(a:Account)
            RETURN
                a.account_id AS account_id,
                rel.role AS role
            ORDER BY
                CASE rel.role
                    WHEN 'SOURCE' THEN 0
                    WHEN 'MULE' THEN 1
                    WHEN 'CHAIN_ACCOUNT' THEN 2
                    WHEN 'FINAL' THEN 3
                    ELSE 4
                END,
                a.account_id
            """,
            alert_id=alert_id,
        )
        accounts = [record_to_dict(record) for record in account_records]

        tx_records = session.run(
            """
            MATCH (s:Scenario {alert_id: $alert_id})
                  -[:CONTAINS_TRANSACTION]->(t:Transaction)
            RETURN
                t.transaction_id AS transaction_id,
                t.timestamp AS timestamp,
                t.source_account AS source_account,
                t.destination_account AS destination_account,
                t.amount AS amount,
                t.currency AS currency,
                t.mode AS mode,
                t.status AS status
            ORDER BY t.timestamp
            """,
            alert_id=alert_id,
        )
        transactions = [record_to_dict(record) for record in tx_records]

        # Transaction nodes may not store source/destination properties.
        if any(
            not tx.get("source_account") or not tx.get("destination_account")
            for tx in transactions
        ):
            filled = session.run(
                """
                MATCH (s:Scenario {alert_id: $alert_id})
                      -[:CONTAINS_TRANSACTION]->(t:Transaction)
                OPTIONAL MATCH (t)-[:FROM]->(src:Account)
                OPTIONAL MATCH (t)-[:TO]->(dst:Account)
                RETURN
                    t.transaction_id AS transaction_id,
                    t.timestamp AS timestamp,
                    src.account_id AS source_account,
                    dst.account_id AS destination_account,
                    t.amount AS amount,
                    t.currency AS currency,
                    t.mode AS mode,
                    t.status AS status
                ORDER BY t.timestamp
                """,
                alert_id=alert_id,
            )
            transactions = [record_to_dict(record) for record in filled]

        state_record = session.run(
            """
            MATCH (s:Scenario {alert_id: $alert_id})
                  -[:CONTAINS_TRANSACTION]->(:Transaction)
                  -[:HAS_CONFIRMATION]->(c:Confirmation)
            RETURN collect(DISTINCT {
                state: c.state,
                key: coalesce(c.receiver_account, c.transaction_id)
            }) AS state_pairs
            """,
            alert_id=alert_id,
        ).single()
        scenario["states"] = unique_states(
            state_record["state_pairs"] if state_record else []
        )

    return {
        "scenario": scenario,
        "accounts": accounts,
        "transactions": transactions,
    }


# ============================================================
# GRAPH
# ============================================================

@app.get("/api/scenarios/{alert_id}/graph")
def get_scenario_graph(alert_id: str):
    with driver.session() as session:
        exists = session.run(
            """
            MATCH (s:Scenario {alert_id: $alert_id})
            RETURN count(s) AS count
            """,
            alert_id=alert_id,
        ).single()

        if not exists or exists["count"] == 0:
            raise HTTPException(status_code=404, detail="Scenario not found")

        result = session.run(
            """
            MATCH (s:Scenario {alert_id: $alert_id})
            OPTIONAL MATCH (s)-[:INVOLVES]->(a:Account)
            OPTIONAL MATCH (s)-[:CONTAINS_TRANSACTION]->(t:Transaction)
            OPTIONAL MATCH (t)-[:FROM]->(src:Account)
            OPTIONAL MATCH (t)-[:TO]->(dst:Account)
            OPTIONAL MATCH (t)-[:HAS_CONFIRMATION]->(c:Confirmation)
            OPTIONAL MATCH (c)-[:PROCESSED_BY]->(bs:BankServer)
            RETURN
                s,
                collect(DISTINCT a) AS accounts,
                collect(DISTINCT t) AS transactions,
                collect(DISTINCT src) AS sources,
                collect(DISTINCT dst) AS destinations,
                collect(DISTINCT c) AS confirmations,
                collect(DISTINCT bs) AS servers
            """,
            alert_id=alert_id,
        ).single()

    scenario = node_to_dict(result["s"])

    nodes: Dict[str, Dict[str, Any]] = {}
    edges: List[Dict[str, Any]] = []

    def add_node(node: Any, node_type: str, node_id: str):
        if node is None:
            return
        props = node_to_dict(node)
        if node_type == "Confirmation" and not props.get("state"):
            props["state"] = infer_state(
                props.get("device_latitude"),
                props.get("device_longitude"),
            )
        nodes[node_id] = {
            "id": node_id,
            "type": node_type,
            "data": props,
        }

    for account in result["accounts"]:
        if account:
            aid = account.get("account_id")
            if aid:
                add_node(account, "Account", f"account:{aid}")

    for tx in result["transactions"]:
        if tx:
            tid = tx.get("transaction_id")
            if tid:
                add_node(tx, "Transaction", f"transaction:{tid}")

    for confirmation in result["confirmations"]:
        if confirmation:
            tid = confirmation.get("transaction_id")
            if tid:
                add_node(
                    confirmation,
                    "Confirmation",
                    f"confirmation:{tid}",
                )

    for server in result["servers"]:
        if server:
            sid = server.get("server_id")
            if sid:
                add_node(server, "BankServer", f"server:{sid}")

    with driver.session() as session:
        rel_result = session.run(
            """
            MATCH (s:Scenario {alert_id: $alert_id})-[sr:INVOLVES]->(a:Account)
            RETURN
                'Scenario' AS source_type,
                s.alert_id AS source_id,
                type(sr) AS relationship,
                'Account' AS target_type,
                a.account_id AS target_id,
                sr.role AS role

            UNION ALL

            MATCH (s:Scenario {alert_id: $alert_id})
                  -[cr:CONTAINS_TRANSACTION]->(t:Transaction)
            RETURN
                'Scenario' AS source_type,
                s.alert_id AS source_id,
                type(cr) AS relationship,
                'Transaction' AS target_type,
                t.transaction_id AS target_id,
                NULL AS role

            UNION ALL

            MATCH (t:Transaction)-[fr:FROM]->(a:Account)
            MATCH (s:Scenario {alert_id: $alert_id})
                  -[:CONTAINS_TRANSACTION]->(t)
            RETURN
                'Transaction' AS source_type,
                t.transaction_id AS source_id,
                type(fr) AS relationship,
                'Account' AS target_type,
                a.account_id AS target_id,
                NULL AS role

            UNION ALL

            MATCH (t:Transaction)-[tr:TO]->(a:Account)
            MATCH (s:Scenario {alert_id: $alert_id})
                  -[:CONTAINS_TRANSACTION]->(t)
            RETURN
                'Transaction' AS source_type,
                t.transaction_id AS source_id,
                type(tr) AS relationship,
                'Account' AS target_type,
                a.account_id AS target_id,
                NULL AS role

            UNION ALL

            MATCH (t:Transaction)-[hc:HAS_CONFIRMATION]->(c:Confirmation)
            MATCH (s:Scenario {alert_id: $alert_id})
                  -[:CONTAINS_TRANSACTION]->(t)
            RETURN
                'Transaction' AS source_type,
                t.transaction_id AS source_id,
                type(hc) AS relationship,
                'Confirmation' AS target_type,
                c.transaction_id AS target_id,
                NULL AS role

            UNION ALL

            MATCH (c:Confirmation)-[pb:PROCESSED_BY]->(bs:BankServer)
            MATCH (t:Transaction)-[:HAS_CONFIRMATION]->(c)
            MATCH (s:Scenario {alert_id: $alert_id})
                  -[:CONTAINS_TRANSACTION]->(t)
            RETURN
                'Confirmation' AS source_type,
                c.transaction_id AS source_id,
                type(pb) AS relationship,
                'BankServer' AS target_type,
                bs.server_id AS target_id,
                NULL AS role
            """,
            alert_id=alert_id,
        )

        for record in rel_result:
            source_type = record["source_type"]
            source_id = record["source_id"]
            target_type = record["target_type"]
            target_id = record["target_id"]

            source_prefix = {
                "Scenario": "scenario",
                "Account": "account",
                "Transaction": "transaction",
                "Confirmation": "confirmation",
                "BankServer": "server",
            }[source_type]

            target_prefix = {
                "Scenario": "scenario",
                "Account": "account",
                "Transaction": "transaction",
                "Confirmation": "confirmation",
                "BankServer": "server",
            }[target_type]

            edges.append(
                {
                    "id": (
                        f"{source_prefix}:{source_id}"
                        f"-{record['relationship']}-"
                        f"{target_prefix}:{target_id}"
                    ),
                    "source": f"{source_prefix}:{source_id}",
                    "target": f"{target_prefix}:{target_id}",
                    "relationship": record["relationship"],
                    "role": record["role"],
                }
            )

    nodes[f"scenario:{alert_id}"] = {
        "id": f"scenario:{alert_id}",
        "type": "Scenario",
        "data": scenario,
    }

    return {
        "scenario": scenario,
        "nodes": list(nodes.values()),
        "edges": edges,
    }


# ============================================================
# TRACE / SEARCH
# ============================================================

@app.get("/api/trace")
def trace(q: str = Query(..., min_length=1)):
    query = q.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Query is required")

    with driver.session() as session:
        scenario_hit = session.run(
            """
            MATCH (s:Scenario)
            WHERE toLower(s.alert_id) = toLower($q)
               OR toLower(s.alert_id) CONTAINS toLower($q)
            RETURN s.alert_id AS alert_id
            ORDER BY
                CASE WHEN toLower(s.alert_id) = toLower($q) THEN 0 ELSE 1 END,
                s.detected_at DESC
            LIMIT 8
            """,
            q=query,
        )
        scenario_ids = [record["alert_id"] for record in scenario_hit]

        tx_hit = session.run(
            """
            MATCH (t:Transaction)
            WHERE toLower(t.transaction_id) = toLower($q)
               OR toLower(t.transaction_id) CONTAINS toLower($q)
            OPTIONAL MATCH (t)-[:FROM]->(src:Account)
            OPTIONAL MATCH (t)-[:TO]->(dst:Account)
            RETURN
                t.transaction_id AS transaction_id,
                t.timestamp AS timestamp,
                t.amount AS amount,
                t.currency AS currency,
                t.mode AS mode,
                t.status AS status,
                src.account_id AS source_account,
                dst.account_id AS destination_account
            ORDER BY
                CASE WHEN toLower(t.transaction_id) = toLower($q) THEN 0 ELSE 1 END
            LIMIT 8
            """,
            q=query,
        )
        transactions = [record_to_dict(record) for record in tx_hit]

        account_hit = session.run(
            """
            MATCH (a:Account)
            WHERE toLower(a.account_id) = toLower($q)
               OR toLower(a.account_id) CONTAINS toLower($q)
            RETURN a.account_id AS account_id
            ORDER BY
                CASE WHEN toLower(a.account_id) = toLower($q) THEN 0 ELSE 1 END
            LIMIT 8
            """,
            q=query,
        )
        account_ids = [record["account_id"] for record in account_hit]

        related_scenarios = []
        if account_ids:
            related = session.run(
                """
                MATCH (s:Scenario)-[rel:INVOLVES]->(a:Account)
                WHERE a.account_id IN $ids
                OPTIONAL MATCH (s)-[:CONTAINS_TRANSACTION]->(:Transaction)
                              -[:HAS_CONFIRMATION]->(c:Confirmation)
                WITH s, rel, a,
                     [state IN collect(DISTINCT c.state) WHERE state IS NOT NULL] AS states
                RETURN DISTINCT
                    s.alert_id AS alert_id,
                    s.pattern AS pattern,
                    s.detected_at AS detected_at,
                    s.source_account AS source_account,
                    s.mule_account AS mule_account,
                    s.final_accounts AS final_accounts,
                    a.account_id AS matched_account,
                    rel.role AS role,
                    states
                ORDER BY s.detected_at DESC
                LIMIT 50
                """,
                ids=account_ids,
            )
            related_scenarios = [record_to_dict(record) for record in related]

        if scenario_ids and not related_scenarios:
            listed = session.run(
                """
                MATCH (s:Scenario)
                WHERE s.alert_id IN $ids
                OPTIONAL MATCH (s)-[:CONTAINS_TRANSACTION]->(:Transaction)
                              -[:HAS_CONFIRMATION]->(c:Confirmation)
                WITH s, [state IN collect(DISTINCT c.state) WHERE state IS NOT NULL] AS states
                RETURN
                    s.alert_id AS alert_id,
                    s.pattern AS pattern,
                    s.detected_at AS detected_at,
                    s.source_account AS source_account,
                    s.mule_account AS mule_account,
                    s.final_accounts AS final_accounts,
                    states
                ORDER BY s.detected_at DESC
                """,
                ids=scenario_ids,
            )
            related_scenarios = [record_to_dict(record) for record in listed]

        account_detail = None
        exact_account = next(
            (
                account_id
                for account_id in account_ids
                if account_id.lower() == query.lower()
            ),
            account_ids[0] if len(account_ids) == 1 else None,
        )
        if exact_account:
            account_detail = _load_account(session, exact_account)

        transaction_detail = None
        if transactions and (
            transactions[0]["transaction_id"].lower() == query.lower()
            or len(transactions) == 1
        ):
            transaction_detail = transactions[0]

        if account_detail:
            match_type = "account"
        elif transaction_detail:
            match_type = "transaction"
        elif related_scenarios:
            match_type = "scenario"
        elif account_ids or transactions:
            match_type = "partial"
        else:
            match_type = "none"

    return {
        "query": query,
        "match_type": match_type,
        "account": account_detail,
        "accounts": account_ids,
        "transaction": transaction_detail,
        "transactions": transactions,
        "scenarios": related_scenarios,
    }


def _load_account(session, account_id: str) -> Dict[str, Any]:
    account_record = session.run(
        """
        MATCH (a:Account {account_id: $account_id})
        RETURN a
        """,
        account_id=account_id,
    ).single()
    if not account_record:
        return {"account": {"account_id": account_id}, "outgoing": [], "incoming": []}

    outgoing = session.run(
        """
        MATCH (a:Account {account_id: $account_id})
              <-[:FROM]-(t:Transaction)-[:TO]->(dst:Account)
        RETURN
            t.transaction_id AS transaction_id,
            t.timestamp AS timestamp,
            t.amount AS amount,
            t.currency AS currency,
            t.mode AS mode,
            dst.account_id AS destination_account
        ORDER BY t.timestamp
        """,
        account_id=account_id,
    )
    incoming = session.run(
        """
        MATCH (src:Account)<-[:FROM]-(t:Transaction)
              -[:TO]->(a:Account {account_id: $account_id})
        RETURN
            t.transaction_id AS transaction_id,
            t.timestamp AS timestamp,
            t.amount AS amount,
            t.currency AS currency,
            t.mode AS mode,
            src.account_id AS source_account
        ORDER BY t.timestamp
        """,
        account_id=account_id,
    )
    return {
        "account": node_to_dict(account_record["a"]),
        "outgoing": [record_to_dict(r) for r in outgoing],
        "incoming": [record_to_dict(r) for r in incoming],
    }


# ============================================================
# ACCOUNTS
# ============================================================

@app.get("/api/accounts/{account_id}")
def get_account(account_id: str):
    with driver.session() as session:
        account_record = session.run(
            """
            MATCH (a:Account {account_id: $account_id})
            RETURN a
            """,
            account_id=account_id,
        ).single()

        if not account_record:
            raise HTTPException(status_code=404, detail="Account not found")

        payload = _load_account(session, account_id)
    return payload


# ============================================================
# TRANSACTIONS
# ============================================================

@app.get("/api/transactions/{transaction_id}")
def get_transaction(transaction_id: str):
    with driver.session() as session:
        record = session.run(
            """
            MATCH (t:Transaction {transaction_id: $transaction_id})
            OPTIONAL MATCH (t)-[:FROM]->(src:Account)
            OPTIONAL MATCH (t)-[:TO]->(dst:Account)
            OPTIONAL MATCH (t)-[:HAS_CONFIRMATION]->(c:Confirmation)
            OPTIONAL MATCH (c)-[pb:PROCESSED_BY]->(bs:BankServer)
            RETURN
                t,
                src.account_id AS source_account,
                dst.account_id AS destination_account,
                c AS confirmation,
                collect(
                    CASE
                        WHEN bs IS NULL THEN NULL
                        ELSE {
                            server_id: bs.server_id,
                            latitude: bs.latitude,
                            longitude: bs.longitude,
                            server_latency_ms: pb.server_latency_ms,
                            processing_latency_ms: pb.processing_latency_ms,
                            confirmation_latency_ms: pb.confirmation_latency_ms
                        }
                    END
                ) AS servers
            """,
            transaction_id=transaction_id,
        ).single()

    if not record:
        raise HTTPException(status_code=404, detail="Transaction not found")

    confirmation = record["confirmation"]
    confirmation_data = node_to_dict(confirmation) if confirmation else None
    if confirmation_data and not confirmation_data.get("state"):
        confirmation_data["state"] = infer_state(
            confirmation_data.get("device_latitude"),
            confirmation_data.get("device_longitude"),
        )

    return {
        "transaction": node_to_dict(record["t"]),
        "source_account": record["source_account"],
        "destination_account": record["destination_account"],
        "confirmation": confirmation_data,
        "servers": [s for s in record["servers"] if s is not None],
    }


# ============================================================
# RUN DIRECTLY
# ============================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "backend:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
