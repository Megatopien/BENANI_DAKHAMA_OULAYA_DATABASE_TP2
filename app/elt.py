import os
import time
from pathlib import Path
from typing import Iterable, List

import pandas as pd
from sqlalchemy import create_engine, text
from neo4j import GraphDatabase


# --------------------------
# Config (via env vars)
# --------------------------
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "shop")
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4j123")


# --------------------------
# Helpers
# --------------------------
def chunk(df: pd.DataFrame, size: int) -> Iterable[pd.DataFrame]:
    """Yield successive DataFrame chunks of given size."""
    for start in range(0, len(df), size):
        yield df.iloc[start : start + size].copy()


def wait_for_postgres(timeout_sec: int = 120):
    """Wait until Postgres is ready to accept connections."""
    url = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    deadline = time.time() + timeout_sec
    last_err = None
    while time.time() < deadline:
        try:
            engine = create_engine(url, future=True)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception as e:
            last_err = e
            time.sleep(2)
    raise RuntimeError(f"Postgres not ready after {timeout_sec}s: {last_err}")


def wait_for_neo4j(timeout_sec: int = 120):
    """Wait until Neo4j is ready (Bolt connection ok)."""
    deadline = time.time() + timeout_sec
    last_err = None
    while time.time() < deadline:
        try:
            with GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)) as driver:
                with driver.session() as sess:
                    sess.run("RETURN 1").consume()
            return
        except Exception as e:
            last_err = e
            time.sleep(2)
    raise RuntimeError(f"Neo4j not ready after {timeout_sec}s: {last_err}")


def run_cypher(query: str, parameters: dict | None = None):
    """Execute a single Cypher query."""
    with GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)) as driver:
        with driver.session() as session:
            return session.run(query, parameters or {}).consume()


def run_cypher_file(path: Path):
    """Execute multiple Cypher statements from a file (split on ';')."""
    text_content = path.read_text(encoding="utf-8")
    statements = [stmt.strip() for stmt in text_content.split(";") if stmt.strip()]
    for stmt in statements:
        run_cypher(stmt)


# --------------------------
# ETL principal
# --------------------------
def etl():
    """
    Main ETL function that migrates data from PostgreSQL to Neo4j.

    Steps:
    1) Wait for Postgres and Neo4j
    2) Setup Neo4j schema (queries.cypher)
    3) Extract tables from Postgres
    4) Transform to graph model
    5) Load nodes and relationships to Neo4j
    """

    # 1) Wait for services
    wait_for_postgres()
    wait_for_neo4j()

    # 2) Schema Neo4j
    queries_path = Path(__file__).with_name("queries.cypher")
    run_cypher_file(queries_path)

    # 3) Extract
    pg_url = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    engine = create_engine(pg_url, future=True)

    customers = pd.read_sql("SELECT * FROM customers", engine)
    categories = pd.read_sql("SELECT * FROM categories", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders = pd.read_sql("SELECT * FROM orders", engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)
    events = pd.read_sql("SELECT * FROM events", engine)

    # 4) Load (we can transform inline; small dataset => direct mappings)

    # 4.1 Categories
    cy = """
    UNWIND $rows AS row
    MERGE (c:Category {id: row.id})
    SET c.name = row.name
    """
    run_cypher(cy, {"rows": categories.to_dict("records")})

    # 4.2 Products + IN_CATEGORY
    cy = """
    UNWIND $rows AS row
    MERGE (p:Product {id: row.id})
      ON CREATE SET p.name = row.name, p.price = toFloat(row.price)
      ON MATCH  SET p.name = row.name, p.price = toFloat(row.price)
    WITH row, p
    MATCH (c:Category {id: row.category_id})
    MERGE (p)-[:IN_CATEGORY]->(c)
    """
    run_cypher(cy, {"rows": products.to_dict("records")})

    # 4.3 Customers
    cy = """
    UNWIND $rows AS row
    MERGE (c:Customer {id: row.id})
    SET c.name = row.name, c.join_date = row.join_date
    """
    run_cypher(cy, {"rows": customers.to_dict("records")})

    # 4.4 Orders + PLACED
    cy = """
    UNWIND $rows AS row
    MERGE (o:Order {id: row.id})
    SET o.ts = row.ts
    WITH row, o
    MATCH (c:Customer {id: row.customer_id})
    MERGE (c)-[:PLACED]->(o)
    """
    run_cypher(cy, {"rows": orders.to_dict("records")})

    #4.5 Order Items -> CONTAINS
    item_rows = order_items.to_dict("records")
    cy = """
    UNWIND $rows AS row
    MATCH (o:Order {id: row.order_id})
    MATCH (p:Product {id: row.product_id})
    MERGE (o)-[r:CONTAINS]->(p)
    SET r.quantity = toInteger(row.quantity)
    """
    for part in chunk(pd.DataFrame(item_rows), size=1000):
        run_cypher(cy, {"rows": part.to_dict("records")})

    #4.6 Events -> dynamic relationships 
    events["rel"] = events["event_type"].map(
        {"view": "VIEWED", "click": "CLICKED", "add_to_cart": "ADDED_TO_CART"}
    )
    cy = """
    UNWIND $rows AS row
    MATCH (c:Customer {id: row.customer_id})
    MATCH (p:Product {id: row.product_id})
    CALL apoc.create.relationship(c, row.rel, {ts: row.ts}, p) YIELD rel
    RETURN count(rel) as created
    """
    for part in chunk(events[["customer_id", "product_id", "ts", "rel"]], size=1000):
        run_cypher(cy, {"rows": part.to_dict("records")})

    print("ETL completed successfully.")


if __name__ == "__main__":
    etl()
