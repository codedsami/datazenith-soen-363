import psycopg2
from neo4j import GraphDatabase

# ----- CONFIG -----
POSTGRES_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "dbname": "phase1",
    "user": "miskatsami",
    "password": "sami2001"
}

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "soen363phase2" 

# -------------------

def get_tables_and_fks():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()

    # Get tables
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
    """)
    tables = [row[0] for row in cur.fetchall()]

    # Get foreign keys
    cur.execute("""
        SELECT
            tc.table_name AS source_table,
            ccu.table_name AS target_table
        FROM
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY';
    """)
    foreign_keys = cur.fetchall()

    cur.close()
    conn.close()

    return tables, foreign_keys

def create_structure_in_neo4j(tables, foreign_keys):
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session() as session:
        # Create dummy nodes for each table
        for table in tables:
            session.run(f"MERGE (:{table} {{dummy: true}})")

        # Create dummy relationships for foreign keys
        for source_table, target_table in foreign_keys:
            cypher = f"""
            MATCH (a:{source_table} {{dummy: true}})
            MATCH (b:{target_table} {{dummy: true}})
            MERGE (a)-[:RELATED_TO]->(b)
            """
            session.run(cypher)

    driver.close()
    print("Graph structure created in Neo4j!")

if __name__ == "__main__":
    print("Fetching schema from PostgreSQL...")
    tables, foreign_keys = get_tables_and_fks()
    print(f"Found {len(tables)} tables and {len(foreign_keys)} relationships.")
    print("Creating structure in Neo4j...")
    create_structure_in_neo4j(tables, foreign_keys)
