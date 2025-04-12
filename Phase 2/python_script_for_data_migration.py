import psycopg2
from neo4j import GraphDatabase

# === CONFIGURATION ===
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

# === DATA MIGRATION ===

def migrate_data():
    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
    pg_cur = pg_conn.cursor()
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with neo4j_driver.session() as session:
        # 1. Migrate Authors
        pg_cur.execute("SELECT author_id, author_name FROM Author")
        for author_id, author_name in pg_cur.fetchall():
            session.run("""
                MERGE (a:Author {author_id: $author_id})
                SET a.name = $author_name
            """, author_id=author_id, author_name=author_name)

        # 2. Migrate Books
        pg_cur.execute("SELECT book_id, title, first_publish_year, cover_edition_key, has_fulltext FROM Book")
        for row in pg_cur.fetchall():
            session.run("""
                MERGE (b:Book {book_id: $book_id})
                SET b.title = $title,
                    b.first_publish_year = $first_publish_year,
                    b.cover_edition_key = $cover_edition_key,
                    b.has_fulltext = $has_fulltext
            """, book_id=row[0], title=row[1], first_publish_year=row[2],
                 cover_edition_key=row[3], has_fulltext=row[4])

        # 3. Migrate Book-Author relationships
        pg_cur.execute("SELECT book_id, author_id FROM Book_Author")
        for book_id, author_id in pg_cur.fetchall():
            session.run("""
                MATCH (b:Book {book_id: $book_id})
                MATCH (a:Author {author_id: $author_id})
                MERGE (a)-[:WROTE]->(b)
            """, book_id=book_id, author_id=author_id)

        # 4. Migrate Book Editions
        pg_cur.execute("SELECT edition_id, book_id, edition_number, edition_year, language FROM Book_Edition")
        for edition_id, book_id, edition_number, edition_year, language in pg_cur.fetchall():
            session.run("""
                MERGE (e:BookEdition {edition_id: $edition_id})
                SET e.edition_number = $edition_number,
                    e.edition_year = $edition_year,
                    e.language = $language
                WITH e
                MATCH (b:Book {book_id: $book_id})
                MERGE (b)-[:HAS_EDITION]->(e)
            """, edition_id=edition_id, book_id=book_id,
                 edition_number=edition_number, edition_year=edition_year, language=language)

        # 5. Migrate Archive Documents
        pg_cur.execute("SELECT doc_id, identifier, title, creator, year, language, downloads, subject FROM Archive_Document")
        for row in pg_cur.fetchall():
            session.run("""
                MERGE (d:ArchiveDocument {doc_id: $doc_id})
                SET d.identifier = $identifier,
                    d.title = $title,
                    d.creator = $creator,
                    d.year = $year,
                    d.language = $language,
                    d.downloads = $downloads,
                    d.subject = $subject
            """, doc_id=row[0], identifier=row[1], title=row[2], creator=row[3],
                 year=row[4], language=row[5], downloads=row[6], subject=row[7])

        # 6. Migrate Archive Stats
        pg_cur.execute("SELECT stat_id, doc_id, last_access_date, daily_downloads FROM Archive_Stats")
        for stat_id, doc_id, last_access_date, daily_downloads in pg_cur.fetchall():
            session.run("""
                MERGE (s:ArchiveStats {stat_id: $stat_id})
                SET s.last_access_date = $last_access_date,
                    s.daily_downloads = $daily_downloads
                WITH s
                MATCH (d:ArchiveDocument {doc_id: $doc_id})
                MERGE (d)-[:HAS_STATS]->(s)
            """, stat_id=stat_id, doc_id=doc_id,
                 last_access_date=str(last_access_date), daily_downloads=daily_downloads)

        # 7. Migrate Book-Archive Links
        pg_cur.execute("SELECT book_id, doc_id FROM Book_Archive_Link")
        for book_id, doc_id in pg_cur.fetchall():
            session.run("""
                MATCH (b:Book {book_id: $book_id})
                MATCH (d:ArchiveDocument {doc_id: $doc_id})
                MERGE (b)-[:LINKED_TO]->(d)
            """, book_id=book_id, doc_id=doc_id)

    pg_cur.close()
    pg_conn.close()
    neo4j_driver.close()
    print("Data migration completed successfully!")

# === MAIN ===
if __name__ == "__main__":
    migrate_data()
