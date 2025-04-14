import psycopg2
from neo4j import GraphDatabase

# === CONFIGURATION ===
POSTGRES_CONFIG = {
    'user': 'soen',
    'password': '363',
    'host': 'localhost',
    'port': 5432,
    'database': 'soen-363-p01-db'
}

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "soen363phase2"

# === UTILITIES ===

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

def fetch_and_chunk(pg_cur, query, row_to_dict, chunk_size=1000):
    pg_cur.execute(query)
    while True:
        rows = pg_cur.fetchmany(chunk_size)
        if not rows:
            break
        yield [row_to_dict(row) for row in rows]
# === DATA MIGRATION ===

def migrate_data():
    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
    pg_cur = pg_conn.cursor()
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with neo4j_driver.session() as session:

        # 1. Migrate Authors
        count = 0
        for authors in fetch_and_chunk(pg_cur, "SELECT author_id, author_name FROM Author",
                                       lambda row: {"author_id": row[0], "name": row[1]}):
            session.run("""
                UNWIND $authors AS author
                MERGE (a:Author {author_id: author.author_id})
                SET a.name = author.name
            """, authors=authors)
            count += len(authors)
        print(f"Authors migrated: {count}")

        # 2. Migrate Books
        count = 0
        for books in fetch_and_chunk(pg_cur, "SELECT book_id, title, first_publish_year, cover_edition_key, has_fulltext FROM Book",
                                     lambda row: {
                                         "book_id": row[0], "title": row[1],
                                         "first_publish_year": row[2],
                                         "cover_edition_key": row[3], "has_fulltext": row[4]
                                     }):
            session.run("""
                UNWIND $books AS book
                MERGE (b:Book {book_id: book.book_id})
                SET b.title = book.title,
                    b.first_publish_year = book.first_publish_year,
                    b.cover_edition_key = book.cover_edition_key,
                    b.has_fulltext = book.has_fulltext
            """, books=books)
            count += len(books)
        print(f"Books migrated: {count}")

        # 3. Migrate Book-Author relationships
        count = 0
        for links in fetch_and_chunk(pg_cur, "SELECT book_id, author_id FROM Book_Author",
                                     lambda row: {"book_id": row[0], "author_id": row[1]}):
            session.run("""
                UNWIND $links AS link
                MATCH (b:Book {book_id: link.book_id})
                MATCH (a:Author {author_id: link.author_id})
                MERGE (a)-[:WROTE]->(b)
            """, links=links)
            count += len(links)
        print(f"Book-Author links created: {count}")

        # 4. Migrate Book Editions
        count = 0
        for editions in fetch_and_chunk(pg_cur, "SELECT edition_id, book_id, edition_number, edition_year, language FROM Book_Edition",
                                        lambda row: {
                                            "edition_id": row[0], "book_id": row[1],
                                            "edition_number": row[2], "edition_year": row[3],
                                            "language": row[4]
                                        }):
            session.run("""
                UNWIND $editions AS edition
                MERGE (e:BookEdition {edition_id: edition.edition_id})
                SET e.edition_number = edition.edition_number,
                    e.edition_year = edition.edition_year,
                    e.language = edition.language
                WITH e, edition
                MATCH (b:Book {book_id: edition.book_id})
                MERGE (b)-[:HAS_EDITION]->(e)
            """, editions=editions)
            count += len(editions)
        print(f"Book Editions migrated: {count}")

        # 5. Migrate Archive Documents
        count = 0
        for docs in fetch_and_chunk(pg_cur, "SELECT doc_id, identifier, title, creator, year, language, downloads, subject FROM Archive_Document",
                                    lambda row: {
                                        "doc_id": row[0], "identifier": row[1],
                                        "title": row[2], "creator": row[3], "year": row[4],
                                        "language": row[5], "downloads": row[6], "subject": row[7]
                                    }):
            session.run("""
                UNWIND $docs AS doc
                MERGE (d:ArchiveDocument {doc_id: doc.doc_id})
                SET d.identifier = doc.identifier,
                    d.title = doc.title,
                    d.creator = doc.creator,
                    d.year = doc.year,
                    d.language = doc.language,
                    d.downloads = doc.downloads,
                    d.subject = doc.subject
            """, docs=docs)
            count += len(docs)
        print(f"Archive Documents migrated: {count}")

        # 6. Migrate Archive Stats
        count = 0
        for stats in fetch_and_chunk(pg_cur, "SELECT stat_id, doc_id, last_access_date, daily_downloads FROM Archive_Stats",
                                     lambda row: {
                                         "stat_id": row[0], "doc_id": row[1],
                                         "last_access_date": str(row[2]),
                                         "daily_downloads": row[3]
                                     }):
            session.run("""
                UNWIND $stats AS stat
                MERGE (s:ArchiveStats {stat_id: stat.stat_id})
                SET s.last_access_date = stat.last_access_date,
                    s.daily_downloads = stat.daily_downloads
                WITH s, stat
                MATCH (d:ArchiveDocument {doc_id: stat.doc_id})
                MERGE (d)-[:HAS_STATS]->(s)
            """, stats=stats)
            count += len(stats)
        print(f"Archive Stats migrated: {count}")

        # 7. Migrate Book-Archive Links
        count = 0
        for links in fetch_and_chunk(pg_cur, "SELECT book_id, doc_id FROM Book_Archive_Link",
                                     lambda row: {"book_id": row[0], "doc_id": row[1]}):
            session.run("""
                UNWIND $links AS link
                MATCH (b:Book {book_id: link.book_id})
                MATCH (d:ArchiveDocument {doc_id: link.doc_id})
                MERGE (b)-[:LINKED_TO]->(d)
            """, links=links)
            count += len(links)
        print(f"Book-Archive links created: {count}")

    pg_cur.close()
    pg_conn.close()
    neo4j_driver.close()
    print("Data migration completed successfully!")
# === MAIN ===
if __name__ == "__main__":
    migrate_data()