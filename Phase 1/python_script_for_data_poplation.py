import requests
import pg8000
import sqlparse

# ============================
# Configuration
# ============================
DB_CONFIG = {
    'user': 'soen',
    'password': '363',
    'host': 'localhost',
    'port': 5432,
    'database': 'soen-363-p01-db'
}

# API URLs
ARCHIVE_API_URL = (
    "https://archive.org/advancedsearch.php?"
    "q=mediatype:texts&fl=identifier,title,creator,year,"
    "language,subject,downloads&rows=30000&page=1&output=json"
)

# Base OpenLibrary API URL
OPENLIBRARY_API_URL = "https://openlibrary.org/search.json?q=subject:{subject}&limit=30000"


# ============================
# Database Utilities
# ============================
def get_db_connection():
    """Create and return a new database connection using pg8000."""
    return pg8000.connect(
        **DB_CONFIG,
        timeout=120
    )



def insert_data(table, data, columns):
    """
    Insert a list of dictionaries into a table.
    Uses 'ON CONFLICT DO NOTHING' to avoid duplicate insertion.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join(columns)

    for row in data:
        values = [row.get(col) for col in columns]
        try:
            query = (
                f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) "
                "ON CONFLICT DO NOTHING"
            )
            cursor.execute(query, values)
        except Exception as e:
            print(f"Error inserting into {table}: {e}")
            continue
    conn.commit()
    cursor.close()
    conn.close()


def run_sql_file(filepath):
    """
    Run SQL statements from a file.
    Uses sqlparse to correctly split statements, handling dollar-quoted functions.
    """
    try:
        with open(filepath, "r") as f:
            sql_content = f.read()
        conn = get_db_connection()
        cursor = conn.cursor()
        statements = sqlparse.split(sql_content)
        for stmt in statements:
            stmt = stmt.strip()
            if stmt:
                try:
                    cursor.execute(stmt)
                except Exception as e:
                    print(f"Failed executing statement:\n{stmt}\nError: {e}")
        conn.commit()
        cursor.close()
        conn.close()
        print(f"SQL script '{filepath}' executed successfully.")
    except Exception as e:
        print(f"Error running SQL file '{filepath}': {e}")


def query_data(sql, params=None):
    """
    Execute a query and return results as a list of dictionaries.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        results = [dict(zip(col_names, row)) for row in rows]
        return results
    except Exception as e:
        print(f"Error executing query: {sql}\nError: {e}")
        return []
    finally:
        cursor.close()
        conn.close()


# ============================
# API Fetching Functions
# ============================
def get_archive_data(subject):
    """
    Fetch and return document list from Archive.org for a specific subject.
    """
    url = ARCHIVE_API_URL + f"&q=subject:{subject}"
    try:
        response = requests.get(url, timeout=3000)  
        response.raise_for_status()
        data = response.json()  
        return data['response']['docs'] 
    except requests.exceptions.RequestException as err:
        print(f"Error fetching data from Archive.org for subject {subject}: {err}")
        return []


def get_openlibrary_data(subject):
    url = OPENLIBRARY_API_URL.format(subject=subject)
    try:
        response = requests.get(url, timeout=3000) 
        response.raise_for_status() 
        data = response.json()  
        return data['docs'] 
    except requests.exceptions.RequestException as err:
        print(f"Error fetching data from OpenLibrary for subject {subject}: {err}")
        return []


# ============================
# Database Population Functions
# ============================
def populate_archive_data(archive_docs):
    """Process Archive.org data and insert into Archive_Document table."""
    archive_records = []
    for doc in archive_docs:
        identifier = doc.get('identifier')
        title = doc.get('title')
        creator = doc.get('creator')
        year = doc.get('year')
        language = doc.get('language')
        downloads = doc.get('downloads')
        subjects = doc.get('subject', "")
        if isinstance(subjects, list):
            subjects = ', '.join(subjects)
        record = {
            'identifier': identifier,
            'title': title,
            'creator': creator,
            'year': year,
            'language': language,
            'subject': subjects,
            'downloads': downloads
        }
        archive_records.append(record)
    columns = ['identifier', 'title', 'creator', 'year', 'language', 'subject', 'downloads']
    insert_data('Archive_Document', archive_records, columns)


def populate_openlibrary_data(openlibrary_docs):
    """Process OpenLibrary data and insert into Book, Author, and Book_Author tables."""
    # --- Insert Books ---
    book_records = []
    for book in openlibrary_docs:
        cover_edition_key = book.get('cover_edition_key')
        if cover_edition_key:
            record = {
                'title': book.get('title'),
                'first_publish_year': book.get('first_publish_year'),
                'cover_edition_key': cover_edition_key,
                'has_fulltext': book.get('has_fulltext', False)
            }
            book_records.append(record)
    book_columns = ['title', 'first_publish_year', 'cover_edition_key', 'has_fulltext']
    insert_data('Book', book_records, book_columns)

    # --- Insert Authors ---
    author_records = []
    authors_set = set()
    for book in openlibrary_docs:
        for author in book.get('author_name', []):
            if author not in authors_set:
                authors_set.add(author)
                author_records.append({'author_name': author})
    author_columns = ['author_name']
    insert_data('Author', author_records, author_columns)

    # --- Insert Book-Author Relationships ---
    conn = get_db_connection()
    cursor = conn.cursor()
    for book in openlibrary_docs:
        cover_edition_key = book.get('cover_edition_key')
        if not cover_edition_key:
            continue
        cursor.execute("SELECT book_id FROM Book WHERE cover_edition_key = %s", (cover_edition_key,))
        book_row = cursor.fetchone()
        if not book_row:
            continue
        book_id = book_row[0]
        for author_key in book.get('author_key', []):
            cursor.execute("SELECT author_id FROM Author WHERE author_name = %s", (author_key,))
            author_row = cursor.fetchone()
            if not author_row:
                cursor.execute("INSERT INTO Author (author_name) VALUES (%s) RETURNING author_id", (author_key,))
                author_id = cursor.fetchone()[0]
            else:
                author_id = author_row[0]
            cursor.execute("SELECT 1 FROM Book_Author WHERE book_id = %s AND author_id = %s", (book_id, author_id))
            if not cursor.fetchone():
                cursor.execute("INSERT INTO Book_Author (book_id, author_id) VALUES (%s, %s)", (book_id, author_id))
    conn.commit()
    cursor.close()
    conn.close()


def populate_book_edition(openlibrary_docs):
    """
    Populate Book_Edition using data from OpenLibrary.
    For each book, if 'edition_count', 'first_publish_year', and 'language' are available,
    insert a record using those fetched values.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    for book in openlibrary_docs:
        cover_edition_key = book.get('cover_edition_key')
        if not cover_edition_key:
            continue
        cursor.execute("SELECT book_id FROM Book WHERE cover_edition_key = %s", (cover_edition_key,))
        row = cursor.fetchone()
        if not row:
            continue
        book_id = row[0]
        edition_count = book.get('edition_count')
        first_publish_year = book.get('first_publish_year')
        language = book.get('language')
        # If language is a list, take the first element
        if isinstance(language, list) and language:
            language = language[0]
        # Only insert if all required values are available and valid
        if edition_count and edition_count > 0 and first_publish_year and language:
            try:
                cursor.execute(
                    "INSERT INTO Book_Edition (book_id, edition_number, edition_year, language) "
                    "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    (book_id, edition_count, first_publish_year, language)
                )
            except Exception as e:
                print(f"Error inserting into Book_Edition for book_id {book_id}: {e}")
    conn.commit()
    cursor.close()
    conn.close()


def populate_book_archive_link():
    """Populate Book_Archive_Link by linking Books to Archive_Documents using a title match."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT book_id, title FROM Book")
    books = cursor.fetchall()

    cursor.execute("SELECT doc_id, title FROM Archive_Document")
    archive_docs = cursor.fetchall()

    archive_doc_titles = {doc_title.lower(): doc_id for doc_id, doc_title in archive_docs}

    links_to_insert = []

    for book_id, book_title in books:
        matching_docs = [
            doc_id for doc_title, doc_id in archive_doc_titles.items() if book_title.lower() in doc_title.lower()
        ]

        for doc_id in matching_docs:
            cursor.execute("SELECT 1 FROM Book_Archive_Link WHERE book_id = %s AND doc_id = %s", (book_id, doc_id))
            if not cursor.fetchone():
                links_to_insert.append((book_id, doc_id))

    if links_to_insert:
        cursor.executemany("INSERT INTO Book_Archive_Link (book_id, doc_id) VALUES (%s, %s)", links_to_insert)

    conn.commit()
    cursor.close()
    conn.close()



# ============================
# Main Function
# ============================
import concurrent.futures

def fetch_and_populate(subject):
    """Fetch and populate data for a given subject."""
    print(f"Fetching data for subject: {subject}...")

    try:
        print(f"Fetching Archive.org data for subject: {subject}...")
        archive_docs = get_archive_data(subject)

        print(f"Fetching OpenLibrary data for subject: {subject}...")
        openlibrary_docs = get_openlibrary_data(subject)

        print(f"Populating Archive data for subject: {subject}...")
        populate_archive_data(archive_docs)

        print(f"Populating OpenLibrary data for subject: {subject}...")
        populate_openlibrary_data(openlibrary_docs)

        print(f"Populating Book Editions for subject: {subject}...")
        populate_book_edition(openlibrary_docs)

        print(f"Linking Books to Archive Documents for subject: {subject}...")
        populate_book_archive_link()

        print(f"Data populated successfully for subject: {subject}!\n")
    
    except Exception as e:
        print(f"Error occurred for subject {subject}: {e}")


def main():
    print("Starting database population...")

    run_sql_file("DDL_aka_requirements.sql")

    subjects = [
        "general science",
        "literature",
        "world history",
        "technology",
        "art",
        "mathematics",
        "philosophy",
        "engineering",
        "biology",
        "music",
        "economics",
        "psychology",
        "sociology",
        "health",
        "environmental science",
        "political science"
    ]

    for subject in subjects:
        fetch_and_populate(subject)

    print("Database population completed successfully for all subjects!")


    
if __name__ == "__main__":
    main()