import requests
import psycopg2
import json

# Database connection parameters
DB_HOST = 'localhost'  # Change to your database host
DB_NAME = 'your_database_name' 
DB_USER = 'your_database_user' 
DB_PASSWORD = 'your_database_password' 

# API URLs
ARCHIVE_API_URL = "https://archive.org/advancedsearch.php?q=mediatype:texts&fl=identifier,title,creator,year,language,subject,downloads&rows=10000&page=1&output=json"
OPENLIBRARY_API_URL = "https://openlibrary.org/search.json?q=subject:Science+&limit=10000"

# Function to get data from Archive API
def get_archive_data():
    response = requests.get(ARCHIVE_API_URL)
    data = response.json()  # Parsing the JSON data from Archive.org
    return data['response']['docs']  # Extracting the document list

# Function to get data from OpenLibrary API
def get_openlibrary_data():
    response = requests.get(OPENLIBRARY_API_URL)
    data = response.json()  # Parsing the JSON data from OpenLibrary
    return data['docs']  # Extracting the document list

# Function to connect to the database
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

# Function to insert data into the Archive_Document table
def insert_archive_data(conn, archive_data):
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO Archive_Document (identifier, title, creator, year, language, subject, downloads)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    for doc in archive_data:
        identifier = doc.get('identifier', '')
        title = doc.get('title', '')
        creator = doc.get('creator', [''])[0]
        year = doc.get('year', 0)
        language = doc.get('language', [''])[0]
        subject = doc.get('subject', [])
        downloads = doc.get('downloads', 0)

        cursor.execute(insert_query, (identifier, title, creator, year, language, subject, downloads))

    conn.commit()
    cursor.close()

# Function to insert data into the Book table (OpenLibrary)
def insert_openlibrary_data(conn, openlibrary_data):
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO Book (title, first_publish_year, cover_edition_key, has_fulltext)
    VALUES (%s, %s, %s, %s)
    """
    for book in openlibrary_data:
        title = book.get('title', '')
        first_publish_year = book.get('first_publish_year', 0)
        cover_edition_key = book.get('cover_edition_key', '')
        has_fulltext = book.get('has_fulltext', False)

        cursor.execute(insert_query, (title, first_publish_year, cover_edition_key, has_fulltext))

    conn.commit()
    cursor.close()

# Function to insert data into the Author table (OpenLibrary)
def insert_author_data(conn, openlibrary_data):
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO Author (author_name)
    VALUES (%s)
    """
    for book in openlibrary_data:
        authors = book.get('author_name', [])
        for author in authors:
            cursor.execute(insert_query, (author,))

    conn.commit()
    cursor.close()

# Function to insert relationships (Book-Author)
def insert_book_author_data(conn, openlibrary_data):
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO Book_Author (book_id, author_id)
    VALUES (%s, %s)
    """
    for book in openlibrary_data:
        book_id = book.get('key', '').split('/')[-1]  # Extract the book ID from the OpenLibrary key
        authors = book.get('author_key', [])
        for author_key in authors:
            cursor.execute("SELECT id FROM Author WHERE author_key = %s", (author_key,))
            author_id = cursor.fetchone()[0]
            cursor.execute(insert_query, (book_id, author_id))

    conn.commit()
    cursor.close()

# Main function to populate the database
def main():
    # Step 1: Connect to the database
    conn = connect_to_db()
    if not conn:
        return

    # Step 2: Fetch data from APIs
    print("Fetching data from Archive.org...")
    archive_data = get_archive_data()

    print("Fetching data from OpenLibrary...")
    openlibrary_data = get_openlibrary_data()

    # Step 3: Insert data into the database
    print("Inserting Archive data into the database...")
    insert_archive_data(conn, archive_data)

    print("Inserting OpenLibrary data into the database...")
    insert_openlibrary_data(conn, openlibrary_data)
    insert_author_data(conn, openlibrary_data)
    insert_book_author_data(conn, openlibrary_data)

    # Close the connection
    conn.close()
    print("Data populated successfully!")

if __name__ == "__main__":
    main()
