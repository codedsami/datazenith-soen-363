import requests
import psycopg2
import json

# Database connection parameters
DB_HOST = 'localhost'  # Change to your database host
DB_NAME = 'Project_phase_1' 
DB_USER = 'miskatsami' 
DB_PASSWORD = 'sami2001' 

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
    
    for data in archive_data:
        identifier = data.get('identifier')
        title = data.get('title')
        creator = data.get('creator')
        year = data.get('year')
        language = data.get('language')
        downloads = data.get('downloads')
        subjects = data.get('subject', "")

        # Ensure 'subject' is a string, if it's a list, join it into a single string
        if isinstance(subjects, list):  
            subjects = ', '.join(subjects)
        
        # Prepare the SQL query
        insert_query = """
            INSERT INTO Archive_Document (identifier, title, creator, year, language, subject, downloads)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (identifier) DO NOTHING
        """
        
        # Execute the query
        cursor.execute(insert_query, (identifier, title, creator, year, language, subjects, downloads))
    
    conn.commit()
    cursor.close()






# Function to insert data into the Book table (OpenLibrary)
def insert_openlibrary_data(conn, openlibrary_data):
    cursor = conn.cursor()
    
    for book in openlibrary_data:
        title = book.get('title')
        first_publish_year = book.get('first_publish_year')
        cover_edition_key = book.get('cover_edition_key')
        has_fulltext = book.get('has_fulltext', False)
        
        # Skip insertion if cover_edition_key is empty or None
        if not cover_edition_key:
            continue
        
        insert_query = """
        INSERT INTO Book (title, first_publish_year, cover_edition_key, has_fulltext)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (cover_edition_key) DO NOTHING
        """  # Ignore duplicates
        
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
        cover_edition_key = book.get('cover_edition_key')
        
        # Check if the book already exists using cover_edition_key
        cursor.execute("SELECT book_id FROM Book WHERE cover_edition_key = %s", (cover_edition_key,))
        existing_book = cursor.fetchone()

        if existing_book is None:
            # Insert into Book table if the book doesn't exist
            cursor.execute("""
            INSERT INTO Book (title, first_publish_year, cover_edition_key, has_fulltext)
            VALUES (%s, %s, %s, %s)
            RETURNING book_id
            """, (book.get('title'), book.get('first_publish_year'), cover_edition_key, book.get('has_fulltext', False)))
            book_id = cursor.fetchone()[0]  # Get the generated book_id
        else:
            # If the book already exists, use the existing book_id
            book_id = existing_book[0]
        
        # Now insert into Book_Author (linking book_id with author_id)
        authors = book.get('author_key', [])
        for author_key in authors:
            cursor.execute("SELECT author_id FROM Author WHERE author_name = %s", (author_key,))
            author_id_result = cursor.fetchone()

            if author_id_result is None:
                # Insert new author and get the author_id
                cursor.execute("INSERT INTO Author (author_name) VALUES (%s) RETURNING author_id", (author_key,))
                author_id = cursor.fetchone()[0]
            else:
                # Use existing author_id
                author_id = author_id_result[0]
            
            # Check if the combination of book_id and author_id already exists in Book_Author
            cursor.execute("SELECT 1 FROM Book_Author WHERE book_id = %s AND author_id = %s", (book_id, author_id))
            if cursor.fetchone() is None:  # If combination does not exist, insert it
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
