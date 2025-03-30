-- Drop tables if they exist (to avoid conflicts when re-running)
DROP TABLE IF EXISTS Book_Author, Book_Archive_Link, Archive_Document, Book, Author, Book_Edition, Archive_Stats CASCADE;

-- Table: Author
CREATE TABLE Author (
    author_id SERIAL PRIMARY KEY,
    author_name TEXT NOT NULL
);

-- Table: Book (General Entity)
CREATE TABLE Book (
    book_id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    first_publish_year INT CHECK (first_publish_year > 0),
    cover_edition_key TEXT UNIQUE,
    has_fulltext BOOLEAN NOT NULL DEFAULT FALSE
);

-- Requirement: IS-A Relationship: Book_Edition is a subtype of Book (Inheritance)
CREATE TABLE Book_Edition (
    edition_id SERIAL PRIMARY KEY,
    book_id INT REFERENCES Book(book_id) ON DELETE CASCADE,
    edition_number INT CHECK (edition_number > 0),  -- Requirement: Domains & Types
    edition_year INT CHECK (edition_year > 0),
    language TEXT
);

-- Table: Book_Author (Many-to-Many Relationship)
CREATE TABLE Book_Author (
    book_id INT REFERENCES Book(book_id) ON DELETE CASCADE,
    author_id INT REFERENCES Author(author_id) ON DELETE CASCADE,
    PRIMARY KEY (book_id, author_id)
);

-- Table: Archive_Document (External data from Archive.org)
CREATE TABLE Archive_Document (
    doc_id SERIAL PRIMARY KEY,
    identifier TEXT UNIQUE NOT NULL,  -- External Key
    title TEXT NOT NULL,
    creator TEXT,
    year INT CHECK (year > 0),
    language TEXT,
    downloads INT CHECK (downloads >= 0),
    subject TEXT
);

-- Requirement: Weak Entity => Archive_Stats (Stats dependent on Archive_Document)
CREATE TABLE Archive_Stats (
    stat_id SERIAL PRIMARY KEY,
    doc_id INT REFERENCES Archive_Document(doc_id) ON DELETE CASCADE,
    last_access_date DATE NOT NULL,
    daily_downloads INT CHECK (daily_downloads >= 0)
);

-- Linking Books to Archives (Complex Key Matching)
-- Requirement: This table is used to link the two data sourse
CREATE TABLE Book_Archive_Link (
    link_id SERIAL PRIMARY KEY,
    book_id INT REFERENCES Book(book_id) ON DELETE CASCADE,
    doc_id INT REFERENCES Archive_Document(doc_id) ON DELETE CASCADE
);

-- Requirement: Aggregation => Summarizing Archive Downloads per Year
CREATE VIEW Archive_Download_Summary AS
SELECT year, COUNT(*) AS num_docs, SUM(downloads) AS total_downloads
FROM Archive_Document
GROUP BY year
ORDER BY year;

-- Requirement: Hard-coded View => Restricting Access Based on Role
CREATE VIEW Restricted_Books AS
SELECT book_id, title, first_publish_year
FROM Book
WHERE first_publish_year >= 2000; -- Limits to books published after 2000

-- Requirement: Referential Integrity => Prevent deleting a book if linked to an archive
CREATE OR REPLACE FUNCTION prevent_book_deletion()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM Book_Archive_Link WHERE book_id = OLD.book_id) THEN
        RAISE EXCEPTION 'Cannot delete book: Linked archive exists';
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prevent_book_delete_trigger
BEFORE DELETE ON Book
FOR EACH ROW
EXECUTE FUNCTION prevent_book_deletion();
