-------------------------------------------------------------
-- 1. JOIN QUERIES
-------------------------------------------------------------
-- (a) INNER JOIN using ANSI syntax:
SELECT b.book_id, b.title, be.edition_year
FROM Book b
INNER JOIN Book_Edition be
    ON b.book_id = be.book_id;

-- (b) Equivalent inner join using cartesian product and WHERE clause:
SELECT b.book_id, b.title, be.edition_year
FROM Book b, Book_Edition be
WHERE b.book_id = be.book_id;

-- (c) LEFT OUTER JOIN (shows all Books even if no matching edition; nulls appear):
SELECT b.book_id, b.title, be.edition_year
FROM Book b
LEFT JOIN Book_Edition be
    ON b.book_id = be.book_id;

-- (d) FULL OUTER JOIN (shows all records from both sides; nulls for non-matches):
-- Note: FULL OUTER JOIN is supported in PostgreSQL.
SELECT b.book_id, b.title, be.edition_year
FROM Book b
FULL OUTER JOIN Book_Edition be
    ON b.book_id = be.book_id;

-------------------------------------------------------------
-- 2. QUERIES DEMONSTRATING NULL VALUES (undefined/non-applicable)
-------------------------------------------------------------
-- (a) Find Books that do not have a defined publication year.
SELECT book_id, title
FROM Book
WHERE first_publish_year IS NULL;

-- (b) Find Archive documents that have no downloads recorded.
SELECT doc_id, title
FROM Archive_Document
WHERE downloads IS NULL;

-------------------------------------------------------------
-- 3. GROUP BY CLAUSES
-------------------------------------------------------------
-- (a) Simple GROUP BY without WHERE:
SELECT first_publish_year, COUNT(*) AS num_books
FROM Book
GROUP BY first_publish_year;

-- (b) GROUP BY with a WHERE clause:
SELECT first_publish_year, COUNT(*) AS num_books
FROM Book
WHERE first_publish_year > 2000
GROUP BY first_publish_year;

-- (c) GROUP BY with HAVING (show years with more than 1 book):
SELECT first_publish_year, COUNT(*) AS num_books
FROM Book
GROUP BY first_publish_year
HAVING COUNT(*) > 1;

-------------------------------------------------------------
-- 4. JOIN WITH NESTED SUB-QUERY
-------------------------------------------------------------
-- List book titles and author names for books that have at least one edition published after 2010.
SELECT b.title, a.author_name
FROM Book b
JOIN Book_Author ba ON b.book_id = ba.book_id
JOIN Author a ON a.author_id = ba.author_id
WHERE b.book_id IN (
    SELECT be.book_id
    FROM Book_Edition be
    WHERE be.edition_year > 2010
);

-------------------------------------------------------------
-- 5. CORRELATED QUERIES
-------------------------------------------------------------
-- (a) For each author, list the author’s name and the number of books written.
SELECT a.author_name,
       (SELECT COUNT(*)
        FROM Book_Author ba
        WHERE ba.author_id = a.author_id) AS books_written
FROM Author a;

-- (b) List books whose publication year equals the average publication year
--     of all books written by at least one of its authors.
SELECT b.title, b.first_publish_year
FROM Book b
WHERE b.first_publish_year = (
    SELECT AVG(b2.first_publish_year)
    FROM Book b2
    JOIN Book_Author ba2 ON b2.book_id = ba2.book_id
    WHERE ba2.author_id IN (
         SELECT ba3.author_id
         FROM Book_Author ba3
         WHERE ba3.book_id = b.book_id
    )
);

-------------------------------------------------------------
-- 6. SET OPERATIONS AND EQUIVALENTS
-------------------------------------------------------------
-- (a) UNION: List distinct titles from both Book and Archive_Document.
SELECT title FROM Book
UNION
SELECT title FROM Archive_Document;

-- (b) INTERSECT: Titles that appear in both Book and Archive_Document.
SELECT title FROM Book
INTERSECT
SELECT title FROM Archive_Document;

-- (c) DIFFERENCE (EXCEPT): Titles in Book that do not appear in Archive_Document.
SELECT title FROM Book
EXCEPT
SELECT title FROM Archive_Document;

-- Equivalents without using set operators:
-- INTERSECT equivalent using EXISTS:
SELECT b.title
FROM Book b
WHERE EXISTS (
    SELECT 1
    FROM Archive_Document ad
    WHERE ad.title = b.title
);

-- DIFFERENCE equivalent using NOT EXISTS:
SELECT b.title
FROM Book b
WHERE NOT EXISTS (
    SELECT 1
    FROM Archive_Document ad
    WHERE ad.title = b.title
);

-------------------------------------------------------------
-- 7. VIEW WITH HARD-CODED CRITERIA
-------------------------------------------------------------
-- This view shows books published after a hard-coded year.
CREATE OR REPLACE VIEW Recent_Books AS
SELECT book_id, title, first_publish_year
FROM Book
WHERE first_publish_year >= 2010;
-- (Change the hard-coded value to modify the view’s content.)

-------------------------------------------------------------
-- 8. QUERIES DEMONSTRATING OVERLAP AND COVERING CONSTRAINTS
-------------------------------------------------------------
-- (a) Overlap Constraint Example:
-- Find books that have at least one edition published in a given year (e.g., 2015).
SELECT b.title, be.edition_year
FROM Book b
JOIN Book_Edition be ON b.book_id = be.book_id
WHERE be.edition_year = 2015;

-- (b) Covering Constraint Example:
-- Find authors who have written books in every publication year that exists in the Book table.
SELECT a.author_name
FROM Author a
WHERE NOT EXISTS (
    SELECT b.first_publish_year
    FROM Book b
    EXCEPT
    SELECT b2.first_publish_year
    FROM Book b2
    JOIN Book_Author ba ON b2.book_id = ba.book_id
    WHERE ba.author_id = a.author_id
);

-------------------------------------------------------------
-- 9. DIVISION OPERATOR IMPLEMENTATIONS
-------------------------------------------------------------
-- Objective: Find authors who have written every book in the Book table.
-- (a) Using NOT IN:
SELECT a.author_name
FROM Author a
WHERE NOT EXISTS (
    SELECT b.book_id
    FROM Book b
    WHERE b.book_id NOT IN (
        SELECT ba.book_id
        FROM Book_Author ba
        WHERE ba.author_id = a.author_id
    )
);

-- (b) Using NOT EXISTS and EXCEPT (PostgreSQL syntax):
SELECT a.author_name
FROM Author a
WHERE NOT EXISTS (
    (SELECT b.book_id FROM Book b)
    EXCEPT
    (SELECT ba.book_id FROM Book_Author ba WHERE ba.author_id = a.author_id)
);
