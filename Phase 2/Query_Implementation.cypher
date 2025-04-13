// 1. Basic Search Query on an Attribute Value
// Find all books published in 1999
MATCH (b:Book)
WHERE b.first_publish_year = 1999
RETURN b.title, b.first_publish_year;

// 2. Aggregate Data Query
// Count how many books have full text available
MATCH (b:Book)
WHERE b.has_fulltext = true
RETURN COUNT(b) AS num_books_with_fulltext;

// 3. Top N Entities by Sorting Attribute
// Get top 5 books published after 1950, sorted by downloads of their linked archive documents
MATCH (b:Book)-[:LINKED_TO]->(d:ArchiveDocument)
WHERE b.first_publish_year > 1950
WITH b, SUM(d.downloads) AS total_downloads
RETURN b.title AS title, b.first_publish_year AS year, total_downloads
ORDER BY total_downloads DESC
LIMIT 5;

// 4. Simulate Relational GROUP BY
// Count number of books grouped by the language of their editions
MATCH (b:Book)-[:HAS_EDITION]->(e:BookEdition)
RETURN e.language AS language, COUNT(DISTINCT b) AS num_books
ORDER BY num_books DESC;

// 5. Create Indexes & Show Query Performance Before/After
// a. Run query without index 
PROFILE
MATCH (b:Book)
WHERE b.first_publish_year = 1999
RETURN b.title;

//Performance : 27604 total db hits in 194 ms (without index)


// b. Create index on first_publish_year
CREATE INDEX book_year_index IF NOT EXISTS FOR (b:Book) ON (b.first_publish_year);

// c. Run the same query again after index creation
PROFILE
MATCH (b:Book)
WHERE b.first_publish_year = 1999
RETURN b.title;

// Total DB hits: 541 (compared to 27,604 without the index)
// Execution time: 112 ms (compared to 194 ms without the index)


// 6. Full-Text Search with Index Performance Boost

// === STEP 1: Full-Text Search BEFORE Index Creation (Expect Failure or Slow Performance) ===
PROFILE
MATCH (d:ArchiveDocument)
WHERE d.title CONTAINS "history" OR d.subject CONTAINS "history"
RETURN d.title AS title, d.subject AS subject
ORDER BY d.title
LIMIT 10;

// Performance : 50001 total db hits in 643 ms.


// === STEP 2: Create Full-Text Index on title and subject ===
CREATE FULLTEXT INDEX archive_fulltext_fts FOR (d:ArchiveDocument) ON EACH [d.title, d.subject];

// === STEP 3: Full-Text Search AFTER Index Creation ===
PROFILE
CALL db.index.fulltext.queryNodes("archive_fulltext_fts", "history") YIELD node, score
RETURN node.title AS title, node.subject AS subject, score
ORDER BY score DESC
LIMIT 10;

// Performance : 27 total db hits in 231 ms

// Key Observations:
// DB Hits: The number of database hits decreased dramatically from 50,001 to 27 after creating the full-text index, showing much more efficient query execution.
// Execution Time: The total execution time also dropped from 643 ms to 231 ms, confirming that the index optimized the search operation.
// This demonstrates how creating a full-text index improves query performance, especially when performing text searches on large datasets. Would you like me to help you interpret or present this data further?
