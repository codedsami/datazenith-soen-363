// 1. Basic Search Query on an Attribute Value
// Search for books by a specific title
MATCH (b:Book)
WHERE b.title = "The Lord of the Rings"
RETURN b;

// 2. Aggregate Query
// Count how many books have full text
MATCH (b:Book)
WHERE b.has_fulltext = true
RETURN count(b) AS number_of_books_with_fulltext;

// 3. Top N Query Sorted by an Attribute
// Get the top 5 books based on the year they were first published
MATCH (b:Book)
RETURN b.title, b.first_publish_year
ORDER BY b.first_publish_year DESC
LIMIT 5;

// 4. Simulate a Relational GROUP BY
// Group books by their first publish year and count how many books were published each year
MATCH (b:Book)
RETURN b.first_publish_year AS year, count(b) AS number_of_books
ORDER BY year;

// 5. Index Creation Statement
// Create an index on the title property of Book nodes
CREATE INDEX book_title_index IF NOT EXISTS FOR (b:Book) ON (b.title);

// Optional: Use PROFILE to measure query time after index creation
// PROFILE MATCH (b:Book) WHERE b.title = "The Lord of the Rings" RETURN b;

// 6. Full Text Search Setup
// Create a full-text index on Book titles
CALL db.index.fulltext.createNodeIndex("book_fulltext_index", ["Book"], ["title"]);

// 6. Full Text Search Query
// Search for books with the word “machine” in the title using full-text index
CALL db.index.fulltext.queryNodes("book_fulltext_index", "machine") YIELD node, score
RETURN node.title AS title, score
ORDER BY score DESC;
