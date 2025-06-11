CREATE TABLE query1 AS
SELECT g.name AS name, COUNT(h.movieid) AS moviecount
FROM genres g
JOIN hasagenre h ON g.genreid = h.genreid
GROUP BY g.name;

CREATE TABLE query2 AS
SELECT g.name AS name, AVG(r.rating) AS rating
FROM genres g
JOIN hasagenre h ON g.genreid = h.genreid
JOIN ratings r ON h.movieid = r.movieid
GROUP BY g.name;

CREATE TABLE query3 AS
SELECT m.title AS title, COUNT(r.rating) AS countofratings
FROM movies m
JOIN ratings r ON m.movieid = r.movieid
GROUP BY m.title
HAVING COUNT(r.rating) >= 10;

CREATE TABLE query4 AS
SELECT m.movieid AS movieid, m.title AS title
FROM movies m
JOIN hasagenre h ON m.movieid = h.movieid
JOIN genres g ON h.genreid = g.genreid
WHERE g.name = 'Comedy';

CREATE TABLE query5 AS
SELECT m.title AS title, AVG(r.rating) AS average
FROM movies m
JOIN ratings r ON m.movieid = r.movieid
GROUP BY m.title;

CREATE TABLE query6 AS
SELECT AVG(r.rating) AS average
FROM movies m
JOIN hasagenre h ON m.movieid = h.movieid
JOIN genres g ON h.genreid = g.genreid
JOIN ratings r ON m.movieid = r.movieid
WHERE g.name = 'Comedy';

CREATE TABLE query7 AS
SELECT AVG(r.rating) AS average
FROM movies m
JOIN hasagenre h1 ON m.movieid = h1.movieid
JOIN genres g1 ON h1.genreid = g1.genreid
JOIN hasagenre h2 ON m.movieid = h2.movieid
JOIN genres g2 ON h2.genreid = g2.genreid
JOIN ratings r ON m.movieid = r.movieid
WHERE g1.name = 'Comedy' AND g2.name = 'Romance';

CREATE TABLE query8 AS
SELECT AVG(r.rating) AS average
FROM movies m
JOIN hasagenre hr ON m.movieid = hr.movieid
JOIN genres gr ON hr.genreid = gr.genreid
JOIN ratings r ON m.movieid = r.movieid
WHERE gr.name = 'Romance' 
  AND m.movieid NOT IN (
      SELECT h.movieid
      FROM hasagenre h
      JOIN genres g ON h.genreid = g.genreid
      WHERE g.name = 'Comedy'
  );

CREATE TABLE query9 AS
SELECT r.movieid AS movieid, r.rating AS rating
FROM ratings r
WHERE r.userid = :v1;
