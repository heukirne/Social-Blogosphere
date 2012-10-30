--Compute Spearman Correlation for Views and Comments from a defined Year

TRUNCATE AuthorViews;
TRUNCATE AuthorComments;

INSERT IGNORE INTO AuthorViews (id)
select profileID from AuthorBlogs 
WHERE ano = 2008
AND views > 0
AND comments > 0
AND numAuthors = 1
ORDER BY views DESC;

INSERT IGNORE INTO AuthorComments (id)
select profileID from AuthorBlogs 
WHERE ano = 2008
AND views > 0
AND comments > 0
AND numAuthors = 1
ORDER BY comments DESC;

UPDATE AuthorViews a SET comments = (SELECT comments FROM AuthorComments where id=a.id);

UPDATE AuthorViews SET rel = POW(views-comments,2);

SELECT 1 - ((6 * SUM(rel))/(count(*)*(count(*)*count(*)-1))) FROM AuthorViews;