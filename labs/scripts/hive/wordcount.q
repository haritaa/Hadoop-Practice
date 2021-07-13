CREATE TABLE IF NOT EXISTS docs (line STRING);

LOAD DATA INPATH '/user/training/wordcount/input/wordcount-input.txt' OVERWRITE INTO TABLE docs;

CREATE TABLE IF NOT EXISTS word_counts AS
SELECT word, count(1) AS count FROM
 (SELECT explode(split(line, '\\s')) AS word FROM docs) temp
GROUP BY word
ORDER BY word;

SELECT * FROM word_counts;
