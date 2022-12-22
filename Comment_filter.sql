CREATE TABLE IF NOT EXISTS comment_filter(
	id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    list1 VARCHAR(30),
    list2 VARCHAR(30))


 COPY comment_filter(list1, list2) FROM '/var/список.csv' DELIMITER ';' CSV HEADER;