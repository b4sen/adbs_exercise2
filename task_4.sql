CREATE DATABASE IF NOT EXISTS adbs_hive;
USE adbs_hive;


CREATE TABLE IF NOT EXISTS badges (id BIGINT, class INT, dat TIMESTAMP, name VARCHAR(100), tagbased BOOLEAN, userid BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' TBLPROPERTIES("skip.header.line.count"="1");
CREATE TABLE IF NOT EXISTS comments (id BIGINT, creationdate TIMESTAMP, postid BIGINT, score INT, text VARCHAR(40000), userdisplayname VARCHAR(100), userid BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' TBLPROPERTIES("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS posts (id BIGINT,acceptedanswerid BIGINT,answercount INT,body VARCHAR(1000),closeddate TIMESTAMP,commentcount INT,communityowneddate TIMESTAMP,creationdate TIMESTAMP,favoritecount INT,lastactivitydate TIMESTAMP,lasteditdate TIMESTAMP,lasteditordisplayname VARCHAR(100),lasteditoruserid BIGINT,ownerdisplayname VARCHAR(100),owneruserid BIGINT,parentid BIGINT,posttypeid TINYINT,score INT,tags VARCHAR(200),title VARCHAR(200),viewcount INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' TBLPROPERTIES("skip.header.line.count"="1");
CREATE TABLE IF NOT EXISTS postlinks (id BIGINT,creationdate TIMESTAMP,linktypeid BIGINT,postid BIGINT,relatedpostid BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' TBLPROPERTIES("skip.header.line.count"="1");
CREATE TABLE IF NOT EXISTS users (id BIGINT,aboutme VARCHAR(3000),accountid BIGINT,creationdate TIMESTAMP,displayname VARCHAR(100),downvotes INT,lastaccessdate TIMESTAMP,location VARCHAR(100),profileimageurl VARCHAR(500),reputation INT,upvotes INT,views INT,websiteurl VARCHAR(500)) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' TBLPROPERTIES("skip.header.line.count"="1");
CREATE TABLE IF NOT EXISTS votes(id BIGINT,bountyamount INT ,creationdate TIMESTAMP,postid BIGINT,userid BIGINT,votetypeid BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/adbs/2019S/shared/hive/badges.csv' OVERWRITE INTO TABLE badges;
LOAD DATA LOCAL INPATH '/home/adbs/2019S/shared/hive/comments.csv' OVERWRITE INTO TABLE comments;
LOAD DATA LOCAL INPATH '/home/adbs/2019S/shared/hive//posts.csv' OVERWRITE INTO TABLE posts;
LOAD DATA LOCAL INPATH '/home/adbs/2019S/shared/hive/postlinks.csv' OVERWRITE INTO TABLE postlinks;
LOAD DATA LOCAL INPATH '/home/adbs/2019S/shared/hive/users.csv' OVERWRITE INTO TABLE users;
LOAD DATA LOCAL INPATH '/home/adbs/2019S/shared/hive/votes.csv' OVERWRITE INTO TABLE votes;


/* 

	DROP TABLE badges;
	DROP TABLE comments;
	DROP TABLE posts;
	DROP TABLE postlinks;
	DROP TABLE users;
	DROP TABLE votes;
	
*/

SET hive.enforce.bucketing = TRUE;
SET hive.exec.dynamic.partition = TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions = 10000;
SET hive.exec.max.dynamic.partitions.pernode = 1000;

EXPLAIN EXTENDED
SELECT p.id FROM posts p, comments c, users u, votes v
WHERE c.postid=p.id 
AND c.userid=p.owneruserid  -- join column
AND u.id=p.owneruserid -- join column
AND u.reputation > 100 
AND v.postid = p.id -- join column
AND v.userid = p.owneruserid -- join column
AND NOT EXISTS (SELECT 1 FROM postlinks l WHERE l.relatedpostid = p.id);

SELECT * FROM users;
select * from badges;
select * from posts;
select * from postlinks;
select * from comments;
select * from votes;

SELECT coalesce(substring(creationdate,0,4),'XXXX') AS YEAR, coalesce(substring(creationdate,6,2),'XX') AS MONTH, count(*) FROM posts group by coalesce(substring(creationdate,0,4),'XXXX'), coalesce(substring(creationdate,6,2),'XX');

SELECT coalesce(substring(creationdate,0,4),'XXXX') AS YEAR, coalesce(substring(creationdate,6,2),'XX') AS MONTH, coalesce(substring(creationdate,9,2),'XX') AS DAY, count(*) FROM posts GROUP BY coalesce(substring(creationdate,0,4),'XXXX'), coalesce(substring(creationdate,6,2),'XX'), coalesce(substring(creationdate,0,4),'XX');


SELECT votetypeid, count(*) FROM votes group by votetypeid;
select name from badges group by name;
select class from badges group by class;
select location from users group by location;

SELECT creationdate, count(*) FROM votes group by creationdate;


select posttypeid, count(*) from posts group by posttypeid;
SELECT votetypeid, count(*) FROM votes group by votetypeid;
SELECT linktypeid, count(*) from postlinks group by linktypeid;

SELECT count(distinct id) from users;
SELECT count(distinct owneruserid) from posts;

drop table posts_part;

/*
 * 
 * THIS PARTITIONING IS BAD, too many partitions with low amount of data.
 * 
 */

CREATE EXTERNAL TABLE IF NOT EXISTS posts_part 
									(id BIGINT,
									acceptedanswerid BIGINT,
									answercount INT,
									body VARCHAR(1000),
									closeddate TIMESTAMP,
									commentcount INT,
									communityowneddate TIMESTAMP,
									favoritecount INT,
									lastactivitydate TIMESTAMP,
									lasteditdate TIMESTAMP,
									lasteditordisplayname VARCHAR(100),
									lasteditoruserid BIGINT,
									ownerdisplayname VARCHAR(100),
									owneruserid BIGINT,
									parentid BIGINT,
									score INT,
									tags VARCHAR(200),
									title VARCHAR(200),
									viewcount INT, 
									creationdate TIMESTAMP) 
PARTITIONED BY (posttypeid TINYINT, year STRING, month STRING)
CLUSTERED BY (owneruserid) into 9 buckets
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' TBLPROPERTIES("skip.header.line.count"="1");



INSERT OVERWRITE TABLE posts_part 
PARTITION(posttypeid, YEAR, MONTH)
SELECT
	id,
	acceptedanswerid,
	answercount,
	body,
	closeddate,
	commentcount,
	communityowneddate,
	favoritecount, 
	lastactivitydate,
	lasteditdate,
	lasteditordisplayname,
	lasteditoruserid, 
	ownerdisplayname, 
	owneruserid, 
	parentid, 
	score,
	tags,
	title,
	viewcount,
	creationdate,
	posttypeid,
	coalesce(substring(creationdate,0,4),'XXXX') AS YEAR, 
	coalesce(substring(creationdate,6,2),'XX') AS MONTH
FROM posts; 




/*
 * 
 * ANOTHER TRY
 * PARTITIONING BY ONLY THE POSTTYPEID
 * CLUSTERING BY THE OWNERUSERID INTO 9 BUCKETS
 * WORKS BETTER THAN THE PREVIOUS ONE
 * and takes ca. 8 sec less to run
 * 
 */
CREATE EXTERNAL TABLE IF NOT EXISTS posts_part 
									(id BIGINT,
									acceptedanswerid BIGINT,
									answercount INT,
									body VARCHAR(1000),
									closeddate TIMESTAMP,
									commentcount INT,
									communityowneddate TIMESTAMP,
									favoritecount INT,
									lastactivitydate TIMESTAMP,
									lasteditdate TIMESTAMP,
									lasteditordisplayname VARCHAR(100),
									lasteditoruserid BIGINT,
									ownerdisplayname VARCHAR(100),
									owneruserid BIGINT,
									parentid BIGINT,
									score INT,
									tags VARCHAR(200),
									title VARCHAR(200),
									viewcount INT, 
									creationdate TIMESTAMP) 
PARTITIONED BY (posttypeid TINYINT)
CLUSTERED BY (owneruserid) into 9 buckets -- bucket count: 78561 unique ID-s -> 9 is a divider
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' TBLPROPERTIES("skip.header.line.count"="1");


INSERT OVERWRITE TABLE posts_part 
PARTITION(posttypeid)
SELECT
	id,
	acceptedanswerid,
	answercount,
	body,
	closeddate,
	commentcount,
	communityowneddate,
	favoritecount, 
	lastactivitydate,
	lasteditdate,
	lasteditordisplayname,
	lasteditoruserid, 
	ownerdisplayname, 
	owneruserid, 
	parentid, 
	score,
	tags,
	title,
	viewcount,
	creationdate,
	coalesce(posttypeid,'99')
FROM posts; 


-- with the partitioned table
SELECT p.id FROM posts_part p, comments c, users u, votes v
WHERE c.postid=p.id 
AND c.userid=p.owneruserid  -- join column
AND u.id=p.owneruserid -- join column
AND u.reputation > 100 
AND v.postid = p.id -- join column
AND v.userid = p.owneruserid -- join column
AND NOT EXISTS (SELECT 1 FROM postlinks l WHERE l.relatedpostid = p.id);


/*
 * 
 * CREATE A CLUSTERED USER TABLE WITH 9 BUCKETS
 * -> the owneruserid in the posts_part and the ID in the users_part are in the same buckets.
 * 
 * using the clustered users table slows down the query.
 * 
 */

CREATE EXTERNAL TABLE IF NOT EXISTS users_part (
							id BIGINT,
							aboutme VARCHAR(3000),
							accountid BIGINT,
							creationdate INT,
							displayname VARCHAR(100),
							downvotes INT,
							lastaccessdate TIMESTAMP,
							location VARCHAR(100),
							profileimageurl VARCHAR(500),
							reputation INT,
							upvotes INT,
							views INT,
							websiteurl VARCHAR(500)) 
CLUSTERED BY (id) into 9 buckets
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' TBLPROPERTIES("skip.header.line.count"="1");

DROP TABLE users_part;

INSERT OVERWRITE TABLE users_part
SELECT 
	id,
	aboutme,
	accountid,
	creationdate,
	displayname,
	downvotes,
	lastaccessdate,
	location,
	profileimageurl,
	reputation,
	upvotes,
	views,
	websiteurl
FROM users;




/*
 * 
 * 
 * TASK C
 * 
 * 
 */
/*
SELECT p.id
FROM posts p 
INNER JOIN comments c 
on (c.postid = p.id) 

LEFT JOIN users u 
on (u.id = p.owneruserid)

LEFT JOIN badges b 
ON (u.id = b.userid)
AND (b.name LIKE 'Autobiographer')

LEFT JOIN users u2 
ON (u2.creationdate = c.creationdate)
AND (u.upvotes+3 >= count(u2.upvotes))

ORDER BY p.id;
*/
--------------------
SELECT p.id
FROM posts p 
INNER JOIN comments c 
on (c.postid = p.id) 

LEFT JOIN users u 
on (u.id = p.owneruserid)

LEFT JOIN badges b 
ON (u.id = b.userid)

LEFT JOIN (select count(upvotes) as upvotes, creationdate from users group by creationdate) u2
ON (u2.creationdate = c.creationdate)
AND (u.upvotes+3 >= u2.upvotes)

WHERE (b.name LIKE 'Autobiographer')
AND EXISTS (SELECT 1 FROM postlinks l WHERE l.relatedpostid > p.id)
ORDER BY p.id
;
