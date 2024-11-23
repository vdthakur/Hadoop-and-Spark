# Homework Assignment: Film Dataset Analysis

This homework assignment involves analyzing a film dataset consisting of the following files:
- `film.csv`
- `actor.csv`
- `film_actor.csv`

The tasks include implementing a Hadoop MapReduce program, Spark DataFrame scripts, and Spark RDD scripts to answer various SQL queries.

---

## Task 1: Hadoop MapReduce Program (40 points)

### Goal:
Write a Hadoop MapReduce program (`SQL2MR.java`) to find the result for the following SQL query on the `film.csv` data:

```sql
SELECT rating, AVG(replacement_cost)
FROM film
WHERE length >= 60
GROUP BY rating
HAVING COUNT(*) >= 160;

Instructions:

	1.	Prepare the Data:
	•	Remove the header from film.csv.
	•	Save the file under an HDFS directory called input.
	2.	Compile and Run the Program:
	•	Compile the program:

hadoop com.sun.tools.javac.Main SQL2MR.java


	•	Create a JAR file:

jar cf sql2mr.jar SQL2MR*.class


	•	Run the program:

hadoop jar sql2mr.jar SQL2MR input output



Submission:

	•	SQL2MR.java
	•	sql2mr.jar
	•	Output file: part-r-00000

Task 2: Spark DataFrame Queries (30 points)

Goal:

Write Spark DataFrame scripts for the following SQL queries. Assume the following initialization:

import pyspark.sql.functions as fc

film = spark.read.csv('film.csv', header=True, inferSchema=True)
actor = spark.read.csv('actor.csv', header=True, inferSchema=True)
film_actor = spark.read.csv('film_actor.csv', header=True, inferSchema=True)

Queries:

a. Retrieve Titles and Descriptions:

SELECT title, description
FROM film
WHERE rating = 'PG'
LIMIT 5;

b. Average Replacement Cost:

SELECT rating, AVG(replacement_cost)
FROM film
WHERE length >= 60
GROUP BY rating
HAVING COUNT(*) >= 160;

c. Common Actors in Films:

SELECT actor_id
FROM film_actor
WHERE film_id = 1
INTERSECT
SELECT actor_id
FROM film_actor
WHERE film_id = 23;

d. Distinct Actors in Films 1, 2, 3:

SELECT DISTINCT first_name, last_name
FROM actor
JOIN film_actor ON actor.actor_id = film_actor.actor_id
WHERE film_id IN (1, 2, 3)
ORDER BY first_name
LIMIT 5;

e. Rental Duration Statistics:

SELECT rental_duration, rating, MIN(length), MAX(length), AVG(length), COUNT(length)
FROM film
GROUP BY rental_duration, rating
ORDER BY rental_duration DESC, rating
LIMIT 10;
```

Submission:

	•	Python scripts for all queries.
	•	Output of the scripts.

Task 3: Spark RDD Queries (30 points)

Goal:

Write Spark RDD scripts for the same queries as in Task 2.

Queries:

a. Retrieve Titles and Descriptions:
Retrieve the titles and descriptions where the rating is “PG” (limit to 5 records).

b. Average Replacement Cost:
Group by rating, filter films with length ≥ 60, and compute averages. Include only groups with at least 160 films.

c. Common Actors in Films:
Find actors who appeared in both film 1 and film 23.

d. Distinct Actors in Films 1, 2, 3:
List distinct actor names for films 1, 2, and 3, ordered by first name (limit to 5 records).

e. Rental Duration Statistics:
Group by rental duration and rating, compute statistics (min, max, avg, count), and sort by rental duration (descending) and rating.

Submission:

	•	Python scripts for all queries.
	•	Output of the scripts.

This markdown format is suitable for a project README or homework assignment instructions.
