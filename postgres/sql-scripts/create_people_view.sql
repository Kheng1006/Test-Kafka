CREATE VIEW people_view AS
SELECT * FROM read_people('hdfs://demo-hadoop-namenode:9000/delta-lake/people');

