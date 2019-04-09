CREATE PROCEDURE [dbo].sp_query_store_awr
AS
BEGIN
    IF NOT EXISTS (SELECT name FROM sys.all_objects WHERE UPPER(name) = 'QUERY_STORE_AWR')
        BEGIN
            SELECT Txt.query_sql_text, pl.plan_id, qry.* INTO [eightsquare].[dbo].[query_store_awr]  
            FROM sys.query_store_plan pl  
            INNER JOIN sys.query_store_query qry  
            ON pl.query_id = qry.query_id  
            INNER JOIN sys.query_store_query_text txt  
            ON qry.query_text_id = txt.query_text_id; 
        END;
    ELSE
        BEGIN
            INSERT INTO [eightsquare].[dbo].[query_store_awr]
            SELECT a.* 
    		FROM (
                  SELECT txt.query_sql_text, pl.plan_id, qry.*  
                  FROM sys.query_store_plan pl  
                  INNER JOIN sys.query_store_query qry  
                  ON pl.query_id = qry.query_id  
                  INNER JOIN sys.query_store_query_text txt  
                  ON qry.query_text_id = txt.query_text_id 
    			 ) a
            WHERE NOT EXISTS (
    		                  SELECT 1 
    		                  FROM [eightsquare].[dbo].[query_store_awr] b
                              WHERE a.query_hash = b.query_hash
            				  AND a.plan_id=b.plan_id
    						  ) ;
        END;   
END;
GO

EXEC [eightsquare].[dbo].sp_query_store_awr;

SELECT q.query_id, qt.query_text_id, qt.query_sql_text,   
    SUM(rs.count_executions) AS total_execution_count  
FROM sys.query_store_query_text AS qt   
JOIN sys.query_store_query AS q   
    ON qt.query_text_id = q.query_text_id   
JOIN sys.query_store_plan AS p   
    ON q.query_id = p.query_id   
JOIN sys.query_store_runtime_stats AS rs   
    ON p.plan_id = rs.plan_id  
GROUP BY q.query_id, qt.query_text_id, qt.query_sql_text  
ORDER BY total_execution_count DESC; 

ALTER DATABASE <<database_name>>   
SET QUERY_STORE (  
    OPERATION_MODE = READ_WRITE,  
    CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = 30),  
    DATA_FLUSH_INTERVAL_SECONDS = 3000,  
    MAX_STORAGE_SIZE_MB = 500,  
    INTERVAL_LENGTH_MINUTES = 15,  
    SIZE_BASED_CLEANUP_MODE = AUTO,  
    QUERY_CAPTURE_MODE = AUTO,  
    MAX_PLANS_PER_QUERY = 1000,
    WAIT_STATS_CAPTURE_MODE = ON 
);  

SELECT
	  s.Name AS SchemaName,
	  t.Name AS TableName,
	  p.rows AS RowCounts,
	  CAST(ROUND((SUM(a.used_pages) / 128.00), 2) AS NUMERIC(36, 2)) AS Used_MB,
	  CAST(ROUND((SUM(a.used_pages) / 128.00), 2) AS NUMERIC(36, 2)) / 1024 AS Used_GB,
	  CAST(ROUND((SUM(a.total_pages) - SUM(a.used_pages)) / 128.00, 2) AS NUMERIC(36, 2)) AS Unused_MB,
	  CAST(ROUND((SUM(a.total_pages) / 128.00), 2) AS NUMERIC(36, 2)) AS Total_MB into table_size_02_04_2019
FROM sys.tables t
INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE t.name ='query_store_awr'
GROUP BY t.Name, s.Name, p.Rows
ORDER BY s.Name, t.Name;

use [TestDB]

select count(*) dml_query_count,
       cast(cast(initial_compile_start_time as date) as datetime) quer_execution_date 
from [TestDB].[dbo].[query_store_awr]
group by cast(cast(initial_compile_start_time as date) as datetime)
order by quer_execution_date;
/*
dml_query_count	quer_execution_date
--------------- -----------------------
2180	        2019-04-02 00:00:00.000
4386	        2019-04-03 00:00:00.000
4043	        2019-04-04 00:00:00.000
1288	        2019-04-05 00:00:00.000
4	            2019-04-06 00:00:00.000
195	            2019-04-07 00:00:00.000
655	            2019-04-08 00:00:00.000
3	            2019-04-09 00:00:00.000
*/

select 'database_size_02_04_2019' size,* from [TestDB].[dbo].[database_size_02_04_2019]
union all
select 'database_size_09_04_2019' size,* from [TestDB].[dbo].[database_size_09_04_2019];
/*
size	                 database_id  Logical_Name Physical_Name	                    state_desc recovery_model_desc total_size data_size	data_used_size log_size	log_used_size full_last_date full_size log_last_date
------------------------ ------------ ------------ ------------------------------------ ---------- ------------------- ---------- --------- -------------- -------- ------------- -------------- --------- -------------
database_size_02_04_2019 10	          TestDB	   C:\Program Files\DATA\TestDB.mdf	    ONLINE	   FULL	               8.00	      8.00	    5.94	       NULL	    3.58	      NULL	         NULL	   NULL
database_size_02_04_2019 10	          TestDB_log   C:\Program Files\DATA\TestDB_log.ldf	ONLINE	   FULL	               72.00	  NULL	    5.94	       72.00	3.58	      NULL	         NULL	   NULL
database_size_09_04_2019 10	          TestDB	   C:\Program Files\DATA\TestDB.mdf	    ONLINE	   FULL	               72.00	  72.00	    35.25	       NULL	    5.13	      NULL	         NULL	   NULL
database_size_09_04_2019 10	          TestDB_log   C:\Program Files\DATA\TestDB_log.ldf	ONLINE	   FULL	               136.00	  NULL	    35.25	       136.00	5.13	      NULL	         NULL	   NULL
*/

select 'table_size_02_04_2019' size,* from [TestDB].[dbo].[table_size_02_04_2019]
union all
select 'table_size_09_04_2019' size,* from [TestDB].[dbo].[table_size_09_04_2019];
/*
size	              SchemaName TableName	     RowCounts Used_MB Used_GB	Unused_MB Total_MB
--------------------- ---------- --------------- --------- ------- -------- --------- --------
table_size_02_04_2019 dbo	     query_store_awr 1387	   2.05	   0.002001	0.16	  2.20
table_size_09_04_2019 dbo	     query_store_awr 12754	   31.12   0.030390	0.09	  31.20
*/
