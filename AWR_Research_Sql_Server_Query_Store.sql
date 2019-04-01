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

