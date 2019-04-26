-- To Find Database Size without Last Backup
SELECT
     DB_NAME(database_id) dbname,
     Name                 Logical_Name,
     Physical_Name,
     (size*8)/1024        SizeMB, 
     (size*8)/(1024*1024) SizeGB
FROM sys.master_files
WHERE DB_NAME(database_id) = (SELECT name FROM sys.sysdatabases u WHERE u.dbid = db_id());

-- To Find Database Size with Last Backup
SELECT
      d.database_id
    , t.name Logical_Name
    , t.Physical_Name
    , d.state_desc
    , d.recovery_model_desc
    , t.total_size/1024      total_size_gb
    , t.data_size/1024       data_size_gb
    , s.data_used_size/1024  data_used_size_gb
    , t.log_size/1024        log_size_gb
    , bu.full_last_date      last_date_of_backup
    , bu.full_size/1024      last_backup_full_size_gb
FROM (
    SELECT
          database_id
        , name
        , Physical_Name
        , log_size = CAST(SUM(CASE WHEN [type] = 1 THEN size END) * 8. / 1024 AS DECIMAL(18,2))
        , data_size = CAST(SUM(CASE WHEN [type] = 0 THEN size END) * 8. / 1024 AS DECIMAL(18,2))
        , total_size = CAST(SUM(size) * 8. / 1024 AS DECIMAL(18,2))
    FROM sys.master_files
    WHERE DB_NAME(database_id) = (SELECT name FROM sys.sysdatabases u WHERE u.dbid = db_id())
    GROUP BY database_id
        , name
        , Physical_Name
) t
JOIN sys.databases d ON d.database_id = t.database_id
LEFT JOIN (SELECT
          DB_ID() database_id
        , SUM(CASE WHEN [type] = 0 THEN space_used END) data_used_size
        , SUM(CASE WHEN [type] = 1 THEN space_used END) log_used_size
    FROM (
        SELECT s.[type], space_used = SUM(FILEPROPERTY(s.name, 'SpaceUsed') * 8. / 1024)
        FROM sys.database_files s
        GROUP BY s.[type]
    ) t) s ON d.database_id = s.database_id
LEFT JOIN (
    SELECT
          database_name
        , full_last_date = MAX(CASE WHEN [type] = 'D' THEN backup_finish_date END)
        , full_size = MAX(CASE WHEN [type] = 'D' THEN backup_size END)
        , log_last_date = MAX(CASE WHEN [type] = 'L' THEN backup_finish_date END)
        , log_size = MAX(CASE WHEN [type] = 'L' THEN backup_size END)
    FROM (
        SELECT
              s.database_name
            , s.[type]
            , s.backup_finish_date
            , backup_size =
                        CAST(CASE WHEN s.backup_size = s.compressed_backup_size
                                    THEN s.backup_size
                                    ELSE s.compressed_backup_size
                        END / 1048576.0 AS DECIMAL(18,2))
            , RowNum = ROW_NUMBER() OVER (PARTITION BY s.database_name, s.[type] ORDER BY s.backup_finish_date DESC)
        FROM msdb.dbo.backupset s
        WHERE s.[type] IN ('D', 'L')
    ) f
    WHERE f.RowNum = 1
    GROUP BY f.database_name
) bu ON d.name = bu.database_name
ORDER BY t.total_size DESC;

-- To Find the Table Size with Rows Count
SELECT
     u.name
    ,s.name AS schemaname
    ,t.name AS tablename
    ,p.rows AS rowcounts
    ,CAST(ROUND((SUM(a.used_pages) / 128.00), 2) AS NUMERIC(36, 2)) AS used_mb
    ,CAST(ROUND((SUM(a.used_pages) / 128.00), 2) AS NUMERIC(36, 2)) / 1024 AS used_gb
    ,CAST(ROUND((SUM(a.total_pages) - SUM(a.used_pages)) / 128.00, 2) AS NUMERIC(36, 2)) AS unused_mb
    ,CAST(ROUND((SUM(a.total_pages) / 128.00), 2) AS NUMERIC(36, 2)) AS total_mb
FROM sys.tables t INNER JOIN sys.indexes i 
ON (t.object_id = i.object_id)
INNER JOIN sys.partitions p 
ON (i.object_id = p.object_id AND i.index_id = p.index_id)
INNER JOIN sys.allocation_units a 
ON (p.partition_id = a.container_id)
INNER JOIN sys.schemas s 
ON (t.schema_id = s.schema_id)
CROSS JOIN sys.sysdatabases u
WHERE u.dbid = db_id()
GROUP BY
     u.name 
    ,t.name
    ,s.name
    ,p.rows
ORDER BY 
     s.name
    ,t.name;

--To find the Object ID
SELECT OBJECT_ID('sp_updatestats');

--To Find the Object Structure
SELECT *
FROM sys.all_sql_modules
WHERE OBJECT_ID = '-838816646';