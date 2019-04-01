use [EightSquare]

DECLARE @name      VARCHAR(50);
DECLARE @database  VARCHAR(50);
SET @database = 'EightSquare';
DECLARE db_names   CURSOR READ_ONLY FOR  
                   SELECT name FROM [sys].[all_objects]
                   WHERE schema_id =1 
                   AND type IN ('U');

				   
IF OBJECT_ID('tempdb.dbo.#privlileges') IS NOT NULL 
	DROP TABLE #privlileges;

CREATE TABLE #privlileges
(
  table_qualifier	 VARCHAR(255),
  table_owner	     VARCHAR(255),
  table_name	     VARCHAR(255),
  grantor	         VARCHAR(255),
  grantee	         VARCHAR(255),
  privilege	         VARCHAR(255),
  is_grantable       VARCHAR(255)
);

IF OBJECT_ID('tempdb.dbo.#integrity_constraints_index') IS NOT NULL 
	DROP TABLE #integrity_constraints_index;

CREATE TABLE #integrity_constraints_index
(
  index_name        VARCHAR(255),
  column_name       VARCHAR(255),
  is_descending_key VARCHAR(255),
  is_unique         VARCHAR(255),
  type_desc         VARCHAR(255)
);

IF OBJECT_ID('tempdb.dbo.#table_list') IS NOT NULL 
	DROP TABLE #table_list;

CREATE TABLE #table_list
(
  table_name      VARCHAR(255), 
  column_name     VARCHAR(255),
  data_type       VARCHAR(255),
  is_nullable     VARCHAR(255),
  column_default  VARCHAR(255),
  columan_lenght  VARCHAR(255),
  description     VARCHAR(255) DEFAULT NULL
);

OPEN db_names   
FETCH NEXT FROM db_names INTO @name   
 
WHILE @@FETCH_STATUS = 0   
BEGIN
     -- To Find the Object Privilage   
     INSERT INTO #privlileges EXEC sp_table_privileges @table_name =@name;

	 -- To Find the Index Status
	 INSERT INTO #integrity_constraints_index  
     SELECT
         a.name                                                       index_name,
         COL_NAME(b.object_id,b.column_id)                            column_name,
     	 CASE WHEN b.is_descending_key =0 THEN 'ASC' ELSE 'DESC' END  is_descending_key,
     	 CASE WHEN a.is_unique=1 THEN 'YES' ELSE 'NO' END             is_unique,
         a.type_desc                                                  type_desc
     FROM
         sys.indexes a  
     INNER JOIN
         sys.index_columns b   
     ON a.object_id = b.object_id AND a.index_id = b.index_id  
     WHERE
         a.is_hypothetical = 0 AND
         a.object_id = OBJECT_ID(@name);

    -- To find the table structure
    INSERT INTO #table_list 
	SELECT
	     a.table_name,
	     a.column_name,
	     a.data_type,
	     a.is_nullable,
	     a.column_default,
	     CASE 
	     	WHEN CAST(a.columan_lenght AS VARCHAR(10)) = -1 THEN 'MAX'
			WHEN CAST(a.columan_lenght AS VARCHAR(10)) IS NULL THEN ''
	     ELSE CAST(a.columan_lenght AS VARCHAR(10))
	     END columan_lenght,
	     a.description
    FROM (
	      SELECT
		       table_name,
	      	   column_name,
	      	   data_type,
	      	   is_nullable,
	      	   column_default,
	      	   ISNULL(character_octet_length, numeric_precision) columan_lenght,
	      	   NULL description
	      FROM information_schema.columns
	      WHERE table_name = @name
	     ) a;
    FETCH NEXT FROM db_names INTO @name   
END   
CLOSE db_names   
DEALLOCATE db_names

-- To find the Object list
IF OBJECT_ID('tempdb.dbo.#object_list') IS NOT NULL 
	DROP TABLE #object_list;

CREATE TABLE #object_list
(
  name          VARCHAR(255),
  type_desc     VARCHAR(255),
  create_date   DATETIME,
  modify_date   DATETIME
);

INSERT INTO #object_list
SELECT
     name,
	 type_desc,
	 create_date,
	 modify_date 
FROM [sys].[all_objects]
WHERE schema_id =1 
AND type IN ('P','FN','V','TR','C','UQ')
ORDER BY 
    type,
	name;
	
-- To Find the User Roles
IF OBJECT_ID('tempdb.dbo.#user_roles') IS NOT NULL 
	DROP TABLE #user_roles;

CREATE TABLE #user_roles
(
  DatabaseRoleName	VARCHAR(255),
  DatabaseUserName  VARCHAR(255)
);

INSERT INTO #user_roles
SELECT
     rp.name AS DatabaseRoleName,
	 mp.name AS DatabaseUserName
FROM sys.database_role_members rm
INNER JOIN sys.database_principals rp ON rm.role_principal_id = rp.principal_id
INNER JOIN sys.database_principals mp ON rm.member_principal_id = mp.principal_id;

-- To Find The Database Size
IF OBJECT_ID('tempdb.dbo.#database_size') IS NOT NULL 
	DROP TABLE #database_size;

CREATE TABLE #database_size
(
 database_id			VARCHAR(255),
 Logical_Name			VARCHAR(255),
 Physical_Name			VARCHAR(500),
 state_desc				VARCHAR(255),
 recovery_model_desc	VARCHAR(255),
 total_size	            VARCHAR(255),
 data_size	            VARCHAR(255),
 data_used_size	        VARCHAR(255),
 log_size	            VARCHAR(255),
 log_used_size	        VARCHAR(255),
 full_last_date	        DATETIME,
 full_size	            VARCHAR(255),
 log_last_date	        DATETIME
);

IF OBJECT_ID('tempdb.dbo.#space') IS NOT NULL
    DROP TABLE #space

CREATE TABLE #space (
      database_id INT PRIMARY KEY
    , data_used_size DECIMAL(18,2)
    , log_used_size DECIMAL(18,2)
)

DECLARE @SQL NVARCHAR(MAX)

SELECT @SQL = STUFF((
    SELECT '
    USE [' + d.name + ']
    INSERT INTO #space (database_id, data_used_size, log_used_size)
    SELECT
          DB_ID()
        , SUM(CASE WHEN [type] = 0 THEN space_used END)
        , SUM(CASE WHEN [type] = 1 THEN space_used END)
    FROM (
        SELECT s.[type], space_used = SUM(FILEPROPERTY(s.name, ''SpaceUsed'') * 8. / 1024)
        FROM sys.database_files s
        GROUP BY s.[type]
    ) t;'
    FROM sys.databases d
    WHERE d.[state] = 0
    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')

EXEC sys.sp_executesql @SQL

INSERT INTO #database_size
SELECT
      d.database_id
    , t.name Logical_Name
	, t.Physical_Name
    , d.state_desc
    , d.recovery_model_desc
    , t.total_size
    , t.data_size
    , s.data_used_size
    , t.log_size
    , s.log_used_size
    , bu.full_last_date
    , bu.full_size
    , bu.log_last_date
FROM (
    SELECT
          database_id
		, name
	    , Physical_Name
        , log_size = CAST(SUM(CASE WHEN [type] = 1 THEN size END) * 8. / 1024 AS DECIMAL(18,2))
        , data_size = CAST(SUM(CASE WHEN [type] = 0 THEN size END) * 8. / 1024 AS DECIMAL(18,2))
        , total_size = CAST(SUM(size) * 8. / 1024 AS DECIMAL(18,2))
    FROM sys.master_files
	where DB_NAME(database_id) = @database
    GROUP BY database_id
	    , name
	    , Physical_Name
) t
JOIN sys.databases d ON d.database_id = t.database_id
LEFT JOIN #space s ON d.database_id = s.database_id
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

--To Find the Current Memory Allocation
IF OBJECT_ID('tempdb.dbo.#memory_allocation') IS NOT NULL 
	DROP TABLE #memory_allocation;
	
CREATE TABLE #memory_allocation
(
  sql_physical_memory_in_use_MB             VARCHAR(255), 
  sql_large_page_allocations_MB             VARCHAR(255), 
  sql_locked_page_allocations_MB            VARCHAR(255),
  sql_VirtulaAddressSpace_reserved_MB       VARCHAR(255), 
  sql_VirtulaAddressSpace_committed_MB      VARCHAR(255), 
  sql_VirtulaAddressSpace_available_MB      VARCHAR(255),
  sql_page_fault_count                      VARCHAR(255),
  sql_memory_utilization_percentage         VARCHAR(255), 
  sql_process_physical_memory_low           VARCHAR(255), 
  sql_process_virtual_memory_low            VARCHAR(255)
);

INSERT INTO #memory_allocation
SELECT 
    physical_memory_in_use_kb/1024             sql_physical_memory_in_use_MB, 
	large_page_allocations_kb/1024             sql_large_page_allocations_MB, 
	locked_page_allocations_kb/1024            sql_locked_page_allocations_MB,
	virtual_address_space_reserved_kb/1024     sql_VirtulaAddressSpace_reserved_MB, 
	virtual_address_space_committed_kb/1024    sql_VirtulaAddressSpace_committed_MB, 
	virtual_address_space_available_kb/1024    sql_VirtulaAddressSpace_available_MB,
	page_fault_count                           sql_page_fault_count,
	memory_utilization_percentage              sql_memory_utilization_percentage, 
	process_physical_memory_low                sql_process_physical_memory_low, 
	process_virtual_memory_low                 sql_process_virtual_memory_low
FROM sys.dm_os_process_memory; 

GO


-- To Verification
SELECT * FROM #table_list order by 1;
SELECT * FROM #integrity_constraints_index;
SELECT * FROM #privlileges order by 3;
SELECT * FROM #object_list order by 2;
SELECT * FROM #database_size;
SELECT * FROM #memory_allocation;
SELECT * FROM #user_roles;

-- To Drop the Temporary Tables
/*
DROP TABLE #table_list;
DROP TABLE #integrity_constraints_index;
DROP TABLE #privlileges;
DROP TABLE #object_list;
DROP TABLE #database_size;
DROP TABLE #memory_allocation;
DROP TABLE #user_roles;
DROP TABLE #space;
*/
