--Anonymous Block
USE [MerchantradeMoneyUAT]

DECLARE @CurrencyId    INT =2
DECLARE @FromDate      DATETIME = '2019-04-01'
DECLARE @ToDate        DATETIME = '2019-04-03'
DECLARE @CurrencyIds   INT

BEGIN
    --To Store the Intervals
    IF OBJECT_ID('tempdb..#RateInterval') IS NOT NULL 
	DROP TABLE #RateInterval;
    SELECT *
    INTO #RateInterval
    FROM
    (
        SELECT '09:00:00' AS Interval
        UNION
        SELECT '12:00:00' AS Interval
        UNION
        SELECT '14:00:00' AS Interval
        UNION
        SELECT '16:00:00' AS Interval
    ) a;
	--To Store the Final Results
	IF OBJECT_ID('tempdb..#temp_4') IS NOT NULL 
	DROP TABLE #temp_4;
	CREATE TABLE #temp_4  
    (
     RateDate	          DATETIME,
     Name	              NVARCHAR(MAX),
     ID	                  INT,
     Code	              NVARCHAR(MAX),
     RateTime	          VARCHAR(100),
     BuyingRate	          DECIMAL(25,15),
     SellingRate	      DECIMAL(25,15),
     WholeSaleBuyingRate  DECIMAL(25,15),
     WholeSaleSellingRate DECIMAL(25,15)
    );
END;
--To Fetch the Currency ID on Condition (Individual Or All)
DECLARE currencyid_cr  CURSOR READ_ONLY FOR  
                       SELECT c.Id
                       FROM dbo.Currency(NOLOCK) c
                       WHERE c.NumericCode <> 458
					   AND (c.ID = @CurrencyId OR  @CurrencyId=-1) 
					   ORDER BY c.ID;
SET NOCOUNT ON;
BEGIN					   
    OPEN currencyid_cr   
    FETCH NEXT FROM currencyid_cr INTO @CurrencyIds
    
    WHILE @@FETCH_STATUS = 0   
    BEGIN
	    BEGIN
		    IF OBJECT_ID('tempdb..#temp_1') IS NOT NULL 
	        DROP TABLE #temp_1;
			IF OBJECT_ID('tempdb..#temp_2') IS NOT NULL 
	        DROP TABLE #temp_2;
			IF OBJECT_ID('tempdb..#temp_3') IS NOT NULL 
	        DROP TABLE #temp_3;
		END;
		--To Store the History Data(#temp_1) 
        BEGIN     
            WITH cteDateRange
                AS 
            (
                SELECT CAST(@FromDate AS DATETIME) DateValue
                UNION ALL
                SELECT DateValue + 1
                FROM cteDateRange
                WHERE DateValue +1 <= @ToDate
            )
            SELECT dr.DateValue AS RateDate,
                    cr.Name,
                    cr.Id,
                    cr.Code,
                    ri.Interval AS RateTime,
                    isnull(rh.BuyingRate, 0.00) AS BuyingRate,
                    isnull(rh.SellingRate, 0.00) AS SellingRate,
                    isnull(rh.WholeSaleBuyingRate, 0.00) AS WholeSaleBuyingRate,
                    isnull(rh.WholeSaleSellingRate, 0.00) AS WholeSaleSellingRate INTO #temp_1
            FROM cteDateRange dr
            CROSS APPLY
            (
                SELECT c.[Name],
                       c.Id,
                       c.Code
                FROM dbo.Currency(NOLOCK) c
                WHERE c.NumericCode <> 458 AND c.Id = @CurrencyIds
            ) AS cr
            	JOIN #RateInterval ri ON 1 = 1
            	OUTER APPLY
            (
                SELECT TOP 1 rh1.SellingRate,
                             rh1.BuyingRate,
                             rh1.WholeSaleBuyingRate,
                             rh1.WholeSaleSellingRate,
                             rh1.ApprovedDate
                FROM dbo.RateHistory(NOLOCK) rh1 
                WHERE rh1.ApprovedDate < CASE   
            				WHEN ri.Interval = '09:00:00' THEN DATEADD(HOUR,-10, (dr.DateValue + CAST(ri.Interval AS DATETIME)))
            				WHEN ri.Interval = '12:00:00' THEN DATEADD(HOUR,0,	 (dr.DateValue + CAST(ri.Interval AS DATETIME)))
            				WHEN ri.Interval = '14:00:00' THEN DATEADD(HOUR,0,	 (dr.DateValue + CAST(ri.Interval AS DATETIME)))
            				WHEN ri.Interval = '16:00:00' THEN DATEADD(HOUR,0,   (dr.DateValue + CAST(ri.Interval AS DATETIME)))
            		END
            	AND rh1.IsApproved = 1
                AND cr.Id = rh1.CurrencyId
                ORDER BY rh1.ApprovedDate DESC
            ) rh
            ORDER BY 
                 RateDate;
        END;
        --To Store the Current Data(#temp_2)
        BEGIN
            WITH cteDateRange
                AS 
            (
                SELECT CAST(@FromDate AS DATETIME) DateValue
                UNION ALL
                SELECT DateValue + 1
                FROM cteDateRange
                WHERE DateValue +1 <= @ToDate
            )
            SELECT 
	    	     dr.DateValue AS RateDate,
                 cr.Name,
                 cr.Id,
                 cr.Code,
                 ri.Interval AS RateTime,
                 ISNULL(r.BuyingRate, 0.00) AS BuyingRate,
                 ISNULL(r.SellingRate, 0.00) AS SellingRate,
                 ISNULL(r.WholeSaleBuyingRate, 0.00) AS WholeSaleBuyingRate,
                 ISNULL(r.WholeSaleSellingRate, 0.00) AS WholeSaleSellingRate INTO #temp_2
            FROM cteDateRange dr
            CROSS APPLY
            (
                SELECT c.[Name],
                       c.Id,
                       c.Code
                FROM dbo.Currency(NOLOCK) c
                WHERE c.NumericCode <> 458 AND c.Id = @CurrencyIds
            ) AS cr
            	JOIN #RateInterval ri ON 1 = 1
            	OUTER APPLY
            (
                SELECT TOP 1 r.SellingRate,
                             r.BuyingRate,
                             r.WholeSaleBuyingRate,
                             r.WholeSaleSellingRate,
                             r.LastUpdatedDate
                FROM dbo.Rate(NOLOCK) r 
                WHERE r.LastUpdatedDate < CASE   
            				WHEN ri.Interval = '09:00:00' THEN DATEADD(HOUR,-10, (dr.DateValue + CAST(ri.Interval AS DATETIME)))
            				WHEN ri.Interval = '12:00:00' THEN DATEADD(HOUR,0,	 (dr.DateValue + CAST(ri.Interval AS DATETIME)))
            				WHEN ri.Interval = '14:00:00' THEN DATEADD(HOUR,0,	 (dr.DateValue + CAST(ri.Interval AS DATETIME)))
            				WHEN ri.Interval = '16:00:00' THEN DATEADD(HOUR,0,   (dr.DateValue + CAST(ri.Interval AS DATETIME)))
            		END
            
                AND cr.Id = r.CurrencyId
                ORDER BY r.LastUpdatedDate DESC
            ) r
            WHERE r.BuyingRate <> 0.00000000000000
            ORDER BY 
                 RateDate;
        END;
        --To Merge the History Data(#temp_1) with Current Data(#temp_2) into #temp_3
	    BEGIN
	        SELECT 
	        	 a.RateDate,
                 a.Name,
                 a.Id,
                 a.Code,
                 a.RateTime,
                 a.BuyingRate,
                 a.SellingRate,
                 a.WholeSaleBuyingRate,
                 a.WholeSaleSellingRate INTO #temp_3
	    	FROM
	    	    #temp_2 a;
        
	    	INSERT INTO #temp_3
	    	SELECT 
	        	 a.RateDate,
                 a.Name,
                 a.Id,
                 a.Code,
                 a.RateTime,
                 a.BuyingRate,
                 a.SellingRate,
                 a.WholeSaleBuyingRate,
                 a.WholeSaleSellingRate
	    	FROM
	    	    #temp_1 a
	    	WHERE NOT EXISTS (
	    	                 SELECT 1
	    					 FROM #temp_2 b
	    					 WHERE a.RateDate=b.RateDate
	    	                 );
        
	    	INSERT INTO #temp_3
	    	SELECT  
	        	 a.RateDate,
                 a.Name,
                 a.Id,
                 a.Code,
                 a.RateTime,
                 a.BuyingRate,
                 a.SellingRate,
                 a.WholeSaleBuyingRate,
                 a.WholeSaleSellingRate
	    	FROM
	    	    #temp_1 a
	    	WHERE NOT EXISTS (
	    	                 SELECT 1
	    					 FROM #temp_3 b
	    					 WHERE (a.RateDate=b.RateDate and a.RateTime = b.RateTime) 
	    	                 );
			--To Store Final Data(#temp_4) 
			INSERT INTO #temp_4 
			SELECT a.* FROM #temp_3 a;
	    END;
        FETCH NEXT FROM currencyid_cr INTO @CurrencyIds   
    END   
    CLOSE currencyid_cr;   
    DEALLOCATE currencyid_cr;
	--To Fetch the Final Data(#temp_4)
    SELECT 
	     a.RateDate,
         a.Name,
         a.Id,
         a.Code,
         a.RateTime,
         a.BuyingRate,
         a.SellingRate,
         a.WholeSaleBuyingRate,
         a.WholeSaleSellingRate
	FROM
	    #temp_4 a
	ORDER BY a.RateDate,a.id,a.RateTime;
	--To Drop the Temporary Tables
	BEGIN
        IF OBJECT_ID('tempdb..#temp_1') IS NOT NULL 
           DROP TABLE #temp_1;
    	IF OBJECT_ID('tempdb..#temp_2') IS NOT NULL 
           DROP TABLE #temp_2;
    	IF OBJECT_ID('tempdb..#temp_3') IS NOT NULL 
           DROP TABLE #temp_3;
		IF OBJECT_ID('tempdb..#temp_4') IS NOT NULL 
           DROP TABLE #temp_4;
		IF OBJECT_ID('tempdb..#RateInterval') IS NOT NULL 
           DROP TABLE #RateInterval;
    END;
	--OPTION (MAXRECURSION 1000);
END;

/*
RateDate                Name	  Id Code RateTime BuyingRate	     SellingRate	   WholeSaleBuyingRate WholeSaleSellingRate
----------------------- --------- -- ---- -------- ----------------- ----------------- ------------------- --------------------
2019-04-01 00:00:00.000	US Dollar 2	 USD  09:00:00 4.100000000000000 5.400000000000000 0.000000000000000   0.000000000000000
2019-04-01 00:00:00.000	US Dollar 2	 USD  12:00:00 4.100000000000000 5.400000000000000 0.000000000000000   0.000000000000000
2019-04-01 00:00:00.000	US Dollar 2	 USD  14:00:00 4.100000000000000 5.400000000000000 0.000000000000000   0.000000000000000
2019-04-01 00:00:00.000	US Dollar 2	 USD  16:00:00 4.100000000000000 5.400000000000000 0.000000000000000   0.000000000000000
2019-04-02 00:00:00.000	US Dollar 2	 USD  09:00:00 4.000000000000000 5.300000000000000 0.000000000000000   0.000000000000000
2019-04-02 00:00:00.000	US Dollar 2	 USD  12:00:00 4.000000000000000 5.300000000000000 0.000000000000000   0.000000000000000
2019-04-02 00:00:00.000	US Dollar 2	 USD  14:00:00 4.000000000000000 5.300000000000000 0.000000000000000   0.000000000000000
2019-04-02 00:00:00.000	US Dollar 2	 USD  16:00:00 4.000000000000000 5.300000000000000 0.000000000000000   0.000000000000000
2019-04-03 00:00:00.000	US Dollar 2	 USD  09:00:00 4.000000000000000 5.300000000000000 0.000000000000000   0.000000000000000
2019-04-03 00:00:00.000	US Dollar 2	 USD  12:00:00 4.000000000000000 5.300000000000000 0.000000000000000   0.000000000000000
2019-04-03 00:00:00.000	US Dollar 2	 USD  14:00:00 4.000000000000000 5.300000000000000 0.000000000000000   0.000000000000000
2019-04-03 00:00:00.000	US Dollar 2	 USD  16:00:00 4.000000000000000 5.300000000000000 0.000000000000000   0.000000000000000
*/
--StoredProcedure
USE [MerchantradeMoneyUAT]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
--EXEC spReportRateInterval_test 2, '2019-04-01','2019-04-02'
CREATE PROCEDURE [dbo].[spReportRateInterval_test]
(
 @CurrencyId INT,
 @FromDate   DATETIME,
 @ToDate     DATETIME
)
AS
BEGIN
    DECLARE @CurrencyIds   INT
BEGIN
    --To Store the Intervals
    IF OBJECT_ID('tempdb..#RateInterval') IS NOT NULL 
	DROP TABLE #RateInterval;
    SELECT *
    INTO #RateInterval
    FROM
    (
        SELECT '09:00:00' AS Interval
        UNION
        SELECT '12:00:00' AS Interval
        UNION
        SELECT '14:00:00' AS Interval
        UNION
        SELECT '16:00:00' AS Interval
    ) a;
	--To Store the Final Results
	IF OBJECT_ID('tempdb..#temp_4') IS NOT NULL 
	DROP TABLE #temp_4;
	CREATE TABLE #temp_4  
    (
     RateDate	          DATETIME,
     Name	              NVARCHAR(MAX),
     ID	                  INT,
     Code	              NVARCHAR(MAX),
     RateTime	          VARCHAR(100),
     BuyingRate	          DECIMAL(25,15),
     SellingRate	      DECIMAL(25,15),
     WholeSaleBuyingRate  DECIMAL(25,15),
     WholeSaleSellingRate DECIMAL(25,15)
    );
END;
--To Fetch the Currency ID on Condition (Individual Or All)
DECLARE currencyid_cr  CURSOR READ_ONLY FOR  
                       SELECT c.Id
                       FROM dbo.Currency(NOLOCK) c
                       WHERE c.NumericCode <> 458
					   AND (c.ID = @CurrencyId OR  @CurrencyId=-1) 
					   ORDER BY c.ID;
SET NOCOUNT ON;
BEGIN					   
    OPEN currencyid_cr   
    FETCH NEXT FROM currencyid_cr INTO @CurrencyIds
    
    WHILE @@FETCH_STATUS = 0   
    BEGIN
	    BEGIN
		    IF OBJECT_ID('tempdb..#temp_1') IS NOT NULL 
	        DROP TABLE #temp_1;
			IF OBJECT_ID('tempdb..#temp_2') IS NOT NULL 
	        DROP TABLE #temp_2;
			IF OBJECT_ID('tempdb..#temp_3') IS NOT NULL 
	        DROP TABLE #temp_3;
		END;
		--To Store the History Data(#temp_1) 
        BEGIN     
            WITH cteDateRange
                AS 
            (
                SELECT CAST(@FromDate AS DATETIME) DateValue
                UNION ALL
                SELECT DateValue + 1
                FROM cteDateRange
                WHERE DateValue +1 <= @ToDate
            )
            SELECT dr.DateValue AS RateDate,
                    cr.Name,
                    cr.Id,
                    cr.Code,
                    ri.Interval AS RateTime,
                    isnull(rh.BuyingRate, 0.00) AS BuyingRate,
                    isnull(rh.SellingRate, 0.00) AS SellingRate,
                    isnull(rh.WholeSaleBuyingRate, 0.00) AS WholeSaleBuyingRate,
                    isnull(rh.WholeSaleSellingRate, 0.00) AS WholeSaleSellingRate INTO #temp_1
            FROM cteDateRange dr
            CROSS APPLY
            (
                SELECT c.[Name],
                       c.Id,
                       c.Code
                FROM dbo.Currency(NOLOCK) c
                WHERE c.NumericCode <> 458 AND c.Id = @CurrencyIds
            ) AS cr
            	JOIN #RateInterval ri ON 1 = 1
            	OUTER APPLY
            (
                SELECT TOP 1 rh1.SellingRate,
                             rh1.BuyingRate,
                             rh1.WholeSaleBuyingRate,
                             rh1.WholeSaleSellingRate,
                             rh1.ApprovedDate
                FROM dbo.RateHistory(NOLOCK) rh1 
                WHERE rh1.ApprovedDate < CASE   
            				WHEN ri.Interval = '09:00:00' THEN DATEADD(HOUR,-10, (dr.DateValue + CAST(ri.Interval AS DATETIME)))
            				WHEN ri.Interval = '12:00:00' THEN DATEADD(HOUR,0,	 (dr.DateValue + CAST(ri.Interval AS DATETIME)))
            				WHEN ri.Interval = '14:00:00' THEN DATEADD(HOUR,0,	 (dr.DateValue + CAST(ri.Interval AS DATETIME)))
            				WHEN ri.Interval = '16:00:00' THEN DATEADD(HOUR,0,   (dr.DateValue + CAST(ri.Interval AS DATETIME)))
            		END
            	AND rh1.IsApproved = 1
                AND cr.Id = rh1.CurrencyId
                ORDER BY rh1.ApprovedDate DESC
            ) rh
            ORDER BY 
                 RateDate;
        END;
        --To Store the Current Data(#temp_2)
        BEGIN
            WITH cteDateRange
                AS 
            (
                SELECT CAST(@FromDate AS DATETIME) DateValue
                UNION ALL
                SELECT DateValue + 1
                FROM cteDateRange
                WHERE DateValue +1 <= @ToDate
            )
            SELECT 
	    	     dr.DateValue AS RateDate,
                 cr.Name,
                 cr.Id,
                 cr.Code,
                 ri.Interval AS RateTime,
                 ISNULL(r.BuyingRate, 0.00) AS BuyingRate,
                 ISNULL(r.SellingRate, 0.00) AS SellingRate,
                 ISNULL(r.WholeSaleBuyingRate, 0.00) AS WholeSaleBuyingRate,
                 ISNULL(r.WholeSaleSellingRate, 0.00) AS WholeSaleSellingRate INTO #temp_2
            FROM cteDateRange dr
            CROSS APPLY
            (
                SELECT c.[Name],
                       c.Id,
                       c.Code
                FROM dbo.Currency(NOLOCK) c
                WHERE c.NumericCode <> 458 AND c.Id = @CurrencyIds
            ) AS cr
            	JOIN #RateInterval ri ON 1 = 1
            	OUTER APPLY
            (
                SELECT TOP 1 r.SellingRate,
                             r.BuyingRate,
                             r.WholeSaleBuyingRate,
                             r.WholeSaleSellingRate,
                             r.LastUpdatedDate
                FROM dbo.Rate(NOLOCK) r 
                WHERE r.LastUpdatedDate < CASE   
            				WHEN ri.Interval = '09:00:00' THEN DATEADD(HOUR,-10, (dr.DateValue + CAST(ri.Interval AS DATETIME)))
            				WHEN ri.Interval = '12:00:00' THEN DATEADD(HOUR,0,	 (dr.DateValue + CAST(ri.Interval AS DATETIME)))
            				WHEN ri.Interval = '14:00:00' THEN DATEADD(HOUR,0,	 (dr.DateValue + CAST(ri.Interval AS DATETIME)))
            				WHEN ri.Interval = '16:00:00' THEN DATEADD(HOUR,0,   (dr.DateValue + CAST(ri.Interval AS DATETIME)))
            		END
            
                AND cr.Id = r.CurrencyId
                ORDER BY r.LastUpdatedDate DESC
            ) r
            WHERE r.BuyingRate <> 0.00000000000000
            ORDER BY 
                 RateDate;
        END;
        --To Merge the History Data(#temp_1) with Current Data(#temp_2) into #temp_3
	    BEGIN
	        SELECT 
	        	 a.RateDate,
                 a.Name,
                 a.Id,
                 a.Code,
                 a.RateTime,
                 a.BuyingRate,
                 a.SellingRate,
                 a.WholeSaleBuyingRate,
                 a.WholeSaleSellingRate INTO #temp_3
	    	FROM
	    	    #temp_2 a;
        
	    	INSERT INTO #temp_3
	    	SELECT 
	        	 a.RateDate,
                 a.Name,
                 a.Id,
                 a.Code,
                 a.RateTime,
                 a.BuyingRate,
                 a.SellingRate,
                 a.WholeSaleBuyingRate,
                 a.WholeSaleSellingRate
	    	FROM
	    	    #temp_1 a
	    	WHERE NOT EXISTS (
	    	                 SELECT 1
	    					 FROM #temp_2 b
	    					 WHERE a.RateDate=b.RateDate
	    	                 );
        
	    	INSERT INTO #temp_3
	    	SELECT  
	        	 a.RateDate,
                 a.Name,
                 a.Id,
                 a.Code,
                 a.RateTime,
                 a.BuyingRate,
                 a.SellingRate,
                 a.WholeSaleBuyingRate,
                 a.WholeSaleSellingRate
	    	FROM
	    	    #temp_1 a
	    	WHERE NOT EXISTS (
	    	                 SELECT 1
	    					 FROM #temp_3 b
	    					 WHERE (a.RateDate=b.RateDate and a.RateTime = b.RateTime) 
	    	                 );
			--To Store Final Data(#temp_4) 
			INSERT INTO #temp_4 
			SELECT a.* FROM #temp_3 a;
	    END;
        FETCH NEXT FROM currencyid_cr INTO @CurrencyIds   
    END   
    CLOSE currencyid_cr;   
    DEALLOCATE currencyid_cr;
	--To Fetch the Final Data(#temp_4)
    SELECT 
	     a.RateDate,
         a.Name,
         a.Id,
         a.Code,
         a.RateTime,
         a.BuyingRate,
         a.SellingRate,
         a.WholeSaleBuyingRate,
         a.WholeSaleSellingRate
	FROM
	    #temp_4 a
	ORDER BY a.RateDate,a.id,a.RateTime;
	--To Drop the Temporary Tables
	BEGIN
        IF OBJECT_ID('tempdb..#temp_1') IS NOT NULL 
           DROP TABLE #temp_1;
    	IF OBJECT_ID('tempdb..#temp_2') IS NOT NULL 
           DROP TABLE #temp_2;
    	IF OBJECT_ID('tempdb..#temp_3') IS NOT NULL 
           DROP TABLE #temp_3;
		IF OBJECT_ID('tempdb..#temp_4') IS NOT NULL 
           DROP TABLE #temp_4;
		IF OBJECT_ID('tempdb..#RateInterval') IS NOT NULL 
           DROP TABLE #RateInterval;
    END;
END;
END;