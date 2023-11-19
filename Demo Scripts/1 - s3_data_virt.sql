/*-------------------------------------------------------------------
-- 1 - s3_data_virt.sql
-- 
-- Summary: 
-- Show off S3 Data Virtualization against on-prem Pure Storage
-- FlashBlade
--
-- Written By: Andy Yun
-- Created On: 2023-11-01
-- PASS Data Community Summit 2023 | Pure Storage Vendor Session
--
-- Note:
-- Check S3 browser and delete prior 
-- AllSales_Flat parquet files first
-------------------------------------------------------------------*/
USE AutoDealershipDemo;
GO




-----
-- Introduce our test data
-- Row Count & Size on Disk
SELECT 
	schemas.name, objects.name, 
	dm_db_partition_stats.row_count,
	(dm_db_partition_stats.reserved_page_count * 8.0) / 1024 AS space_used_MB,
	dm_db_partition_stats.partition_number,
	indexes.index_id, indexes.name, indexes.type_desc
FROM sys.objects
INNER JOIN sys.schemas
	ON objects.schema_id = schemas.schema_id
INNER JOIN sys.indexes
	ON indexes.object_id = objects.object_id
INNER JOIN sys.dm_db_partition_stats
	ON dm_db_partition_stats.object_id = objects.object_id
	AND dm_db_partition_stats.index_id = indexes.index_id
WHERE objects.name = 'AllSales_Flat'
ORDER BY objects.name,
	dm_db_partition_stats.partition_number
GO

-- Data distribution by TransactionYear
SELECT 
	'Data distribution by TransactionYear' AS Label,
	TransactionYear,
	COUNT(1) AS TotalPerYear
FROM demo.AllSales_Flat
GROUP BY TransactionYear
ORDER BY TransactionYear
GO

-- Columns and datatypes
EXEC sp_help 'demo.AllSales_Flat'
GO

-- Sample of raw data
SELECT TOP 100 *
FROM demo.AllSales_Flat;
GO








/*
-----
-- Creates an external file format OBJECT for use in creating an external table
CREATE EXTERNAL FILE FORMAT parquet_file_format_object
WITH (
	FORMAT_TYPE = PARQUET
);
GO
*/
-----
-- Show current external data sources
SELECT *
FROM sys.external_data_sources;
GO

-----
-- Show current external file formats
SELECT *
FROM sys.external_file_formats;
GO








-----
-- Let's create an external table
-- 
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE name = 'AllSales_Flat')
	DROP EXTERNAL TABLE parquet.AllSales_Flat
GO

CREATE EXTERNAL TABLE parquet.AllSales_Flat
WITH (
	LOCATION = '/AllSales_Flat.parquet', 
	DATA_SOURCE = cetas_demo, 
	FILE_FORMAT = parquet_file_format_object
)
AS 
SELECT 
	[InventoryID], 
	[VIN]  COLLATE Latin1_General_100_BIN2_UTF8 AS [VIN], 
	[BaseModelID], [TrueCost], [InvoicePrice], [MSRP], [DateReceived], 
	[SalesHistoryID], [TransactionDate], [SellPrice], [MakeID], 
	[MakeName] COLLATE Latin1_General_100_BIN2_UTF8 AS [MakeName], 
	[ModelID], 
	[ModelName] COLLATE Latin1_General_100_BIN2_UTF8 AS [ModelName], 
	[ColorID], 
	[ColorName] COLLATE Latin1_General_100_BIN2_UTF8 AS [ColorName], 
	[ColorCode] COLLATE Latin1_General_100_BIN2_UTF8 AS [ColorCode], 
	[PackageID], 
	[PackageName] COLLATE Latin1_General_100_BIN2_UTF8 AS [PackageName], 
	[PackageCode] COLLATE Latin1_General_100_BIN2_UTF8 AS [PackageCode], 
	[Description] COLLATE Latin1_General_100_BIN2_UTF8 AS [Description], 
	[CustomerID], 
	[Customer_FirstName] COLLATE Latin1_General_100_BIN2_UTF8 AS [Customer_FirstName], 
	[Customer_LastName] COLLATE Latin1_General_100_BIN2_UTF8 AS [Customer_LastName], 
	[Customer_Address] COLLATE Latin1_General_100_BIN2_UTF8 AS [Customer_Address], 
	[Customer_City] COLLATE Latin1_General_100_BIN2_UTF8 AS [Customer_City], 
	[Customer_State] COLLATE Latin1_General_100_BIN2_UTF8 AS [Customer_State], 
	[Customer_ZipCode] COLLATE Latin1_General_100_BIN2_UTF8 AS [Customer_ZipCode], 
	[Customer_Email] COLLATE Latin1_General_100_BIN2_UTF8 AS [Customer_Email], 
	[Customer_PhoneNumber] COLLATE Latin1_General_100_BIN2_UTF8 AS [Customer_PhoneNumber], 
	[Customer_FirstVisit], [Customer_RepeatCustomer], [SalesPersonID], 
	[SalesPerson_FirstName],	-- Not converting collation for demo later
	[SalesPerson_LastName],	-- Not converting collation for demo later
	[SalesPerson_Email],	-- Not converting collation for demo later
	[SalesPerson_PhoneNumber],	-- Not converting collation for demo later
	[SalesPerson_DateOfHire], [SalesPerson_Salary], 
	[SalesPerson_CommissionRate], [AllSalesID], [TransactionYear]
FROM demo.AllSales_Flat;
GO




-----
-- Re-query Row Count & Size on Disk
--
-- Check S3 browser
-- How much capacity do all parquet files consume?
SELECT 
	schemas.name, objects.name, 
	dm_db_partition_stats.row_count,
	(dm_db_partition_stats.reserved_page_count * 8.0) / 1024 AS space_used_MB,
	dm_db_partition_stats.partition_number,
	indexes.index_id, indexes.name, indexes.type_desc
FROM sys.objects
INNER JOIN sys.schemas
	ON objects.schema_id = schemas.schema_id
INNER JOIN sys.indexes
	ON indexes.object_id = objects.object_id
INNER JOIN sys.dm_db_partition_stats
	ON dm_db_partition_stats.object_id = objects.object_id
	AND dm_db_partition_stats.index_id = indexes.index_id
WHERE objects.name = 'AllSales_Flat'
ORDER BY objects.name,
	dm_db_partition_stats.partition_number
GO








-----
-- Let's look at a little querying 
-- and performance characteristics


-----
-- SETUP: Flush buffer cache
DBCC DROPCLEANBUFFERS
GO
SET STATISTICS TIME ON
SET STATISTICS IO ON
GO








-----
-- SELECT all from original table & external parquet table
-- Ctrl-M: Actual Execution Plan: Check Actual Number of Rows Read
SELECT 
	TransactionYear,
	COUNT(1) AS TotalTransactions,
	SUM(SellPrice) AS AvgSellPrice
FROM demo.AllSales_Flat
GROUP BY TransactionYear
ORDER BY TransactionYear
GO
PRINT '------'
GO
SELECT 
	TransactionYear,
	COUNT(1) AS TotalTransactions,
	SUM(SellPrice) AS AvgSellPrice
FROM parquet.AllSales_Flat
GROUP BY TransactionYear
ORDER BY TransactionYear
GO
PRINT '--- END ---'
GO

-- Total Number of Logical Reads: 85526
-----







-----
-- What's in the buffer pool?
-- Let's use Glenn Berry's sys.dm_os_buffer_descriptors script
SELECT 
	OBJECT_NAME(p.[object_id]) AS [ObjectName],  
	p.index_id, COUNT(*)/128 AS [buffer size(MB)],  
	COUNT(*) AS [buffer_count] 
FROM sys.allocation_units AS a
INNER JOIN sys.dm_os_buffer_descriptors AS b
	ON a.allocation_unit_id = b.allocation_unit_id
INNER JOIN sys.partitions AS p
	ON a.container_id = p.hobt_id
WHERE b.database_id = DB_ID()
	AND p.[object_id] > 100
	AND (
		p.[object_id] = OBJECT_ID('demo.AllSales_Flat')
		OR p.[object_id] = OBJECT_ID('parquet.AllSales_Flat')
	)
GROUP BY p.[object_id], p.index_id
ORDER BY buffer_count DESC;

-- What's in the Buffer Pool?  
-- Any consequences/caveats?
-----








-----
-- Let's try some predicates
-- Ctrl-M: Actual Execution Plan: Check Actual Number of Rows Read
SELECT *
FROM demo.AllSales_Flat
WHERE AllSalesID = 12345
GO
PRINT '------'
GO
SELECT *
FROM parquet.AllSales_Flat
WHERE AllSalesID = 12345
GO








-----
-- DateReceived is not indexed
-- Ctrl-M: Actual Execution Plan: Check Actual Number of Rows Read
SELECT 
	TransactionYear,
	COUNT(1) AS TotalTransactions,
	SUM(SellPrice) AS AvgSellPrice
FROM demo.AllSales_Flat
WHERE DateReceived BETWEEN '01-01-2021' AND '04-01-2021'
GROUP BY TransactionYear
ORDER BY TransactionYear
GO
PRINT '------'
GO
SELECT 
	TransactionYear,
	COUNT(1) AS TotalTransactions,
	SUM(SellPrice) AS AvgSellPrice
FROM parquet.AllSales_Flat
WHERE DateReceived BETWEEN '01-01-2021' AND '04-01-2021'
GROUP BY TransactionYear
ORDER BY TransactionYear
GO








-----
-- What about string predicates?
-- Remember the COLLATE Latin1_General_100_BIN2_UTF8?
-- 
/*
[Customer_FirstName] COLLATE Latin1_General_100_BIN2_UTF8 AS [Customer_FirstName], 
[Customer_LastName] COLLATE Latin1_General_100_BIN2_UTF8 AS [Customer_LastName], 
[SalesPerson_FirstName],	-- Not converting collation for demo later
[SalesPerson_LastName],	-- Not converting collation for demo later
*/
-----
-- Query against a column with and without Latin1_General_100_BIN2_UTF8 collation
SELECT 
	'Latin1_General_100_BIN2_UTF8' AS Collation,
	TransactionYear,
	COUNT(1) AS TotalTransactions,
	SUM(SellPrice) AS AvgSellPrice
FROM parquet.AllSales_Flat
WHERE Customer_LastName = 'Smith'
GROUP BY TransactionYear
ORDER BY TransactionYear
GO
PRINT '------'
GO
SELECT 
	'SQL_Latin1_General_CP1_CI_AS' AS Collation,
	TransactionYear,
	COUNT(1) AS TotalTransactions,
	SUM(SellPrice) AS AvgSellPrice
FROM parquet.AllSales_Flat
WHERE SalesPerson_LastName = 'Smith'
GROUP BY TransactionYear
ORDER BY TransactionYear
GO








-----
-- Sargable LIKE predicate?
SELECT 
	'Latin1_General_100_BIN2_UTF8' AS Collation,
	TransactionYear,
	COUNT(1) AS TotalTransactions,
	SUM(SellPrice) AS AvgSellPrice
FROM parquet.AllSales_Flat
WHERE Customer_LastName LIKE 'Sm%'
GROUP BY TransactionYear
ORDER BY TransactionYear
GO
PRINT '------'
GO
SELECT 
	'SQL_Latin1_General_CP1_CI_AS' AS Collation,
	TransactionYear,
	COUNT(1) AS TotalTransactions,
	SUM(SellPrice) AS AvgSellPrice
FROM parquet.AllSales_Flat
WHERE SalesPerson_LastName LIKE 'Sm%'
GROUP BY TransactionYear
ORDER BY TransactionYear
GO








-----
-- Finally look at S3 browser; cetas-demo -> Properties -> Total Size
-- Then look at the FlashBlade bucket & DRR
-- https://sn1-fb-c07-23.puretec.purestorage.com/storage/objectstore/ayun/buckets/cetas-demo



-------------------------------------------------------------
-------------------------------------------------------------
-------------------------------------------------------------
-------------------------------------------------------------