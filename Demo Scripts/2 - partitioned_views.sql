/*-------------------------------------------------------------------
-- 2 - partitioned_views.sql
-- 
-- Summary: 
-- Showcase "partitioned views" with S3 Data Virtualization and
-- Parquet files
--
-- Written By: Andy Yun
-- Created On: 2023-11-01
-- PASS Data Community Summit 2023 | Pure Storage Vendor Session
-------------------------------------------------------------------*/
USE AutoDealershipDemo;
GO




-----
-- Introduce our test data
EXEC sp_helptext 'parquet.pv_AllSales';
GO




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
WHERE objects.name LIKE 'AllSales_20%'
ORDER BY objects.name,
	dm_db_partition_stats.partition_number
GO




-- Data distribution by TransactionYear
SELECT 
	TransactionYear,
	COUNT(1) AS TotalPerYear
FROM parquet.pv_AllSales
GROUP BY TransactionYear
ORDER BY TransactionYear
GO



-----
-- Create parquet files for AllSales_20nn for
-- 2019, 2020, 2021, & 2022 
-- but NOT for 2023
IF EXISTS(SELECT * FROM sys.external_tables WHERE name = 'AllSales_2019')
	DROP EXTERNAL TABLE parquet.AllSales_2019
GO

CREATE EXTERNAL TABLE parquet.AllSales_2019
WITH (
	LOCATION = '/AllSales_2019.parquet', 
	DATA_SOURCE = cetas_demo, 
	FILE_FORMAT = parquet_file_format_object
)
AS 
SELECT [InventoryID], 
	[VIN] COLLATE Latin1_General_100_BIN2_UTF8 AS [VIN], 
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
FROM demo.AllSales_2019;
GO

---
IF EXISTS(SELECT * FROM sys.external_tables WHERE name = 'AllSales_2020')
	DROP EXTERNAL TABLE parquet.AllSales_2020
GO

CREATE EXTERNAL TABLE parquet.AllSales_2020
WITH (
	LOCATION = '/AllSales_2020.parquet', 
	DATA_SOURCE = cetas_demo, 
	FILE_FORMAT = parquet_file_format_object
)
AS 
SELECT [InventoryID], 
	[VIN] COLLATE Latin1_General_100_BIN2_UTF8 AS [VIN], 
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
FROM demo.AllSales_2020;
GO

---
IF EXISTS(SELECT * FROM sys.external_tables WHERE name = 'AllSales_2021')
	DROP EXTERNAL TABLE parquet.AllSales_2021
GO

CREATE EXTERNAL TABLE parquet.AllSales_2021
WITH (
	LOCATION = '/AllSales_2021.parquet', 
	DATA_SOURCE = cetas_demo, 
	FILE_FORMAT = parquet_file_format_object
)
AS 
SELECT [InventoryID], 
	[VIN] COLLATE Latin1_General_100_BIN2_UTF8 AS [VIN], 
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
FROM demo.AllSales_2021;
GO

---
IF EXISTS(SELECT * FROM sys.external_tables WHERE name = 'AllSales_2022')
	DROP EXTERNAL TABLE parquet.AllSales_2022
GO

CREATE EXTERNAL TABLE parquet.AllSales_2022
WITH (
	LOCATION = '/AllSales_2022.parquet', 
	DATA_SOURCE = cetas_demo, 
	FILE_FORMAT = parquet_file_format_object
)
AS 
SELECT [InventoryID], 
	[VIN] COLLATE Latin1_General_100_BIN2_UTF8 AS [VIN], 
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
FROM demo.AllSales_2022;
GO

-- END Create
-----








-----
-- Check external tables
SELECT 
	external_tables.name, external_tables.location, 
	external_data_sources.name, external_data_sources.location, 
	sys.external_data_sources.pushdown,
	external_tables.create_date, external_tables.object_id
FROM sys.external_tables
INNER JOIN sys.external_data_sources
	ON external_tables.data_source_id = external_data_sources.data_source_id
WHERE external_tables.name LIKE 'AllSales%'
ORDER BY external_tables.name
GO








-----
-- Create a "partitioned view" the four 
-- parquet.AllSales_20nn external tables plus
-- AllSales_2023 local table
CREATE OR ALTER VIEW parquet.pv_AllSales
/* WITH SCHEMABINDING */
AS
SELECT [InventoryID], [VIN], [BaseModelID], [TrueCost], [InvoicePrice], [MSRP], [DateReceived], [SalesHistoryID], [TransactionDate], [SellPrice], [MakeID], [MakeName], [ModelID], [ModelName], [ColorID], [ColorName], [ColorCode], [PackageID], [PackageName], [PackageCode], [Description], [CustomerID], [Customer_FirstName], [Customer_LastName], [Customer_Address], [Customer_City], [Customer_State], [Customer_ZipCode], [Customer_Email], [Customer_PhoneNumber], [Customer_FirstVisit], [Customer_RepeatCustomer], [SalesPersonID], [SalesPerson_FirstName], [SalesPerson_LastName], [SalesPerson_Email], [SalesPerson_PhoneNumber], [SalesPerson_DateOfHire], [SalesPerson_Salary], [SalesPerson_CommissionRate], [AllSalesID], [TransactionYear]
FROM parquet.AllSales_2019
UNION ALL
SELECT [InventoryID], [VIN], [BaseModelID], [TrueCost], [InvoicePrice], [MSRP], [DateReceived], [SalesHistoryID], [TransactionDate], [SellPrice], [MakeID], [MakeName], [ModelID], [ModelName], [ColorID], [ColorName], [ColorCode], [PackageID], [PackageName], [PackageCode], [Description], [CustomerID], [Customer_FirstName], [Customer_LastName], [Customer_Address], [Customer_City], [Customer_State], [Customer_ZipCode], [Customer_Email], [Customer_PhoneNumber], [Customer_FirstVisit], [Customer_RepeatCustomer], [SalesPersonID], [SalesPerson_FirstName], [SalesPerson_LastName], [SalesPerson_Email], [SalesPerson_PhoneNumber], [SalesPerson_DateOfHire], [SalesPerson_Salary], [SalesPerson_CommissionRate], [AllSalesID], [TransactionYear]
FROM parquet.AllSales_2020
UNION ALL
SELECT [InventoryID], [VIN], [BaseModelID], [TrueCost], [InvoicePrice], [MSRP], [DateReceived], [SalesHistoryID], [TransactionDate], [SellPrice], [MakeID], [MakeName], [ModelID], [ModelName], [ColorID], [ColorName], [ColorCode], [PackageID], [PackageName], [PackageCode], [Description], [CustomerID], [Customer_FirstName], [Customer_LastName], [Customer_Address], [Customer_City], [Customer_State], [Customer_ZipCode], [Customer_Email], [Customer_PhoneNumber], [Customer_FirstVisit], [Customer_RepeatCustomer], [SalesPersonID], [SalesPerson_FirstName], [SalesPerson_LastName], [SalesPerson_Email], [SalesPerson_PhoneNumber], [SalesPerson_DateOfHire], [SalesPerson_Salary], [SalesPerson_CommissionRate], [AllSalesID], [TransactionYear]
FROM parquet.AllSales_2021
UNION ALL
SELECT [InventoryID], [VIN], [BaseModelID], [TrueCost], [InvoicePrice], [MSRP], [DateReceived], [SalesHistoryID], [TransactionDate], [SellPrice], [MakeID], [MakeName], [ModelID], [ModelName], [ColorID], [ColorName], [ColorCode], [PackageID], [PackageName], [PackageCode], [Description], [CustomerID], [Customer_FirstName], [Customer_LastName], [Customer_Address], [Customer_City], [Customer_State], [Customer_ZipCode], [Customer_Email], [Customer_PhoneNumber], [Customer_FirstVisit], [Customer_RepeatCustomer], [SalesPersonID], [SalesPerson_FirstName], [SalesPerson_LastName], [SalesPerson_Email], [SalesPerson_PhoneNumber], [SalesPerson_DateOfHire], [SalesPerson_Salary], [SalesPerson_CommissionRate], [AllSalesID], [TransactionYear]
FROM parquet.AllSales_2022
UNION ALL
SELECT [InventoryID], 
	[VIN] COLLATE Latin1_General_100_BIN2_UTF8 AS [VIN], 
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
	[SalesPerson_FirstName],	-- COLLATE Latin1_General_100_BIN2_UTF8 AS [SalesPerson_FirstName], 
	[SalesPerson_LastName],	-- COLLATE Latin1_General_100_BIN2_UTF8 AS [SalesPerson_LastName], 
	[SalesPerson_Email],	-- COLLATE Latin1_General_100_BIN2_UTF8 AS [SalesPerson_Email], 
	[SalesPerson_PhoneNumber],	-- COLLATE Latin1_General_100_BIN2_UTF8 AS [SalesPerson_PhoneNumber], 
	[SalesPerson_DateOfHire], [SalesPerson_Salary], 
	[SalesPerson_CommissionRate], [AllSalesID], [TransactionYear]
FROM demo.AllSales_2023
GO

-- END Create
-----








-----
-- Query with a single partitioning key value
-- Ctrl-M: Actual Execution Plan
SET STATISTICS TIME ON
GO
SELECT 
	TransactionYear,
	COUNT(1) AS TotalTransactions,
	SUM(SellPrice) AS AvgSellPrice
FROM demo.pv_AllSales
WHERE TransactionYear = 2021
GROUP BY TransactionYear
ORDER BY TransactionYear;
GO
PRINT '--------'
GO
SELECT 
	TransactionYear,
	COUNT(1) AS TotalTransactions,
	SUM(SellPrice) AS AvgSellPrice
FROM parquet.pv_AllSales
WHERE TransactionYear = 2021
GROUP BY TransactionYear
ORDER BY TransactionYear;
GO








-----
-- Query with multiple partitioning key values
-- including 2023 to include regular table
SELECT 
	TransactionYear,
	COUNT(1) AS TotalTransactions,
	SUM(SellPrice) AS AvgSellPrice
FROM demo.pv_AllSales
WHERE TransactionYear IN (2021, 2022, 2023)
	AND Customer_LastName = 'Smith'
GROUP BY TransactionYear
ORDER BY TransactionYear;
GO
PRINT '--------'
GO
SELECT 
	TransactionYear,
	COUNT(1) AS TotalTransactions,
	SUM(SellPrice) AS AvgSellPrice
FROM parquet.pv_AllSales
WHERE TransactionYear IN (2021, 2022, 2023)
	AND Customer_LastName = 'Smith'
GROUP BY TransactionYear
ORDER BY TransactionYear;
GO








-----
-- Query with an additional string predicate
-- [Customer_LastName] COLLATE Latin1_General_100_BIN2_UTF8 AS [Customer_LastName], 
-- [SalesPerson_LastName],	-- Not converting collation for demo later


-- With matching collation column
SELECT 
	'Latin1_General_100_BIN2_UTF8' AS Collation,
	TransactionYear,
	COUNT(1) AS TotalTransactions,
	SUM(SellPrice) AS AvgSellPrice
FROM demo.pv_AllSales
WHERE TransactionYear IN (2021, 2022, 2023)
	AND Customer_LastName = 'smith'
GROUP BY TransactionYear
ORDER BY TransactionYear;
GO
PRINT '--------'
GO
SELECT 
	'Latin1_General_100_BIN2_UTF8' AS Collation,
	TransactionYear,
	COUNT(1) AS TotalTransactions,
	SUM(SellPrice) AS AvgSellPrice
FROM parquet.pv_AllSales
WHERE TransactionYear IN (2021, 2022, 2023)
	AND Customer_LastName = 'Smith'
GROUP BY TransactionYear
ORDER BY TransactionYear;
GO








-- With mismatched collation column
SELECT 
	'SQL_Latin1_General_CP1_CI_AS' AS Collation,
	TransactionYear,
	COUNT(1) AS TotalTransactions,
	SUM(SellPrice) AS AvgSellPrice
FROM demo.pv_AllSales
WHERE TransactionYear IN (2021, 2022, 2023)
	AND SalesPerson_LastName = 'Smith'
GROUP BY TransactionYear
ORDER BY TransactionYear;
GO
PRINT '--------'
GO
SELECT 
	'SQL_Latin1_General_CP1_CI_AS' AS Collation,
	TransactionYear,
	COUNT(1) AS TotalTransactions,
	SUM(SellPrice) AS AvgSellPrice
FROM parquet.pv_AllSales
WHERE TransactionYear IN (2021, 2022, 2023)
	AND SalesPerson_LastName = 'Smith'
GROUP BY TransactionYear
ORDER BY TransactionYear;
GO
