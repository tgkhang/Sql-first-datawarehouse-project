/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
Usage :
    EXEC bronze.load_bronze;
===============================================================================
*/

use Datawarehouse;
go


create or alter procedure bronze.load_bronze
as
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 

	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\PERSONAL PAGE\REPO\Sql-first-datawarehouse-project\datasets\source_crm\cust_info.csv'
		with (
			FIRSTROW =2,
			FIELDTERMINATOR= ',',
			--imporove performance, when loading to db, lock table
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		--SELECT * from bronze.crm_cust_info
		--SELECT count(*) from bronze.crm_cust_info

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\PERSONAL PAGE\REPO\Sql-first-datawarehouse-project\datasets\source_crm\prd_info.csv'
		with (
			FIRSTROW =2,
			FIELDTERMINATOR= ',',
			--imporove performance, when loading to db, lock table
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
		--select * from bronze.crm_prd_info


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details
		PRINT '>> Inserting Data into Table: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\PERSONAL PAGE\REPO\Sql-first-datawarehouse-project\datasets\source_crm\sales_details.csv'
		with (
			FIRSTROW =2,
			FIELDTERMINATOR= ',',
			--imporove performance, when loading to db, lock table
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
		--select * from bronze.crm_sales_details


		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12
		PRINT '>> Insert Data into Table: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'C:\PERSONAL PAGE\REPO\Sql-first-datawarehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			FIRSTROW =2,
			FIELDTERMINATOR= ',',
			--imporove performance, when loading to db, lock table
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
		--select * from bronze.erp_cust_az12


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101
		PRINT '>> Insert Data into Table: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'C:\PERSONAL PAGE\REPO\Sql-first-datawarehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			FIRSTROW =2,
			FIELDTERMINATOR= ',',
			--imporove performance, when loading to db, lock table
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
		--select * from bronze.erp_loc_a101


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2
		PRINT '>> Insert Data into Table: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\PERSONAL PAGE\REPO\Sql-first-datawarehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			FIRSTROW =2,
			FIELDTERMINATOR= ',',
			--imporove performance, when loading to db, lock table
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
		--select * from bronze.erp_px_cat_g1v2

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	
		END TRY
		BEGIN CATCH
			PRINT '=========================================='
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
			PRINT '=========================================='
		END CATCH
END;

exec bronze.load_bronze