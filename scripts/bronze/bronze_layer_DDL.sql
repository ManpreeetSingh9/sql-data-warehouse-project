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
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

create or alter procedure  bronze.load_bronze as 
BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME , @batch_start_time DATETIME, @batch_end_time DATETIME ;
	BEGIN TRY
		set @batch_start_time = GETDATE();
		print '================================================';
		print 'Loading Bronze Layer ';
		print '================================================';


		print '------------------------------------------------';
		print 'Loading CRM Tables ';
		print '------------------------------------------------';

		set @start_time = GETDATE() ;
		print '>>Truncating Table : bronze.crm_cust_info ' ;
		Truncate Table bronze.crm_cust_info ;

		print '>>Inserting Data into :  bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info 
		from 'E:\python\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		with (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',' ,
		 TABLOCK
		);
		set @end_time = GETDATE() ;
		print '>> Load Duration : ' + cast( DATEDIFF(second , @start_time,@end_time ) as NVARCHAR) + 'seconds ';
		print '_____________________________________' ;

		set @start_time = GETDATE() ;
		print '>>Truncating Table : bronze.crm_prd_info ' ;
		Truncate Table bronze.crm_prd_info ;

		print '>>Inserting Data into :  bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		from 'E:\python\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		with (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',' ,
		 TABLOCK
		);
		set @end_time = GETDATE() ;
		print '>> Load Duration : ' + cast( DATEDIFF(second , @start_time,@end_time ) as NVARCHAR) + 'seconds ';
		print '_____________________________________' ;
		
		set @start_time = GETDATE() ;
		print '>>Truncating Table : bronze.crm_sales_details' ;
		Truncate Table bronze.crm_sales_details ;

		print '>>Inserting Data into :  bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		from 'E:\python\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		with (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',' ,
		 TABLOCK
		);
		set @end_time = GETDATE() ;
		print '>> Load Duration : ' + cast( DATEDIFF(second , @start_time,@end_time ) as NVARCHAR) + 'seconds ';
		print '_____________________________________' ;
	

		print '------------------------------------------------';
		print 'Loading ERP Tables ';
		print '------------------------------------------------';

		set @start_time = GETDATE() ;
		print '>>Truncating Table :bronze.erp_cust_az12' ;
		Truncate Table bronze.erp_cust_az12 ;

		print '>>Inserting Data into :  bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		from 'E:\python\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
		with (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',' ,
		 TABLOCK
		);
		set @end_time = GETDATE() ;
		print '>> Load Duration : ' + cast( DATEDIFF(second , @start_time,@end_time ) as NVARCHAR) + 'seconds ';
		print '_____________________________________' ;
	
		set @start_time = GETDATE() ;
		print '>>Truncating Table :bronze.erp_loc_a101' ;
		Truncate Table bronze.erp_loc_a101 ;

		print '>>Inserting Data into :   bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		from 'E:\python\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
		with (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',' ,
		 TABLOCK
		);
		set @end_time = GETDATE() ;
		print '>> Load Duration : ' + cast( DATEDIFF(second , @start_time,@end_time ) as NVARCHAR) + 'seconds ';
		print '_____________________________________' ;
	

	    set @start_time = GETDATE() ;
		print '>>Truncating Table :bronze.erp_px_cat_g1v2' ;
		Truncate Table bronze.erp_px_cat_g1v2 ;

		print '>>Inserting Data into :   erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		from 'E:\python\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
		with (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',' ,
		 TABLOCK
		);
		set @end_time = GETDATE() ;
		print '>> Load Duration : ' + cast( DATEDIFF(second , @start_time,@end_time ) as NVARCHAR) + 'seconds ';
		print '_____________________________________' ;

		set @batch_end_time = GETDATE();
		print'_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_';
		print '>> Bronze Layer Loaded ';
		print '>> Batch Load Duration : ' + cast( DATEDIFF(second , @batch_start_time,@batch_end_time ) as NVARCHAR) + 'seconds ';
		print'_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_';

	END TRY
	BEGIN CATCH
	print '==================================';
	print 'Error Occured During Loading Bronze Layer ' ;
	Print 'Error Message' + ERROR_MESSAGE() ;
	print 'Error Message ' + cast(ERROR_NUMBER () as NVARCHAR ) ;
	print '==================================';
	END CATCH
END

