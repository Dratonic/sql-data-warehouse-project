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

create or alter procedure bronze.load_bronze as 
begin
	declare @start_time datetime , @end_time datetime ,@batch_start_time datetime , @batch_end_time datetime;
	begin try
		set @start_time = getdate();
		print'======================================';
		print'Loading Bronze Layer ';
		print'======================================';

		print'--------------------------------------';
		print'Loading CRM Tables';
		print'--------------------------------------';

		set @start_time = GETDATE();

		print'>> truncating table bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;
		print 'inserting data into table bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\devis\OneDrive\Desktop\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		);
		set @end_time = GETDATE();
		print'>> Load Duration: ' + cast(Datediff(second , @start_time , @end_time) AS nvarchar) + 'seconds';

		print'--------------------------------';


		set @start_time = GETDATE();
		print'>> truncating table bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
		print 'inserting data into table bronze.rm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\devis\OneDrive\Desktop\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		);
		set @end_time = GETDATE();
		print'>> Load Duration: ' + cast(Datediff(second , @start_time , @end_time) AS nvarchar) + 'seconds';
		print'--------------------------------';

		set @start_time = GETDATE();
		print '<< Truncating table bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
		print'inserting data into table bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\devis\OneDrive\Desktop\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE();
		print'>> Load Duration: ' + cast(Datediff(second , @start_time , @end_time) AS nvarchar) + 'seconds';
		print'--------------------------------';

		print'--------------------------------------';
		print'Loading ERP Tables';
		print'--------------------------------------';

		set @start_time = GETDATE();
		print'<<Truncating Table bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;
		print'Inserting Data into bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from'C:\Users\devis\OneDrive\Desktop\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE();
		print'>> Load Duration: ' + cast(Datediff(second , @start_time , @end_time) AS nvarchar) + 'seconds';
		print'--------------------------------';


		set @start_time = GETDATE();
		print'<<Truncating table bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;
		print'Inserting data into table bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from'C:\Users\devis\OneDrive\Desktop\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE();
		print'>> Load Duration: ' + cast(Datediff(second , @start_time , @end_time) AS nvarchar) + 'seconds';
		print'--------------------------------';


		set @start_time = GETDATE();
		print' <<Truncating table bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
		print'Inserting data into table bronze.erp_px_cat_g1v2;';
		bulk insert bronze.erp_px_cat_g1v2
		from'C:\Users\devis\OneDrive\Desktop\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE();
		print'>> Load Duration: ' + cast(Datediff(second , @start_time , @end_time) AS nvarchar) + 'seconds';
		print'--------------------------------';

		set @batch_end_time = getdate();
		print'=====================================';
		print 'Loading Bronze Layer is Competed';
		print '- Total load Duration :' + cast(datediff(second , @batch_start_time , @batch_end_time) as nvarchar) + 'seconds';
		print'=====================================';
	end try
	begin catch 
	print'====================================='
	print'Error While loading Bronze Layer'
	print'Error Message' + error_message();
	print'Error Message' +cast(error_message() as nvarchar);
	print'Error Message' +cast(error_state() as nvarchar);
	print'====================================='
	end catch
end 
