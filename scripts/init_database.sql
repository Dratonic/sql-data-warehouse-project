/*
============================
CREATE DATABASE WITH SCHEMAS 
============================
SCRIPTS PURPOSE 
This scripts creates a new database named 'DataWarehouse' after checking if it already exists.
is yes then drop the previous one and create a new one.
*/

use master;
go
-- drop and recreate the 'Datawarehouse' database
if exists (select 1 from sys.database where name = 'DatawWarehouse')
begin 
  alter database DataWarehouse set single_user with rollback immediate;
  drop database DataWarehouse;
end;
go


create database DataWarehouse;

use dataWarehouse;


create schema bronze;
go
create schema silver;
go
create schema gold;
go
