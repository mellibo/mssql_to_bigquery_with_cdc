use master
IF OBJECT_ID('test_db') IS NULL CREATE DATABASE test_db

use test_db

CREATE TABLE test_table (Id int primary key identity (1,1), time_created datetime default(getdate())
, str_field varchar(500), float_field float)
insert into test_table (str_field , float_field)
VALUES ('record1',1.1 ),('record2',2.2 ),('record3',3.3 ),('record4',4.4 ),('record5',5.5 ),('record6',6.6 )
GO
use test_db
EXEC sys.sp_cdc_enable_db 
create role cdc_role

EXEC sys.sp_cdc_enable_table  @source_schema = [dbo],  @source_name   = [test_table],  @role_name     = [cdc_role]
,  @supports_net_changes = 0

GO
use test_db
DECLARE @gcp_project varchar(200)
DECLARE @destination varchar(500) = @gcp_project + '.demo.test_table'

CREATE TABLE SyncParams(database_name sysname, table_name sysname, last_lsn_synced varbinary(10)
			, destination varchar(500), active int, last_sync_start datetime, last_sync_end datetime)

insert into SyncParams (database_name, table_name , last_lsn_synced , destination , active 
					, last_sync_start , last_sync_end )
VALUES ('test_db', 'test_table', null, @destination , 1,null, null)

SELECT * FROM test_db.dbo.SyncParams
  SELECT last_lsn_synced FROM test_db.dbo.SyncParams
            WHERE database_name='test_db' AND table_name = 'test_table' and destination = @destination  
			AND Active = 1 
/*
delete FROM test_db.dbo.SyncParams            

insert into test_table (str_field , float_field)
VALUES ('record7',7.7 )
DELETE FROM test_table  WHERE id = 1
UPDATE test_table  SET str_field ='update' where id = 2
UPDATE test_table  SET str_field ='update 2' where id = 2


*/