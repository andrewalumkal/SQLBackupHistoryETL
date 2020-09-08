use [MYDB]
go

create schema [Utility] authorization [dbo]
go

create table Utility.SQLBackupHistoryConsolidated
(
    [LogID]                int            identity(1, 1) not null
   ,[database_name]        nvarchar(200)  not null
   ,[BackupType]           varchar(10)    not null
   ,[physical_device_name] nvarchar(500)  null
   ,[backup_start_date]    datetime       not null
   ,[backup_finish_date]   datetime       not null
   ,[server_name]          nvarchar(250)  null
   ,[ag_name]              nvarchar(250)  null 
   ,[recovery_model]       varchar(10)    null
   ,[first_lsn]            numeric(25, 0) not null
   ,[last_lsn]             numeric(25, 0) not null
   ,[UncompressedSizeMB]   int            null
   ,[CompressedSizeMB]     int            null
)
with (data_compression = page);

alter table Utility.SQLBackupHistoryConsolidated add constraint PK_SQLBackupHistoryConsolidated primary key clustered (LogID) with (data_compression=page);

alter table Utility.SQLBackupHistoryConsolidated add constraint UQ_SQLBackupHistoryConsolidated unique ([last_lsn],[first_lsn],[database_name]) with (data_compression=page,ignore_dup_key=on);

create nonclustered index [IX_database_backupfinishdate] on Utility.SQLBackupHistoryConsolidated ([database_name],[backup_finish_date]) with (data_compression=page);

go

create table Utility.SQLBackupHistorySourceServers
(
    [ServerName]      nvarchar(200) not null
   ,[LastETLDatetime] datetime2(3)  not null
)
with (data_compression = page);

alter table Utility.SQLBackupHistorySourceServers add constraint PK_SQLBackupHistorySourceServers primary key clustered ([ServerName]) with (data_compression=page);

alter table Utility.SQLBackupHistorySourceServers add constraint DF_LastETLDatetime default getutcdate() for [LastETLDatetime];
go

create or alter proc Utility.InsertSQLBackupHistory
    @database_name        nvarchar(200)
   ,@BackupType           varchar(10)
   ,@physical_device_name nvarchar(500)
   ,@backup_start_date    datetime
   ,@backup_finish_date   datetime
   ,@server_name          nvarchar(250)
   ,@ag_name              nvarchar(250)
   ,@recovery_model       varchar(10)
   ,@first_lsn            numeric(25, 0)
   ,@last_lsn             numeric(25, 0)
   ,@UncompressedSizeMB   int
   ,@CompressedSizeMB     int
as
begin

    set nocount on;
    set xact_abort on;

    insert into Utility.SQLBackupHistoryConsolidated
    (
        database_name
       ,BackupType
       ,physical_device_name
       ,backup_start_date
       ,backup_finish_date
       ,server_name
       ,ag_name
       ,recovery_model
       ,first_lsn
       ,last_lsn
       ,UncompressedSizeMB
       ,CompressedSizeMB
    )
    values
    (@database_name, @BackupType, @physical_device_name, @backup_start_date
    ,@backup_finish_date, @server_name, @ag_name, @recovery_model, @first_lsn
    ,@last_lsn, @UncompressedSizeMB, @CompressedSizeMB);

end;
go
