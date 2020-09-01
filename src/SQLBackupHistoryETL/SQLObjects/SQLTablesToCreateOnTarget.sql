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
