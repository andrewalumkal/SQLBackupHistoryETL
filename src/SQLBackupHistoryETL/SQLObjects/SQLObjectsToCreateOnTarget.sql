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
   ,[is_copy_only]         bit            null  
)
with (data_compression = page);

alter table Utility.SQLBackupHistoryConsolidated add constraint PK_SQLBackupHistoryConsolidated primary key clustered (LogID) with (data_compression=page);

alter table Utility.SQLBackupHistoryConsolidated add constraint UQ_SQLBackupHistoryConsolidated unique ([last_lsn],[first_lsn],[database_name],[physical_device_name]) with (data_compression=page,ignore_dup_key=on);

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
   ,@is_copy_only         bit  
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
       ,is_copy_only
    )
    values
    (@database_name, @BackupType, @physical_device_name, @backup_start_date
    ,@backup_finish_date, @server_name, @ag_name, @recovery_model, @first_lsn
    ,@last_lsn, @UncompressedSizeMB, @CompressedSizeMB, @is_copy_only);

end;
go


create or alter proc Utility.GetLatestFullBackupFromSQLBackupHistoryConsolidated
    @DatabaseName nvarchar(200)
   ,@ServerName   nvarchar(250)
as
begin


    drop table if exists #BackupHistory;
    create table #BackupHistory
    (
        [BackupPath]       nvarchar(500)  null
       ,[BackupStartDate]  datetime       not null
       ,[BackupFinishDate] datetime       not null
       ,[FirstLSN]         numeric(25, 0) not null
       ,[LastLSN]          numeric(25, 0) not null
       ,[BackupType]       varchar(10)    not null
       ,[Rank]             int            not null
    );

    insert into #BackupHistory
    (
        BackupPath
       ,BackupStartDate
       ,BackupFinishDate
       ,FirstLSN
       ,LastLSN
       ,BackupType
       ,[Rank]
    )
    select  sbhc.physical_device_name
           ,sbhc.backup_start_date
           ,sbhc.backup_finish_date
           ,sbhc.first_lsn
           ,sbhc.last_lsn
           ,sbhc.BackupType
           ,dense_rank() over (order by sbhc.last_lsn desc) as [Rank]
    from    Utility.SQLBackupHistoryConsolidated sbhc
    where   sbhc.BackupType = 'Full'
    and     sbhc.database_name = @DatabaseName
    and     sbhc.server_name = @ServerName;

    --If no backups found using servername, check for backups using AG Name
    if @@ROWCOUNT = 0
    begin

        insert into #BackupHistory
        (
            BackupPath
           ,BackupStartDate
           ,BackupFinishDate
           ,FirstLSN
           ,LastLSN
           ,BackupType
           ,[Rank]
        )
        select  sbhc.physical_device_name
               ,sbhc.backup_start_date
               ,sbhc.backup_finish_date
               ,sbhc.first_lsn
               ,sbhc.last_lsn
               ,sbhc.BackupType
               ,dense_rank() over (order by sbhc.last_lsn desc) as [Rank]
        from    Utility.SQLBackupHistoryConsolidated sbhc
        where   sbhc.BackupType = 'Full'
        and     sbhc.database_name = @DatabaseName
        and     sbhc.ag_name = @ServerName;

    end;

    select  @DatabaseName as DatabaseName
           ,bh.BackupPath
           ,bh.BackupStartDate
           ,bh.BackupFinishDate
           ,bh.FirstLSN
           ,bh.LastLSN
           ,bh.BackupType
    from    #BackupHistory as bh
    where   bh.[Rank] = 1;

end;
go




create or alter proc Utility.GetRemainingLogBackupsFromSQLBackupHistoryConsolidated
    @DatabaseName nvarchar(200)
   ,@ServerName   nvarchar(250)
   ,@LastLSN      numeric(25, 0)
as
begin


    drop table if exists #BackupHistory;
    create table #BackupHistory
    (
        [BackupPath]       nvarchar(500)  null
       ,[BackupStartDate]  datetime       not null
       ,[BackupFinishDate] datetime       not null
       ,[FirstLSN]         numeric(25, 0) not null
       ,[LastLSN]          numeric(25, 0) not null
       ,[BackupType]       varchar(10)    not null
    );


    insert into #BackupHistory
    (
        BackupPath
       ,BackupStartDate
       ,BackupFinishDate
       ,FirstLSN
       ,LastLSN
       ,BackupType
    )
    select  sbhc.physical_device_name as BackupPath
           ,sbhc.backup_start_date as BackupStartDate
           ,sbhc.backup_finish_date as BackupFinishDate
           ,sbhc.first_lsn as FirstLSN
           ,sbhc.last_lsn as LastLSN
           ,sbhc.BackupType
    from    Utility.SQLBackupHistoryConsolidated as sbhc
    where   sbhc.BackupType = 'Log'
    and     sbhc.last_lsn > @LastLSN
    and     sbhc.database_name = @DatabaseName
    and     sbhc.server_name = @ServerName;


    --If no backups found using servername, check for backups using AG Name
    if @@ROWCOUNT = 0
    begin

        insert into #BackupHistory
        (
            BackupPath
           ,BackupStartDate
           ,BackupFinishDate
           ,FirstLSN
           ,LastLSN
           ,BackupType
        )
        select  sbhc.physical_device_name as BackupPath
               ,sbhc.backup_start_date as BackupStartDate
               ,sbhc.backup_finish_date as BackupFinishDate
               ,sbhc.first_lsn as FirstLSN
               ,sbhc.last_lsn as LastLSN
               ,sbhc.BackupType
        from    Utility.SQLBackupHistoryConsolidated as sbhc
        where   sbhc.BackupType = 'Log'
        and     sbhc.last_lsn > @LastLSN
        and     sbhc.database_name = @DatabaseName
        and     sbhc.ag_name = @ServerName;

    end;

    select      bh.BackupPath
               ,bh.BackupStartDate
               ,bh.BackupFinishDate
               ,bh.FirstLSN
               ,bh.LastLSN
               ,bh.BackupType
    from        #BackupHistory as bh
    order by    bh.LastLSN asc;


end;
go
