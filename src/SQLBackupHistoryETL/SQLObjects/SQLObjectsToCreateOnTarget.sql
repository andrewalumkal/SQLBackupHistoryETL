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
   ,[encryptor_type]       nvarchar(32)   null
   ,[key_algorithm]        nvarchar(32)   null
   ,[device_type]		   tinyint        null
   ,[position]			   int            null
   ,[DBFileInformation]    nvarchar(max)  null
)
with (data_compression = page);

alter table Utility.SQLBackupHistoryConsolidated add constraint PK_SQLBackupHistoryConsolidated primary key clustered (LogID) with (data_compression=page);

alter table Utility.SQLBackupHistoryConsolidated add constraint UQ_SQLBackupHistoryConsolidated unique ([last_lsn],[first_lsn],[database_name],[physical_device_name]) with (data_compression=page,ignore_dup_key=on);

create nonclustered index [IX_database_server] ON [Utility].[SQLBackupHistoryConsolidated] ([database_name],[BackupType],[server_name],[last_lsn]) WITH (DATA_COMPRESSION = PAGE)

create nonclustered index [IX_database_ag] ON [Utility].[SQLBackupHistoryConsolidated] ([database_name],[BackupType],[ag_name],[last_lsn]) WITH (DATA_COMPRESSION = PAGE)

create nonclustered index [IX_backup_start_date] on [Utility].[SQLBackupHistoryConsolidated] (backup_start_date) WITH (DATA_COMPRESSION = PAGE)
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
   ,@physical_device_name nvarchar(500) = null
   ,@backup_start_date    datetime
   ,@backup_finish_date   datetime
   ,@server_name          nvarchar(250) = null
   ,@ag_name              nvarchar(250) = null
   ,@recovery_model       varchar(10) = null
   ,@first_lsn            numeric(25, 0)
   ,@last_lsn             numeric(25, 0)
   ,@UncompressedSizeMB   int = null
   ,@CompressedSizeMB     int = null
   ,@is_copy_only         bit = null
   ,@encryptor_type       nvarchar(32) = null
   ,@key_algorithm        nvarchar(32) = null
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
       ,encryptor_type
       ,key_algorithm
    )
    values
    (@database_name, @BackupType, @physical_device_name, @backup_start_date
    ,@backup_finish_date, @server_name, @ag_name, @recovery_model, @first_lsn
    ,@last_lsn, @UncompressedSizeMB, @CompressedSizeMB, @is_copy_only,@encryptor_type,@key_algorithm);

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


create or alter proc Utility.GetLastDiffBackupFromSQLBackupHistoryConsolidated
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
       ,[is_copy_only]     bit            null
    );


    insert into #BackupHistory
    (
        BackupPath
       ,BackupStartDate
       ,BackupFinishDate
       ,FirstLSN
       ,LastLSN
       ,BackupType
       ,is_copy_only
    )
    select  sbhc.physical_device_name as BackupPath
           ,sbhc.backup_start_date as BackupStartDate
           ,sbhc.backup_finish_date as BackupFinishDate
           ,sbhc.first_lsn as FirstLSN
           ,sbhc.last_lsn as LastLSN
           ,sbhc.BackupType
           ,sbhc.is_copy_only
    from    Utility.SQLBackupHistoryConsolidated as sbhc
    where   sbhc.BackupType in ( 'Diff', 'Full' )
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
           ,is_copy_only
        )
        select  sbhc.physical_device_name as BackupPath
               ,sbhc.backup_start_date as BackupStartDate
               ,sbhc.backup_finish_date as BackupFinishDate
               ,sbhc.first_lsn as FirstLSN
               ,sbhc.last_lsn as LastLSN
               ,sbhc.BackupType
               ,sbhc.is_copy_only
        from    Utility.SQLBackupHistoryConsolidated as sbhc
        where   sbhc.BackupType in ( 'Diff', 'Full' )
        and     sbhc.last_lsn > @LastLSN
        and     sbhc.database_name = @DatabaseName
        and     sbhc.ag_name = @ServerName;

    end;

    --Need to handle situations where there are new full backups after the LSN passed in. If so, we need to get only the latest diff backup prior to those full backups
    if exists
    (
        select  *
        from    #BackupHistory bh
        where   bh.BackupType = 'Full'
        and     bh.is_copy_only = 0
    )
    begin

        declare @FullbackupLastLSN numeric(25, 0);
        set @FullbackupLastLSN =
        (
            select      top 1   bh.LastLSN
            from        #BackupHistory bh
            where       bh.BackupType = 'Full'
            and         bh.is_copy_only = 0
            order by    bh.LastLSN asc
        );

        --Get only backups before this full backup
        delete  from #BackupHistory
        where   LastLSN >= @FullbackupLastLSN;

    end

    --Handle striped backups - there may be multiple files for a single diff backup

    ;
    with AvailableFullBackups
    as (select  bh.BackupPath
               ,bh.BackupStartDate
               ,bh.BackupFinishDate
               ,bh.FirstLSN
               ,bh.LastLSN
               ,bh.BackupType
               ,dense_rank() over (order by bh.LastLSN desc) as [Rank]
        from    #BackupHistory as bh
        where   bh.BackupType = 'Diff')

    select  afb.BackupPath
           ,afb.BackupStartDate
           ,afb.BackupFinishDate
           ,afb.FirstLSN
           ,afb.LastLSN
           ,afb.BackupType
    from    AvailableFullBackups afb
    where   afb.[Rank] = 1;


end;
go

create or alter proc Utility.CleanupSQLBackupHistoryConsolidated @BatchSize int = 500, @RetentionDays int = 180
as
begin

	set nocount on;
	set deadlock_priority low;
	set xact_abort on;

	declare @CleanupToDate datetime;
	select @CleanupToDate = max(sbhc.backup_start_date) from Utility.SQLBackupHistoryConsolidated as sbhc
	where sbhc.backup_start_date < dateadd(day,-1 * @RetentionDays,getutcdate())

	while 1=1
	begin

		delete top (@BatchSize) from Utility.SQLBackupHistoryConsolidated
		where backup_start_date < @CleanupToDate

		if @@ROWCOUNT = 0
		begin
			return;
		end

	end

end
go


alter proc Utility.GenerateRestoreScript
    @SourceDB        varchar(200) = null
   ,@DestinationDB   varchar(200) = null
   ,@SourceDBServer  varchar(200) = null
   ,@SourceAGName    varchar(200) = null
   ,@RestoreToTime   datetime     = null    --'yyyy-mm-dd hh:mi:ss', ie. '2012-04-27 22:19:20'
   ,@OutputAsPrint   bit          = 1       -- Additionally Output as PRINT statements in SSMS
   ,@RestoreDataPath varchar(500) = null    --'X:\MSSQL\DATA'
   ,@RestoreLogPath  varchar(500) = null    --'Y:\MSSQL\LOG'
   ,@FileNamePrefix  varchar(50)  = null    --'Restored_'
   ,@Help            bit          = 0
as
begin

    set nocount on;
    set xact_abort on;

    declare @last_lsn                   [numeric](25, 0)
           ,@ConcatenatedPhysicalDevice nvarchar(max)
           ,@IsAGDB                     bit;

    if (@Help = 1)
    begin
		
		drop table if exists #HelpTable
        create table #HelpTable (ID              int           not null identity(1, 1)
                                ,ProcParameters  nvarchar(max) null
                                ,[Description]   nvarchar(max) null
								,IsRequired		bit not null
                                ,Example         nvarchar(max) null
                                ,AdditionalInfo nvarchar(max) null);

		insert into #HelpTable (ProcParameters
		                       ,Description
							   ,IsRequired
		                       ,Example
		                       ,AdditionalInfo)
		values ('@SourceDB','Name of source database',1,'''MyDB1''','Required Parameter')
			   ,('@DestinationDB','Name of destination database',0,'''MyDB1_Restored''','Optional. Defaults to @SourceDB if not provided')
			   ,('@SourceDBServer','Name of source machine name (@@SERVERNAME). Use if source database is standalone. Do not include fully qualified domain name.',1,'''SQLQA01''','Must provide only one param - either @SourceDBServer or @SourceAGName')
			   ,('@SourceAGName','Name of source Availability Group (@@SERVERNAME). Use if source database is part of an AG group. Do not include fully qualified domain name.',1,'''AG01''','Must provide only one param - either @SourceDBServer or @SourceAGName')
			   ,('@RestoreToTime','Restore to point in time',0,'''2022-04-27 22:19:20''','Defaults to getutcdate()')
			   ,('@OutputAsPrint','Outputs all restore commands as PRINT statements for better formatting',0,'1','Defaults to 1 (true)')
			   ,('@RestoreDataPath','Provide data path to restore all data files',0,'''X:\MSSQL\DATA''','Defaults to the data path found in backup file')
			   ,('@RestoreLogPath','Provide log path to restore all log files',0,'''Y:\MSSQL\LOG''','Defaults to the log path found in backup file')
			   ,('@FileNamePrefix','Add a prefix to all restored physical files',0,'''Restored_''','Renames a file MyDBData01.mdf to Restored_MyDBData01.mdf')
			   ,('@Help','Output this help window :)',0,'1','Only generates this help window, does not generate any scripts')


		select * from  #HelpTable as ht

        return;
    end;

	if (@SourceDB is null or @SourceDB = '')
	begin
		; throw 50000, 'Please provide a valid @SourceDB. Execute ''exec Utility.GenerateRestoreScript @Help = 1'' for more information', 1;
	end


    --Provide only 1 param - either @SourceDBServer or @SourceAGName
    if  (    @SourceDBServer is null
     and     @SourceAGName is null)
    or  (   @SourceDBServer is not null
     and    @SourceAGName is not null)
    begin
        ; throw 50000, 'For standalone source DB''s, only provide @SourceDBServer. For AG source DB''s, only provide @SourceAGName. Execute ''exec Utility.GenerateRestoreScript @Help = 1'' for more information', 1;
    end;

	if (@DestinationDB is null or @DestinationDB = '')
	begin
		set @DestinationDB = @SourceDB
	end

    if @SourceDBServer is null
    begin
        set @IsAGDB = 1;
    end;
    else
    begin
        set @IsAGDB = 0;
    end;

    if (@RestoreToTime is null)
    begin
        set @RestoreToTime = getdate ();
    end;

    drop table if exists #AllBackupsToRestore;
    drop table if exists #Backups;

    create table #AllBackupsToRestore ([RestoreID]            int              identity(1, 1) not null
                                      ,[HistoryLogID]         int              not null
                                      ,[DatabaseName]         [nvarchar](200)  collate SQL_Latin1_General_CP1_CI_AS not null
                                      ,[BackupType]           [varchar](10)    collate SQL_Latin1_General_CP1_CI_AS not null
                                      ,[backup_start_date]    [datetime]       not null
                                      ,[backup_finish_date]   [datetime]       not null
                                      ,[RestoreCommand]       nvarchar(max)    null
                                      ,[physical_device_name] [nvarchar](max)  collate SQL_Latin1_General_CP1_CI_AS null
                                      ,[server_name]          [nvarchar](250)  collate SQL_Latin1_General_CP1_CI_AS null
                                      ,[ag_name]              [nvarchar](250)  collate SQL_Latin1_General_CP1_CI_AS null
                                      ,[recovery_model]       [varchar](10)    collate SQL_Latin1_General_CP1_CI_AS null
                                      ,[first_lsn]            [numeric](25, 0) not null
                                      ,[last_lsn]             [numeric](25, 0) not null
                                      ,[UncompressedSizeMB]   [int]            null
                                      ,[CompressedSizeMB]     [int]            null
                                      ,[is_copy_only]         [bit]            null
                                      ,[encryptor_type]       [nvarchar](32)   collate SQL_Latin1_General_CP1_CI_AS null
                                      ,[key_algorithm]        [nvarchar](32)   collate SQL_Latin1_General_CP1_CI_AS null);

    create table #Backups ([LogID]                [int]            not null
                          ,[database_name]        [nvarchar](200)  collate SQL_Latin1_General_CP1_CI_AS not null
                          ,[BackupType]           [varchar](10)    collate SQL_Latin1_General_CP1_CI_AS not null
                          ,[physical_device_name] [nvarchar](500)  collate SQL_Latin1_General_CP1_CI_AS null
                          ,[backup_start_date]    [datetime]       not null
                          ,[backup_finish_date]   [datetime]       not null
                          ,[server_name]          [nvarchar](250)  collate SQL_Latin1_General_CP1_CI_AS null
                          ,[ag_name]              [nvarchar](250)  collate SQL_Latin1_General_CP1_CI_AS null
                          ,[recovery_model]       [varchar](10)    collate SQL_Latin1_General_CP1_CI_AS null
                          ,[first_lsn]            [numeric](25, 0) not null
                          ,[last_lsn]             [numeric](25, 0) not null
                          ,[UncompressedSizeMB]   [int]            null
                          ,[CompressedSizeMB]     [int]            null
                          ,[is_copy_only]         [bit]            null
                          ,[encryptor_type]       [nvarchar](32)   collate SQL_Latin1_General_CP1_CI_AS null
                          ,[key_algorithm]        [nvarchar](32)   collate SQL_Latin1_General_CP1_CI_AS null
                          ,[device_type]          [tinyint]        null
                          ,[position]             [int]            null
                          ,[DBFileInformation]    [nvarchar](max)  collate SQL_Latin1_General_CP1_CI_AS null
                          ,[Rank]                 int              null);


    ------------------------GET FULL BACKUPS-----------------------------
    --Check using AGName
    if (@IsAGDB = 1)
    begin

        ;with cte
         as (select *
                   ,dense_rank () over (order by sbhc.last_lsn desc) as [Rank]
             from   Utility.SQLBackupHistoryConsolidated sbhc
             where  sbhc.BackupType = 'Full'
             and    sbhc.database_name = @SourceDB
             and    sbhc.ag_name = @SourceAGName
             and    sbhc.backup_start_date <= @RestoreToTime
			 and	sbhc.device_type in (2,9))
        insert into #Backups (LogID
                             ,database_name
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
                             ,encryptor_type
                             ,key_algorithm
                             ,device_type
                             ,position
                             ,DBFileInformation
                             ,[Rank])
        select      bh.LogID
                   ,bh.database_name
                   ,bh.BackupType
                   ,bh.physical_device_name
                   ,bh.backup_start_date
                   ,bh.backup_finish_date
                   ,bh.server_name
                   ,bh.ag_name
                   ,bh.recovery_model
                   ,bh.first_lsn
                   ,bh.last_lsn
                   ,bh.UncompressedSizeMB
                   ,bh.CompressedSizeMB
                   ,bh.is_copy_only
                   ,bh.encryptor_type
                   ,bh.key_algorithm
                   ,bh.device_type
                   ,bh.position
                   ,bh.DBFileInformation
                   ,bh.[Rank]
        from        cte as bh
        where       bh.[Rank] = 1
        order by    bh.physical_device_name
        option (fast 1);
    end;
    --Else check using ServerName
    else
    begin
        ;with cte
         as (select *
                   ,dense_rank () over (order by sbhc.last_lsn desc) as [Rank]
             from   Utility.SQLBackupHistoryConsolidated sbhc
             where  sbhc.BackupType = 'Full'
             and    sbhc.database_name = @SourceDB
             and    sbhc.server_name = @SourceDBServer
             and    sbhc.backup_start_date <= @RestoreToTime
			 and	sbhc.device_type in (2,9))
        insert into #Backups (LogID
                             ,database_name
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
                             ,encryptor_type
                             ,key_algorithm
                             ,device_type
                             ,position
                             ,DBFileInformation
                             ,[Rank])
        select      bh.LogID
                   ,bh.database_name
                   ,bh.BackupType
                   ,bh.physical_device_name
                   ,bh.backup_start_date
                   ,bh.backup_finish_date
                   ,bh.server_name
                   ,bh.ag_name
                   ,bh.recovery_model
                   ,bh.first_lsn
                   ,bh.last_lsn
                   ,bh.UncompressedSizeMB
                   ,bh.CompressedSizeMB
                   ,bh.is_copy_only
                   ,bh.encryptor_type
                   ,bh.key_algorithm
                   ,bh.device_type
                   ,bh.position
                   ,bh.DBFileInformation
                   ,bh.[Rank]
        from        cte as bh
        where       bh.[Rank] = 1
        order by    bh.physical_device_name
        option (fast 1);

    end;


    if not exists (select   top (1) * from  #Backups as b)
    begin
        ; throw 50000, 'No available full backups found', 1;
        return;
    end;

    ----Get moveto file information

    --Add backslash to path if not already provided
    set @RestoreDataPath
        = case when @RestoreDataPath = '' then null
			   when @RestoreDataPath is null then @RestoreDataPath
               when right(@RestoreDataPath, 1) <> '\' then
                   concat (@RestoreDataPath, '\') 
			   else @RestoreDataPath end;

    set @RestoreLogPath
        = case when @RestoreLogPath = '' then null
			   when @RestoreLogPath is null then @RestoreLogPath
               when right(@RestoreLogPath, 1) <> '\' then
                   concat (@RestoreLogPath, '\') 
			   else @RestoreLogPath end;

    --Have an example move command in case we didnt successfully get data file info from the SQLBackupHistoryConsolidated table
    declare @ExampleMoveCommand nvarchar(max)
        = char (13)
          + N'MOVE N''MyLogicalDataFile1'' TO N''X:\MSSQL\MyPhysicalDataFile01.mdf'', '
          + char (13)
          + N'MOVE N''MyLogicalDataFile2'' TO N''X:\MSSQL\MyPhysicalDataFile02.ndf'', '
          + char (13)
          + N'MOVE N''MyLogFile'' TO N''Y:\MSSQL\MyLogFile.ldf'' '
          + char (13);


    declare @jsonFileInfo nvarchar(max)
           ,@IsCopyOnly   bit;

    --Break out file information for moveto command
    select  top (1) @jsonFileInfo = b.DBFileInformation
                   ,@IsCopyOnly = b.is_copy_only
    from    #Backups as b;


    drop table if exists #DBFiles;
    select  logical_name as logical_name
           ,physical_drive as physical_drive
           ,physical_name as physical_name
           ,file_type as file_type
           ,file_number as file_number
           ,left(physical_name, len (physical_name)
                                - charindex ('\', reverse (physical_name), 1)
                                + 1) as LeafPath
           ,concat (
                coalesce (@FileNamePrefix, '')
               ,right(physical_name, charindex ('\', reverse (physical_name))
                                     - 1)) as [FileName]
    into    #DBFiles
    from
            openjson (@jsonFileInfo)
            with (logical_name varchar (100) '$.logical_name'
                 ,physical_drive varchar (100) '$.physical_drive'
                 ,physical_name varchar (100) '$.physical_name'
                 ,file_type varchar (100) '$.file_type'
                 ,file_number varchar (100) '$.file_number');

    declare @MoveCommand nvarchar(max);
    ;with FileMoveToBuilder
    as (select  *
               ,case when df.file_type = 'D' --DataFile
               then      concat (
                             coalesce (@RestoreDataPath, df.LeafPath)
                            ,df.FileName)
                     when df.file_type = 'L' --DataFile
               then      concat (
                             coalesce (@RestoreLogPath, df.LeafPath)
                            ,df.FileName) end as FullFilePath
        from    #DBFiles as df)
    select  @MoveCommand
        = string_agg (cast(N'' as nvarchar(max)) +
              ('MOVE N''' + fm.logical_name + ''' TO N''' + fm.FullFilePath
               + '''')
             ,(', ' + char (13)))
    from    FileMoveToBuilder as fm;



    --Create final restore command
    declare @RestoreCommand nvarchar(max);

    select  @RestoreCommand
        = N'RESTORE DATABASE [' + @DestinationDB + N'] FROM ' + char (13)
          + string_agg (cast(N'' as nvarchar(max)) +
                concat (
                    case when b.device_type = 9 then 'URL = N''' else
                                                                     'DISK = N''' end
                   ,b.physical_device_name
                   ,'''')
               ,',' + ' ' + char (13)) + char (13) +
        ------------MOVE COMMAND----------' +
        + ' WITH ' +coalesce (@MoveCommand, @ExampleMoveCommand)
          ------------MOVE COMMAND----------' +
          + char (13) + N',NORECOVERY,  NOUNLOAD,  STATS = 5;'
           ,@ConcatenatedPhysicalDevice
                = string_agg (cast(N'' as nvarchar(max)) + b.physical_device_name, ', ')
    from    #Backups as b;

    insert into #AllBackupsToRestore (HistoryLogID
                                     ,DatabaseName
                                     ,BackupType
                                     ,RestoreCommand
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
                                     ,encryptor_type
                                     ,key_algorithm)
    select  top 1   fb.LogID as HistoryLogID
                   ,fb.database_name as DatabaseName
                   ,fb.BackupType
                   ,@RestoreCommand
                   ,@ConcatenatedPhysicalDevice
                   ,fb.backup_start_date
                   ,fb.backup_finish_date
                   ,fb.server_name
                   ,fb.ag_name
                   ,fb.recovery_model
                   ,fb.first_lsn
                   ,fb.last_lsn
                   ,fb.UncompressedSizeMB
                   ,fb.CompressedSizeMB
                   ,fb.is_copy_only
                   ,fb.encryptor_type
                   ,fb.key_algorithm
    from    #Backups as fb;

    if @OutputAsPrint = 1
    begin
        print '----------------INITIAL FULL BACKUP RESTORE COMMAND----------------';
        print @RestoreCommand;
        print '-------------------------------------------------------------------';
    end;

    --Get the last lsn from the full backup
    select  top (1) @last_lsn = fb.last_lsn
    from    #Backups as fb;


    ------------------------GET DIFF BACKUPS-----------------------------
    if (@IsCopyOnly = 1)
    begin
        --if the last full backup is a copy only backup, we cant restore a diff backup, so skip diff backups

        if @OutputAsPrint = 1
        begin
            print char (13);
            print '-------------------------------------------------------------------';
            print '-----------------------NO DIFF BACKUPS FOUND-----------------------';
            print '-------------------------------------------------------------------';
        end;

        --Skip directly to log backups
        goto Log_Backups;

    end;


    truncate table #Backups;

    --Check using AGName
    if (@IsAGDB = 1)
    begin
        ;with cte
         as (select *
                   ,dense_rank () over (order by sbhc.last_lsn desc) as [Rank]
             from   Utility.SQLBackupHistoryConsolidated as sbhc
             where  sbhc.BackupType = 'Diff'
             and    sbhc.last_lsn > @last_lsn
             and    sbhc.database_name = @SourceDB
             and    sbhc.ag_name = @SourceAGName
             and    sbhc.backup_start_date <= @RestoreToTime)
        insert into #Backups (LogID
                             ,database_name
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
                             ,encryptor_type
                             ,key_algorithm
                             ,device_type
                             ,position
                             ,DBFileInformation
                             ,Rank)
        select  bh.LogID
               ,bh.database_name
               ,bh.BackupType
               ,bh.physical_device_name
               ,bh.backup_start_date
               ,bh.backup_finish_date
               ,bh.server_name
               ,bh.ag_name
               ,bh.recovery_model
               ,bh.first_lsn
               ,bh.last_lsn
               ,bh.UncompressedSizeMB
               ,bh.CompressedSizeMB
               ,bh.is_copy_only
               ,bh.encryptor_type
               ,bh.key_algorithm
               ,bh.device_type
               ,bh.position
               ,bh.DBFileInformation
               ,bh.Rank
        from    cte as bh
        where   bh.[Rank] = 1;

    end;
    --Else check using ServerName
    else
    begin
        ;with cte
         as (select *
                   ,dense_rank () over (order by sbhc.last_lsn desc) as [Rank]
             from   Utility.SQLBackupHistoryConsolidated as sbhc
             where  sbhc.BackupType = 'Diff'
             and    sbhc.last_lsn > @last_lsn
             and    sbhc.database_name = @SourceDB
             and    sbhc.server_name = @SourceDBServer
             and    sbhc.backup_start_date <= @RestoreToTime)
        insert into #Backups (LogID
                             ,database_name
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
                             ,encryptor_type
                             ,key_algorithm
                             ,device_type
                             ,position
                             ,DBFileInformation
                             ,Rank)
        select  bh.LogID
               ,bh.database_name
               ,bh.BackupType
               ,bh.physical_device_name
               ,bh.backup_start_date
               ,bh.backup_finish_date
               ,bh.server_name
               ,bh.ag_name
               ,bh.recovery_model
               ,bh.first_lsn
               ,bh.last_lsn
               ,bh.UncompressedSizeMB
               ,bh.CompressedSizeMB
               ,bh.is_copy_only
               ,bh.encryptor_type
               ,bh.key_algorithm
               ,bh.device_type
               ,bh.position
               ,bh.DBFileInformation
               ,bh.Rank
        from    cte as bh
        where   bh.[Rank] = 1;

    end;

    --Create restore command
    if exists (select   top (1) * from  #Backups as b)
    begin

        select  @RestoreCommand
            = N'RESTORE DATABASE [' + @DestinationDB + N'] FROM ' + char (13)
              + string_agg (cast(N'' as nvarchar(max)) +
                    concat (
                        case when b.device_type = 9 then 'URL = N''' else
                                                                         'DISK = N''' end
                       ,b.physical_device_name
                       ,'''')
                   ,',' + ' ' + char (13)) + N' WITH NORECOVERY,  STATS = 5'
        from    #Backups as b;

        insert into #AllBackupsToRestore (HistoryLogID
                                         ,DatabaseName
                                         ,BackupType
                                         ,RestoreCommand
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
                                         ,encryptor_type
                                         ,key_algorithm)
        select      top (1) db.LogID as HistoryLogID
                           ,db.database_name as DatabaseName
                           ,db.BackupType
                           ,@RestoreCommand
                           ,db.physical_device_name
                           ,db.backup_start_date
                           ,db.backup_finish_date
                           ,db.server_name
                           ,db.ag_name
                           ,db.recovery_model
                           ,db.first_lsn
                           ,db.last_lsn
                           ,db.UncompressedSizeMB
                           ,db.CompressedSizeMB
                           ,db.is_copy_only
                           ,db.encryptor_type
                           ,db.key_algorithm
        from        #Backups as db
        order by    db.physical_device_name asc;

        if @OutputAsPrint = 1
        begin
            print char (13);
            print '--------------------DIFF BACKUP RESTORE COMMAND--------------------';
            print @RestoreCommand;
            print '-------------------------------------------------------------------';
        end;


        --Get the last lsn from the diff backup
        select  top (1) @last_lsn = fb.last_lsn
        from    #Backups as fb;

    end;


    ------------------------GET LOG BACKUPS-----------------------------
    Log_Backups:

    truncate table #Backups;

    --Check using AGName
    if (@IsAGDB = 1)
    begin

        ;with cte
         as (select *
             from   Utility.SQLBackupHistoryConsolidated as sbhc with (forceseek)
             where  sbhc.BackupType = 'Log'
             and    sbhc.last_lsn > @last_lsn
             and    sbhc.database_name = @SourceDB
             and    sbhc.ag_name = @SourceAGName
             and    sbhc.backup_start_date <= @RestoreToTime
             union
             --Get the first log backup after @RestoreToTime to get any overlap data in the next backup
             select top (1) *
             from   Utility.SQLBackupHistoryConsolidated as sbhc with (forceseek)
             where  sbhc.BackupType = 'Log'
             and    sbhc.last_lsn > @last_lsn
             and    sbhc.database_name = @SourceDB
             and    sbhc.ag_name = @SourceAGName
             and    sbhc.backup_start_date >= @RestoreToTime)
        insert into #Backups (LogID
                             ,database_name
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
                             ,encryptor_type
                             ,key_algorithm
                             ,device_type
                             ,position
                             ,DBFileInformation)
        select  bh.LogID
               ,bh.database_name
               ,bh.BackupType
               ,bh.physical_device_name
               ,bh.backup_start_date
               ,bh.backup_finish_date
               ,bh.server_name
               ,bh.ag_name
               ,bh.recovery_model
               ,bh.first_lsn
               ,bh.last_lsn
               ,bh.UncompressedSizeMB
               ,bh.CompressedSizeMB
               ,bh.is_copy_only
               ,bh.encryptor_type
               ,bh.key_algorithm
               ,bh.device_type
               ,bh.position
               ,bh.DBFileInformation
        from    cte as bh
        option (fast 1);
    end;
    --Else check using ServerName
    else
    begin
        ;with cte
         as (select *
             from   Utility.SQLBackupHistoryConsolidated as sbhc with (forceseek)
             where  sbhc.BackupType = 'Log'
             and    sbhc.last_lsn > @last_lsn
             and    sbhc.database_name = @SourceDB
             and    sbhc.server_name = @SourceDBServer
             and    sbhc.backup_start_date <= @RestoreToTime
             union
             --Get the first log backup after @RestoreToTime to get any overlap data in the next backup
             select top (1) *
             from   Utility.SQLBackupHistoryConsolidated as sbhc with (forceseek)
             where  sbhc.BackupType = 'Log'
             and    sbhc.last_lsn > @last_lsn
             and    sbhc.database_name = @SourceDB
             and    sbhc.server_name = @SourceDBServer
             and    sbhc.backup_start_date >= @RestoreToTime)
        insert into #Backups (LogID
                             ,database_name
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
                             ,encryptor_type
                             ,key_algorithm
                             ,device_type
                             ,position
                             ,DBFileInformation)
        select  bh.LogID
               ,bh.database_name
               ,bh.BackupType
               ,bh.physical_device_name
               ,bh.backup_start_date
               ,bh.backup_finish_date
               ,bh.server_name
               ,bh.ag_name
               ,bh.recovery_model
               ,bh.first_lsn
               ,bh.last_lsn
               ,bh.UncompressedSizeMB
               ,bh.CompressedSizeMB
               ,bh.is_copy_only
               ,bh.encryptor_type
               ,bh.key_algorithm
               ,bh.device_type
               ,bh.position
               ,bh.DBFileInformation
        from    cte as bh
        option (fast 1);
    end;

    if exists (select   top (1) * from  #Backups as b)
    begin
        insert into #AllBackupsToRestore (HistoryLogID
                                         ,DatabaseName
                                         ,BackupType
                                         ,RestoreCommand
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
                                         ,encryptor_type
                                         ,key_algorithm)
        select      b.LogID as HistoryLogID
                   ,b.database_name as DatabaseName
                   ,b.BackupType
                   ,N'RESTORE LOG [' + @DestinationDB + N'] FROM '
                    + case when b.device_type = 9 then 'URL = N''' else
                                                                       'DISK = N''' end
                    + b.physical_device_name + ''' WITH FILE = '
                    + cast(b.position as varchar(50))
                    + ',NORECOVERY, NOUNLOAD, STATS = 10;' as RestoreCommand
                   ,b.physical_device_name
                   ,b.backup_start_date
                   ,b.backup_finish_date
                   ,b.server_name
                   ,b.ag_name
                   ,b.recovery_model
                   ,b.first_lsn
                   ,b.last_lsn
                   ,b.UncompressedSizeMB
                   ,b.CompressedSizeMB
                   ,b.is_copy_only
                   ,b.encryptor_type
                   ,b.key_algorithm
        from        #Backups as b
        order by    b.last_lsn asc;

        ---Update last 2 log restore command to have STOP AT Option (updating 2 to be safe)

        declare @StopAt nvarchar(500)
            = N', STOPAT = ''' + cast(@RestoreToTime as varchar(50)) + N''';';
        with cte
        as (select      top (2) abtr.RestoreCommand
            from        #AllBackupsToRestore as abtr
            where       abtr.BackupType = 'Log'
            order by    abtr.RestoreID desc)
        update  cte
        set     cte.RestoreCommand = replace (cte.RestoreCommand, ';', @StopAt);

    end;

    select      *
    from        #AllBackupsToRestore as abtr
    order by    abtr.RestoreID;

    if @OutputAsPrint = 1
    begin
        declare @MinVal int = (   select    min (abtr.RestoreID)
                                  from      #AllBackupsToRestore as abtr
                                  where     abtr.BackupType = 'Log');
        declare @MaxVal int = (   select    max (abtr.RestoreID)
                                  from      #AllBackupsToRestore as abtr
                                  where     abtr.BackupType = 'Log');

        print char (13);
        print '--------------------LOG BACKUP RESTORE COMMAND---------------------';

        while @MinVal <= @MaxVal
        begin

            declare @LogRestoreCommand nvarchar(max);

            select  @LogRestoreCommand = abtr.RestoreCommand
            from    #AllBackupsToRestore as abtr
            where   abtr.RestoreID = @MinVal;

            print @LogRestoreCommand;

            set @MinVal += 1;

        end;

        print '-------------------------------------------------------------------';

    end;

end;
GO
