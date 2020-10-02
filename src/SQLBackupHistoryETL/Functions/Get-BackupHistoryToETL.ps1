Function Get-BackupHistoryToETL {
    [cmdletbinding()]
    Param(
        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        $ServerInstance,

        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        [Datetime]
        $LastETLDateTime,

        [Parameter(Mandatory = $false)]
        [pscredential]
        $CredentialObject

    )
  
    $query = @"
            
    set nocount on;
    drop table if exists #tmp_hadr_database_replica_states;
    drop table if exists #tmp_availability_groups;
    select  * into #tmp_hadr_database_replica_states from sys.dm_hadr_database_replica_states; 
    select  * into #tmp_availability_groups  from sys.availability_groups; 

    select      s.database_name
            ,case s.[type] when 'D' then 'Full'
                            when 'I' then 'Diff'
                            when 'L' then 'Log' end as BackupType
            ,m.physical_device_name
            ,s.backup_start_date
            ,s.backup_finish_date
            ,s.server_name
            ,grp.name as ag_name
            ,s.recovery_model
            ,s.first_lsn
            ,s.last_lsn
            ,convert(bigint, s.backup_size / 1048576) as [UncompressedSizeMB]
            ,convert(bigint, s.compressed_backup_size / 1048576) as [CompressedSizeMB]
            ,s.is_copy_only
    from        msdb.dbo.backupset s
    join        msdb.dbo.backupmediafamily m
    on          s.media_set_id = m.media_set_id

    join        sys.databases as d
    on          d.name = s.database_name

    left join   #tmp_hadr_database_replica_states as rs
    on          d.database_id = rs.database_id
    and         rs.is_local = 1

    left join   #tmp_availability_groups as grp
    on          grp.group_id = rs.group_id

    where       s.backup_finish_date >= '$($LastETLDateTIme)'
    order by    s.backup_finish_date asc;
        
"@

    try {

        if ($CredentialObject) {
            $BackupHistory = Invoke-Sqlcmd -ServerInstance $ServerInstance -query $query -Database msdb -Credential $CredentialObject -ErrorAction Stop
        }

        else {
            $BackupHistory = Invoke-Sqlcmd -ServerInstance $ServerInstance -query $query -Database msdb -ErrorAction Stop
        }

        return $BackupHistory
        
    }
    
    catch {
        Write-Error "Failed to retrieve backup history from Server: $ServerInstance"
        Write-Error "Error Message: $_.Exception.Message"
        return
    }

    
    
}