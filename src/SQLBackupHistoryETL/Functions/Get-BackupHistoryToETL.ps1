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
            
    select  s.database_name
           ,case s.[type] when 'D' then 'Full'
                          when 'I' then 'Diff'
                          when 'L' then 'Log' end as BackupType
           ,m.physical_device_name
           ,s.backup_start_date
           ,s.backup_finish_date
           ,s.server_name
           ,s.recovery_model
           ,s.first_lsn
           ,s.last_lsn
           ,convert(bigint, s.backup_size / 1048576) as [UncompressedSizeMB]
           ,convert(bigint, s.compressed_backup_size / 1048576) as [CompressedSizeMB]
    from        msdb.dbo.backupset s
    inner join  msdb.dbo.backupmediafamily m
    on          s.media_set_id = m.media_set_id
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