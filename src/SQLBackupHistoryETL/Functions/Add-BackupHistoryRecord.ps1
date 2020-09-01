Function Add-BackupHistoryRecord {
    [cmdletbinding()]
    Param(
        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        $TargetServerInstance,

        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        $TargetDatabase,

        [Parameter(Mandatory = $false)]
        [pscredential]
        $CredentialObject,

        [String]
        $database_name,

        [String]
        $BackupType,

        [String]
        $physical_device_name,

        [datetime]
        $backup_start_date,

        [datetime]
        $backup_finish_date,

        [string]
        $server_name,

        [string]
        $recovery_model,

        [double]
        $first_lsn,

        [double]
        $last_lsn,

        [int]
        $UncompressedSizeMB,

        [int]
        $CompressedSizeMB

    )
  
    $query = @"
            
    insert into Utility.SQLBackupHistoryConsolidated
    (
        database_name
        ,BackupType
        ,physical_device_name
        ,backup_start_date
        ,backup_finish_date
        ,server_name
        ,recovery_model
        ,first_lsn
        ,last_lsn
        ,UncompressedSizeMB
        ,CompressedSizeMB
    )
    values
    (   N'$($database_name)'          
        ,'$($BackupType)'
        ,N'$($physical_device_name)'
        ,'$($backup_start_date)'
        ,'$($backup_finish_date)'
        ,N'$($server_name)'
        ,'$($recovery_model)'
        ,$first_lsn
        ,$last_lsn
        ,$UncompressedSizeMB
        ,$CompressedSizeMB
    );
        
"@

    try {
        
        if ($CredentialObject) {
            Invoke-Sqlcmd -ServerInstance $TargetServerInstance -query $query -Database $TargetDatabase -Credential $CredentialObject -ErrorAction Stop
        }

        else {
            Invoke-Sqlcmd -ServerInstance $TargetServerInstance -query $query -Database $TargetDatabase -ErrorAction Stop
        }
        
    }
    
    catch {
        Write-Error "Failed to write backup history record to target server: $ServerInstance"
        Write-Error "Error Message: $_.Exception.Message"
        exit
    }

    
    
}