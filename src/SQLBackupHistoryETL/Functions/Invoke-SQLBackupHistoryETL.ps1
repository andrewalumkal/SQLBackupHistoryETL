Function Invoke-SQLBackupHistoryETL {
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
        $TargetCredentialObject,

        [Parameter(Mandatory = $false)]
        [pscredential]
        $SourceCredentialObject

    )
  
    #Get all source SQL Servers along with ETL markers
    $AllSourceServers = Get-AllSourceServersToETL -TargetServerInstance $TargetServerInstance -TargetDatabase $TargetDatabase -CredentialObject $TargetCredentialObject

    foreach ($SourceServer in $AllSourceServers) {

        #Get backup history data
        $AllHistory = @(Get-BackupHistoryToETL -ServerInstance $SourceServer.ServerName -LastETLDateTime $SourceServer.LastETLDatetime -CredentialObject $SourceCredentialObject)

        #Get max datetime
        if ($AllHistory.Count -gt 0) {

            $MaxDateTimeObj = $AllHistory | Measure-Object -Property backup_finish_date -Maximum | Select-Object -Property Maximum
            [datetime]$MaxETLDateTime = $MaxDateTimeObj.Maximum
        

            Write-output "ETLing $($AllHistory.Count) records for $($SourceServer.ServerName)"

            #Inserting records one by one. Using this method since Write-SqlTableData does not support Azure SQL DB
            foreach ($HistoryRecord in $AllHistory) {
                Add-BackupHistoryRecord -TargetServerInstance $TargetServerInstance `
                    -TargetDatabase $TargetDatabase `
                    -CredentialObject $TargetCredentialObject `
                    -database_name $HistoryRecord.database_name `
                    -BackupType $HistoryRecord.BackupType `
                    -physical_device_name $HistoryRecord.physical_device_name `
                    -backup_start_date $HistoryRecord.backup_start_date `
                    -backup_finish_date $HistoryRecord.backup_finish_date `
                    -server_name $HistoryRecord.server_name `
                    -recovery_model $HistoryRecord.recovery_model `
                    -first_lsn $HistoryRecord.first_lsn `
                    -last_lsn $HistoryRecord.last_lsn `
                    -UncompressedSizeMB $HistoryRecord.UncompressedSizeMB `
                    -CompressedSizeMB $HistoryRecord.CompressedSizeMB
            }


            Update-LastETLDateTimeForServer -TargetServerInstance $TargetServerInstance `
                -TargetDatabase $TargetDatabase `
                -CredentialObject $TargetCredentialObject `
                -SourceServerToUpdate $SourceServer.ServerName `
                -MaxETLDateTime $MaxETLDateTime `
    
        }

        else {
            Write-Output "No backup history records found for $($SourceServer.ServerName)"
        }
        
    }
    
    
}