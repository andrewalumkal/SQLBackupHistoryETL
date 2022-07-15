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
        [ValidateNotNullOrEmpty()]
        [pscredential]
        $TargetCredentialObject,

        [Parameter(Mandatory = $false)]
        [ValidateNotNullOrEmpty()]
        $TargetAzureDBCertificateAuth,

        [Parameter(Mandatory = $false)]
        [ValidateNotNullOrEmpty()]
        [pscredential]
        $SourceCredentialObject,

        [Parameter(Mandatory = $false)]
        $ServerListTable

    )
  
    #Get all source SQL Servers along with ETL markers
    $AllSourceServers = Get-AllSourceServersToETL -TargetServerInstance $TargetServerInstance `
        -TargetDatabase $TargetDatabase `
        -TargetCredentialObject $TargetCredentialObject `
        -TargetAzureDBCertificateAuth $TargetAzureDBCertificateAuth `
        -ServerListTable $ServerListTable

    foreach ($SourceServer in $AllSourceServers) {

        #Get backup history data
        $AllHistory = @(Get-BackupHistoryToETL -ServerInstance $SourceServer.ServerName -LastETLDateTime $SourceServer.LastETLDatetime -CredentialObject $SourceCredentialObject)

        #Get max datetime
        if ($AllHistory.Count -gt 0) {

            $MaxDateTimeObj = $AllHistory | Measure-Object -Property backup_finish_date -Maximum | Select-Object -Property Maximum
            [datetime]$MaxETLDateTime = $MaxDateTimeObj.Maximum
        

            Write-output "ETLing $($AllHistory.Count) records for $($SourceServer.ServerName)"

            Add-BackupHistoryToTarget -TargetServerInstance $TargetServerInstance `
                -TargetDatabase $TargetDatabase `
                -TargetCredentialObject $TargetCredentialObject `
                -TargetAzureDBCertificateAuth $TargetAzureDBCertificateAuth `
                -HistoryRecordsObject $AllHistory


            Update-LastETLDateTimeForServer -TargetServerInstance $TargetServerInstance `
                -TargetDatabase $TargetDatabase `
                -TargetCredentialObject $TargetCredentialObject `
                -TargetAzureDBCertificateAuth $TargetAzureDBCertificateAuth `
                -SourceServerToUpdate $SourceServer.ServerName `
                -MaxETLDateTime $MaxETLDateTime `
    
        }

        else {
            Write-Output "No backup history records found for $($SourceServer.ServerName)"
        }
        
    }
    
    
}