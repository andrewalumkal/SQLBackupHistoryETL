Function Get-AllSourceServersToETL {
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
        $TargetAzureDBCertificateAuth

    )
  
    $query = @"
            
    select  sbhss.ServerName
            ,sbhss.LastETLDatetime
    from    Utility.SQLBackupHistorySourceServers as sbhss;
        
"@

    try {

        if ($TargetAzureDBCertificateAuth) {
            $conn = New-AzureSQLDbConnectionWithCert -AzureSQLDBServerName $TargetServerInstance `
                -DatabaseName $TargetDatabase `
                -TenantID $TargetAzureDBCertificateAuth.TenantID `
                -ClientID $TargetAzureDBCertificateAuth.ClientID `
                -FullCertificatePath $TargetAzureDBCertificateAuth.FullCertificatePath

            #Using Invoke-Sqlcmd2 to be able to pass in an existing connection
            $SourceServers = Invoke-Sqlcmd2 -SQLConnection $conn -query $query -ErrorAction Stop
            $conn.Close()
        }

        elseif ($TargetCredentialObject) {
            $SourceServers = Invoke-Sqlcmd -ServerInstance $TargetServerInstance -query $query -Database $TargetDatabase -Credential $TargetCredentialObject -ErrorAction Stop
        }

        else {
            $SourceServers = Invoke-Sqlcmd -ServerInstance $TargetServerInstance -query $query -Database $TargetDatabase -ErrorAction Stop
        }

        return $SourceServers
        
    }
    
    catch {
        Write-Error "Failed to retrieve servers to ETL from Server: $ServerInstance"
        Write-Error "Error Message: $_.Exception.Message"

        if($conn){
            $conn.Close()
        }

        exit
    }

    
    
}