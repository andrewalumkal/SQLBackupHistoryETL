Function Update-LastETLDateTimeForServer {
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
        $TargetAzureDBCertificateAuth,

        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        $SourceServerToUpdate,

        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        [Datetime]
        $MaxETLDateTime

    )
  
    $query = @"
            
    update  sbhss
    set     sbhss.LastETLDatetime = '$($MaxETLDateTime)'
    from    Utility.SQLBackupHistorySourceServers as sbhss
    where   sbhss.ServerName = '$($SourceServerToUpdate)';
        
"@

    try {

        if ($TargetAzureDBCertificateAuth) {
            $conn = New-AzureSQLDbConnectionWithCert -AzureSQLDBServerName $TargetServerInstance `
                -DatabaseName $TargetDatabase `
                -TenantID $TargetAzureDBCertificateAuth.TenantID `
                -ClientID $TargetAzureDBCertificateAuth.ClientID `
                -FullCertificatePath $TargetAzureDBCertificateAuth.FullCertificatePath

            #Using Invoke-Sqlcmd2 to be able to pass in an existing connection
            Invoke-Sqlcmd2 -SQLConnection $conn -query $query -ErrorAction Stop
            $conn.Close()
        }
        
        elseif ($TargetCredentialObject) {
            Invoke-Sqlcmd -ServerInstance $TargetServerInstance -query $query -Database $TargetDatabase -Credential $TargetCredentialObject -ErrorAction Stop
        }

        else {
            Invoke-Sqlcmd -ServerInstance $TargetServerInstance -query $query -Database $TargetDatabase -ErrorAction Stop
        }

    }
    
    catch {
        Write-Error "Failed to update max ETLDateTime for SourceServer: $SourceServerToUpdate on target Server: $ServerInstance"
        Write-Error "Error Message: $_.Exception.Message"

        if($conn){
            $conn.Close()
        }

        exit
    }

    
    
}