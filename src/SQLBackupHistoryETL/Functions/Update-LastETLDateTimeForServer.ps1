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
        $MaxETLDateTime,

        [Parameter(Mandatory = $false)]
        $ServerListTable

    )
    
    if ($ServerListTable) {
        $query = @"
            
    update  sbhss
    set     sbhss.LastETLDatetime = '$($MaxETLDateTime)'
    from    $($ServerListTable) as sbhss
    where   sbhss.ServerName = '$($SourceServerToUpdate)';
        
"@
    }

    else {
        $query = @"
            
    update  sbhss
    set     sbhss.LastETLDatetime = '$($MaxETLDateTime)'
    from    Utility.SQLBackupHistorySourceServers as sbhss
    where   sbhss.ServerName = '$($SourceServerToUpdate)';
        
"@
    }
    

    try {

        if ($TargetAzureDBCertificateAuth) {
            $conn = New-AzureSQLDbConnectionWithCert -AzureSQLDBServerName $TargetServerInstance `
                -DatabaseName $TargetDatabase `
                -TenantID $TargetAzureDBCertificateAuth.TenantID `
                -ClientID $TargetAzureDBCertificateAuth.ClientID `
                -CertificateThumbprint $TargetAzureDBCertificateAuth.CertificateThumbprint

            #Using Invoke-Sqlcmd2 to be able to pass in an existing connection
            Invoke-Sqlcmd2 -SQLConnection $conn -query $query -ErrorAction Stop
            $conn.Close()
        }
        
        elseif ($TargetCredentialObject) {
            Invoke-SqlCmd -TrustServerCertificate -ServerInstance $TargetServerInstance -query $query -Database $TargetDatabase -Credential $TargetCredentialObject -ErrorAction Stop
        }

        else {
            Invoke-SqlCmd -TrustServerCertificate -ServerInstance $TargetServerInstance -query $query -Database $TargetDatabase -ErrorAction Stop
        }

    }
    
    catch {
        Write-Error "Failed to update max ETLDateTime for SourceServer: $SourceServerToUpdate on target Server: $ServerInstance"
        Write-Error "Error Message: $_.Exception.Message"

        if ($conn) {
            $conn.Close()
        }

        exit
    }

    
    
}