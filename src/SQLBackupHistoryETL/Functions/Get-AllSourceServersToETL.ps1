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
        $CredentialObject

    )
  
    $query = @"
            
    select  sbhss.ServerName
            ,sbhss.LastETLDatetime
    from    Utility.SQLBackupHistorySourceServers as sbhss;
        
"@

    try {

        if ($CredentialObject) {
            $SourceServers = Invoke-Sqlcmd -ServerInstance $TargetServerInstance -query $query -Database $TargetDatabase -Credential $CredentialObject -ErrorAction Stop
        }

        else {
            $SourceServers = Invoke-Sqlcmd -ServerInstance $TargetServerInstance -query $query -Database $TargetDatabase -ErrorAction Stop
        }

        return $SourceServers
        
    }
    
    catch {
        Write-Error "Failed to retrieve servers to ETL from Server: $ServerInstance"
        Write-Error "Error Message: $_.Exception.Message"
        exit
    }

    
    
}