Function Update-LastETLDateTimeForServer {
    [cmdletbinding()]
    Param(
        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        $TargetServerInstance,

        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        $TargetDatabase,

        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        $SourceServerToUpdate,

        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        [Datetime]
        $MaxETLDateTime,

        [Parameter(Mandatory = $false)]
        [pscredential]
        $CredentialObject

    )
  
    $query = @"
            
    update  sbhss
    set     sbhss.LastETLDatetime = '$($MaxETLDateTime)'
    from    Utility.SQLBackupHistorySourceServers as sbhss
    where   sbhss.ServerName = '$($SourceServerToUpdate)';
        
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
        Write-Error "Failed to update max ETLDateTime for SourceServer: $SourceServerToUpdate on target Server: $ServerInstance"
        Write-Error "Error Message: $_.Exception.Message"
        exit
    }

    
    
}