# SQLBackupHistoryETL
ETL SQL backup history from multiple sql servers to a central server. Supports SQL Server and Azure SQL DB as central backup history server.

Requires `SqlServer` and `Invoke-SqlCmd2` modules. Optionally will also require `AzureRM.profile` if using an Azure SQL DB as central server with certificate authentication.  

## Prerequisites
Create all SQL objects on target database of your choice. Script located in `.\src\SQLBackupHistoryETL\SQLObjects`

Populate Utility.SQLBackupHistorySourceServers with all source servers to get backup history from

All consolidated backup history lives in Utility.SQLBackupHistoryConsolidated

## Example Usage

### Import modules

```powershell
Import-Module SqlServer -Force
Import-Module .\src\SQLBackupHistoryETL -force
Import-Module Invoke-SqlCmd2 -force
```
#### ETL all backup history from source servers to target Azure SQL DB (using certificate)

```powershell
$TargetServerInstance = "myazuredbserver.database.windows.net"
$TargetDatabase = "DBADatabase"
$TargetAzureDBCertificateAuth = @{TenantID = <AzureTenantIDHere>; ClientID = <AzureClientIDHere>; FullCertificatePath = "Cert:\LocalMachine\My\<CertificateThumbprintHere>"}

Invoke-SQLBackupHistoryETL -TargetServerInstance $ServerInstance `
                            -TargetDatabase "DBAdmin" `
                            -TargetAzureDBCertificateAuth $TargetAzureDBCertificateAuth
```

#### ETL all backup history from source servers to target server (using credentials for both source and target servers)

```powershell
$TargetServerInstance = "myazuredbserver.database.windows.net"
$TargetDatabase = "DBADatabase"

[string]$sourceuserName = 'sourceuser'
[string]$sourceuserPassword = 'sourceuserpass'
[securestring]$sourcesecStringPassword = ConvertTo-SecureString $sourceuserPassword -AsPlainText -Force
[pscredential]$sourcecredObject = New-Object System.Management.Automation.PSCredential ($sourceuserName, $sourcesecStringPassword)

[string]$targetuserName = 'targetuser'
[string]$targetuserPassword = 'targetuserpass'
[securestring]$targetsecStringPassword = ConvertTo-SecureString $targetuserPassword -AsPlainText -Force
[pscredential]$targetcredObject = New-Object System.Management.Automation.PSCredential ($targetuserName, $targetsecStringPassword)


Invoke-SQLBackupHistoryETL -TargetServerInstance $ServerInstance `
                            -TargetDatabase "DBAdmin" `
                            -TargetCredentialObject $targetcredObject `
                            -SourceCredentialObject $sourcecredObject
```


#### ETL all backup history from source servers to target server (using integrated authentication on source and target)

```powershell
$TargetServerInstance = "myazuredbserver.database.windows.net"
$TargetDatabase = "DBADatabase"

Invoke-SQLBackupHistoryETL -TargetServerInstance $ServerInstance `
                            -TargetDatabase "DBAdmin"
```

