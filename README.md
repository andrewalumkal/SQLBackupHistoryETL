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

Invoke-SQLBackupHistoryETL -TargetServerInstance $TargetServerInstance `
                            -TargetDatabase $TargetDatabase `
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


Invoke-SQLBackupHistoryETL -TargetServerInstance $TargetServerInstance `
                            -TargetDatabase $TargetDatabase `
                            -TargetCredentialObject $targetcredObject `
                            -SourceCredentialObject $sourcecredObject
```


#### ETL all backup history from source servers to target server (using integrated authentication on source and target)

```powershell
$TargetServerInstance = "myazuredbserver.database.windows.net"
$TargetDatabase = "DBADatabase"

Invoke-SQLBackupHistoryETL -TargetServerInstance $TargetServerInstance `
                            -TargetDatabase $TargetDatabase
```

## Generate restore script

Restore scripts can be generated directly from the central server using a stored procedure. Supports Full, Diff, Log restores to point in time along with options to move SQL files to different drive locations.

#### Use @Help for instructions
```sql
exec Utility.GenerateRestoreScript @Help = 1
```

#### Example - Standalone Database
```sql
exec Utility.GenerateRestoreScript @SourceDB = 'DBAdmin'
                                  ,@SourceDBServer = 'SQLNODE5236'
```
#### Example - AG Database
```sql
exec Utility.GenerateRestoreScript @SourceDB = 'DBAdminAG1'
                                  ,@SourceAGName = 'AG1'
```
#### Example 2 - AG Database
```sql
exec Utility.GenerateRestoreScript @SourceDB = 'DBAdminAG1'
                                  ,@DestinationDB = 'DBAdminAG1_Restored'
                                  ,@SourceAGName = 'AG1'
                                  ,@RestoreToTime = '2022-07-19 19:30:27'
                                  ,@RestoreDataPath = 'X\MSSQL\DATA'
                                  ,@RestoreLogPath = 'Y\MSSQL\Log'
                                  ,@FileNamePrefix = 'zRestored_'
```
