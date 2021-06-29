Function Add-BackupHistoryToTarget {
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
        $HistoryRecordsObject

    )
  
    
    try {

        #Create an open SQL Connection based on inputs

        #Passing in certificate info to connect to Azure SQL DB
        if ($TargetAzureDBCertificateAuth) {

            #This returns an open connection
            $conn = New-AzureSQLDbConnectionWithCert -AzureSQLDBServerName $TargetServerInstance `
                -DatabaseName $TargetDatabase `
                -TenantID $TargetAzureDBCertificateAuth.TenantID `
                -ClientID $TargetAzureDBCertificateAuth.ClientID `
                -FullCertificatePath $TargetAzureDBCertificateAuth.FullCertificatePath

        }
        
        #Pass in regular credential object
        elseif ($TargetCredentialObject) {

            #First convert creds to a sql credential
            $TargetCredentialObject.Password.MakeReadOnly()
            $sqlCred = New-Object System.Data.SqlClient.SqlCredential($TargetCredentialObject.username, $TargetCredentialObject.password)

            #Create connection
            $conn = New-Object System.Data.SqlClient.SQLConnection 
            $conn.ConnectionString = "Data Source=$TargetServerInstance;Initial Catalog=$TargetDatabase;"
            $conn.Credential = $sqlCred
            $conn.Open()
            
        }

        #Using integrated security
        else {

            $conn = New-Object System.Data.SqlClient.SQLConnection 
            $conn.ConnectionString = "Data Source=$TargetServerInstance;Initial Catalog=$TargetDatabase;Integrated Security=true;"
            $conn.Open()
            
        }



        #Loop through object and insert one by one using the same connection
        foreach ($HistoryRecord in $HistoryRecordsObject) {

            $query = @"
            
                    exec Utility.InsertSQLBackupHistory @database_name = N'$($HistoryRecord.database_name)'                        
                                                ,@BackupType = '$($HistoryRecord.BackupType)'                            
                                                ,@physical_device_name = N'$($HistoryRecord.physical_device_name)'                 
                                                ,@backup_start_date = '$($HistoryRecord.backup_start_date)'  
                                                ,@backup_finish_date = '$($HistoryRecord.backup_finish_date)' 
                                                ,@server_name = N'$($HistoryRecord.server_name)'
                                                ,@ag_name = N'$($HistoryRecord.ag_name)'                          
                                                ,@recovery_model = '$($HistoryRecord.recovery_model)'                        
                                                ,@first_lsn = $($HistoryRecord.first_lsn)                           
                                                ,@last_lsn = $($HistoryRecord.last_lsn)                            
                                                ,@UncompressedSizeMB = $($HistoryRecord.UncompressedSizeMB)                     
                                                ,@CompressedSizeMB = $($HistoryRecord.CompressedSizeMB)
                                                ,@is_copy_only = $($HistoryRecord.is_copy_only)
                                                ,@encryptor_type = N'$($HistoryRecord.encryptor_type)'
                                                ,@key_algorithm = N'$($HistoryRecord.key_algorithm)'; 
        
"@


            Invoke-Sqlcmd2 -SQLConnection $conn -query $query -ErrorAction Stop


        }

        $conn.Close()
        
    }
    
    catch {
        Write-Error "Failed to write backup history record to target server: $ServerInstance"
        Write-Error "Error Message: $_.Exception.Message"

        if ($conn) {
            $conn.Close()
        }

        exit
    }

    
    
}