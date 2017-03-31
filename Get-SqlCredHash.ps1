Function Get-SqlCredHash {
[cmdletbinding()]
    Param 
    (            
        [Parameter()]
        [string] 
        $Connection,
        
        [Parameter()]
        [string] 
        $File 
    )

    begin {
            
        $credObject = @{
            Credential = ''
            ServerInstance = ''
            Database = ''
        }
    }

    process {
                          
        if($Connection) {
                 
            $builder = New-Object System.Data.SqlClient.SqlConnectionStringBuilder($Connection)

            $secpasswd = ConvertTo-SecureString $builder.Password -AsPlainText -Force
                          
            $credObject.Credential = New-Object System.Management.Automation.PSCredential ($builder.UserID, $secpasswd)
    
            $credObject.ServerInstance = $builder.DataSource

            $credObject.Database =  $builder.InitialCatalog
        }
        elseif(test-path $file) { 
                                  
            Write-Host "using $file"
                    
            $bootini  = Get-IniContent -FilePath $file
            
            $connStringElement  = $bootini["boot"]["connection string"]
                   
            $builder = New-Object System.Data.SqlClient.SqlConnectionStringBuilder($connStringElement)

            $secpasswd = ConvertTo-SecureString $builder.Password -AsPlainText -Force
            
            $credObject.Credential = New-Object System.Management.Automation.PSCredential ($builder.UserID, $secpasswd)
    
            $credObject.ServerInstance = $builder.DataSource

            $credObject.Database =  $builder.InitialCatalog      
        }                       
        else { 
                       
            write-host "Get-SqlCredHash did not build parameters"
        }          
    }
                       
    end {
        
        return $CredObject   
    }
}
