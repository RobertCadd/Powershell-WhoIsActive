
    [cmdletbinding()]

    param (            
        [Parameter(
        ValueFromPipelineByPropertyName=$true,
        Position=0)]
        [string] $Connection,
        [Parameter(
        ValueFromPipelineByPropertyName=$true,
        Position=1)]
        [string] $file
  
        )

    Begin {  
       
         $CredObject = @{
            Credential = ''
            ServerInstance = ''
            Database = ''
        }

        $connStringElement =$null    
    }

    process {
                       
        if($Connection) {
           
            $connStringElement  = $Connection
                   
            $builder = New-Object System.Data.SqlClient.SqlConnectionStringBuilder($connStringElement)

            $secpasswd = ConvertTo-SecureString $builder.Password -AsPlainText -Force
                          
            $CredObject.Credential = New-Object System.Management.Automation.PSCredential ($builder.UserID, $secpasswd)
    
            $CredObject.ServerInstance = $builder.DataSource

            $CredObject.Database =  $builder.InitialCatalog

        }
        elseif(test-path $file) {
                           
            Write-Host "using $file"
                    
            $bootini  = Get-IniContent -FilePath $file
            
            $connStringElement  = $bootini["boot"]["connection string"]
                   
            $builder = New-Object System.Data.SqlClient.SqlConnectionStringBuilder($connStringElement)

            $secpasswd = ConvertTo-SecureString $builder.Password -AsPlainText -Force
            
            $CredObject.Credential = New-Object System.Management.Automation.PSCredential ($builder.UserID, $secpasswd)
    
            $CredObject.ServerInstance = $builder.DataSource

            $CredObject.Database =  $builder.InitialCatalog
       
        }                       
        else {
                
            write-host "Get-SqlCredHash did not build parameters"

        }
           
    }           
      
    end {
    
        return $CredObject
    
    }

