# **Powershell**
This  will run sp_whoisactive once every 5 seconds for a minute then log the results to a table
# Instructions
Create the tables and sp_whoisactive:
```powershell 

# One time setup
    # Download the repository
    # Unblock the zip
    # Extract the  WhoIsActive folder to a module path (e.g. $env:USERPROFILE\Documents\WindowsPowerShell\Modules\)

import-module WhoIsActive

$secpasswd = ConvertTo-SecureString "FooPw" -AsPlainText -Force                        

$Credential = New-Object System.Management.Automation.PSCredential ("FooUser", $secpasswd)

$SqlCredHash = @{"Credential" = $Credential;"ServerInstance" = "FooServer";"Database" = "FooDb"}

Setup-WhoIsActive -SqlCredHash $SqlCredHash 
```
Run it:
```powershell 

Invoke-WhoIsActive -SqlCredHash $SqlCredHash 

```

To specify the number of minutes to run :
```powershell 

Invoke-WhoIsActive -Minutes 2 -SqlCredHash $SqlCredHash

```

To view the results :
```powershell 

Get-WhoIsActiveLog -SqlCredHash $SqlCredHash 

#out-grid allows for column filtering and reads better

Get-WhoIsActiveLog -SqlCredHash $SqlCredHash | Out-GridView

#use the -desc switch to sort results by most recent

Get-WhoIsActiveLog -SqlCredHash $SqlCredHash -desc | Out-GridView

#use the -WaitDuration switch to sort results by the [dd hh:mm:ss.mss] column

Get-WhoIsActiveLog -SqlCredHash $SqlCredHash -WaitDuration | Out-GridView

```