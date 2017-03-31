# **Powershell**
This  will run sp_whoisactive once every 5 seconds for a minute then log the results to a table
# Instructions
Create the tables and sp_whoisactive:
```powershell 

Import-Module "C:\Temp\WhoIsActive.psm1"

$secpasswd = ConvertTo-SecureString "Foo" -AsPlainText -Force                        

$Credential = New-Object System.Management.Automation.PSCredential ("FooUser", $secpasswd)

$SqlCredHash = @{"Credential" = $Credential;"ServerInstance" = "MsSql";"Database" = "Foo"}

Setup-WhoIsActive -SqlCredHash $SqlCredHash 
```
Run it:
```powershell 

Invoke-WhoIsActive -SqlCredHash $SqlCredHash 

```

You can specify the number of minutes to run :
```powershell 

Invoke-WhoIsActive -minutes 2 -SqlCredHash $SqlCredHash

```