# **Powershell**
This  will run sp_whoisactive once every 5 seconds for a minute then log the results to a table
# Instructions
Create the tables:
```powershell 

Import-Module "C:\Temp\WhoIsActive.psm1"

$secpasswd = ConvertTo-SecureString "Foo" -AsPlainText -Force                        

$Credential = New-Object System.Management.Automation.PSCredential ("FooUser", $secpasswd)

$SqlCredHash = @{"Credential" = $Credential;"ServerInstance" = "MsSql";"Database" = "Foo"}

Create-WhoisActiveTables -SqlCredHash $SqlCredHash 
```
Run it:
```powershell 

Run-WhoIsActive -SqlCredHash $SqlCredHash 

```