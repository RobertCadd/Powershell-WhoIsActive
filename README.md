# Powershell

This Module will run sp_whoisactive once every 5 seconds for a minute then log the results to a table:

import-module WhoIsActive.psm1

\$secpasswd = ConvertTo-SecureString "Foo" -AsPlainText -Force
                          
\$Credential = New-Object System.Management.Automation.PSCredential ("FooUser", $secpasswd)

\$SqlCredHash = @{"Credential" = \$Credential;"ServerInstance" = "MsSql";"Database" = "Foo"}

Run-WhoIsActive -SqlCredHash $SqlCredHash 

To Create the tables needed for logging:

Create-WhoisActiveTables -SqlCredHash $SqlCredHash 