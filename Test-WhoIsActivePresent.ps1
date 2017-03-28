Function Test-WhoIsActivePresent {

    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
  
    )
    begin
    {
        $Query = @"              
        SELECT 1  FROM sys.procedures WHERE Name = 'sp_WhoIsActive' 
"@

    }

    process {
       
        $result = Invoke-Sqlcmd2 -ServerInstance $SqlCredHash.ServerInstance -Database Master -Credential $SqlCredHash.Credential  -Query $query

        return $result

    }
      

}
