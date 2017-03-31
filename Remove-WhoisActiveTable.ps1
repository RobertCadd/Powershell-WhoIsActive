Function Remove-WhoisActiveTable {
[CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
  
    )
    process {
   
        $query = "DROP TABLE WHOISACTIVE"
        
        Invoke-Sqlcmd2 @SqlCredHash -Query $query

    }
}
