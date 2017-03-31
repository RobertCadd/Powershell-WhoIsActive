Function Remove-AppFailureTable {
[CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
  
    )
    process {
   
        $query = "DROP TABLE APP_FAILURE"
        
        Invoke-Sqlcmd2 @SqlCredHash -Query $query

    }
}
