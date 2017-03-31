Function Remove-WhoisActiveAppLockTable {


    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
  
    )
    process {
   
        $query = "DROP TABLE WHOISACTIVE_AppLock"
        
        Invoke-Sqlcmd2 @SqlCredHash -Query $query

    }
    

}
