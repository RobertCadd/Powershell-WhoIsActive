Function Get-WhoIsActiveLock {

    [CmdletBinding()]
    Param
    (   
        [Parameter(Mandatory=$true)]     
        [hashtable] 
        $SqlCredHash
    )
    
    process {
          
        $query = "SELECT [WIA_Running] FROM [WHOISACTIVE_AppLock]"

        $result = Invoke-Sqlcmd2 @SqlCredHash -Query $query

        return $result      
    }

}
