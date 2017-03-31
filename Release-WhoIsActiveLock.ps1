Function Release-WhoIsActiveLock {
[CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
    )
    
    process {
           
        $query = "update [WHOISACTIVE_AppLock] set [WIA_Running] = '0'"
        
        Invoke-Sqlcmd2 @SqlCredHash -Query $query             
    }
}
