Function Lock-WhoIsActiveLock {

    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
    )
    
    process {
           
        $date = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
        
        $query = "update [WHOISACTIVE_AppLock] set [WIA_Running] = '-1',Lock_Acquired = '$date'"  
        
        Invoke-Sqlcmd2 @SqlCredHash -Query $query             
    }

}
