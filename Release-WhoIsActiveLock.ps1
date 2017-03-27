Function Release-WhoIsActiveLock {

    [CmdletBinding()]
    Param
    (       [Parameter(Mandatory=$true,
                   ValueFromPipelineByPropertyName=$true,
                   Position=0)]
       [hashtable] $SqlCredHash

    )

    Begin
    { 
        $query = "update [WHOISACTIVE_AppLock] set [WIA_Running] = '0'"

    }
    Process
    {
          Invoke-Sqlcmd2 @SqlCredHash -Query $query
              
    }
    End
    {   
             
    }

}
