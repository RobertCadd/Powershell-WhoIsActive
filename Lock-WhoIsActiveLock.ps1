
    [CmdletBinding()]
    Param
    (       [Parameter(Mandatory=$true,
                   ValueFromPipelineByPropertyName=$true,
                   Position=0)]
       [hashtable] $SqlCredHash

    )

    Begin
    { 
        $date = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
        $query = "update [WHOISACTIVE_AppLock] set [WIA_Running] = '-1',Lock_Acquired = '$date'"
          
    }
    Process
    {
          
        Invoke-Sqlcmd2 @SqlCredHash -Query $query
              
    }
    End
    {   
             
    }

