
    [CmdletBinding()]
    Param
    (       [Parameter(Mandatory=$true,
                   ValueFromPipelineByPropertyName=$true,
                   Position=0)]
        [hashtable] $SqlCredHash

    )

    Begin
    { 
        $query = "SELECT [WIA_Running] FROM [WHOISACTIVE_AppLock]"

          
    }
    Process
    {
         
        $Result = Invoke-Sqlcmd2 @SqlCredHash -Query $query
      
    }
    End
    {
    
        return $Result
        
    }

