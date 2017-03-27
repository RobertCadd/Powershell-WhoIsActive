
    [CmdletBinding()]
    Param
    (       [Parameter(Mandatory=$true,
                   ValueFromPipelineByPropertyName=$true,
                   Position=0)]
            [hashtable] $SqlCredHash

    )

    Begin
    { 
        $query = "exec sp_whoisactive"
    }
    Process
    {
           
        $Result = Invoke-Sqlcmd2 @SqlCredHash -Query $query -As PSObject
                
    }
    End
    {
    
        return $Result
        
    }

