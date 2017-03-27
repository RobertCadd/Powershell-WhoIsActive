
    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true,
                   ValueFromPipelineByPropertyName=$true,
                   Position=0)]
         [hashtable] $SqlCredHash
        )

    Begin
    { 
          
    }
    Process
    {
          
        New-AppFailureTable -SqlCredHash $SqlCredHash

        New-WhoIsActiveTable -SqlCredHash $SqlCredHash

        New-WhoIsActiveAppLockTable -SqlCredHash $SqlCredHash
              
    }
    End
    {   
         
    }

