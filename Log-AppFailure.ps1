
    [CmdletBinding()]
    Param
    (       
        
        [Parameter(Mandatory=$true,
                   ValueFromPipelineByPropertyName=$true,
                   Position=0)]
        [hashtable] $SqlCredHash,
        [Parameter(Mandatory=$true,
            ValueFromPipelineByPropertyName=$true,
            Position=1)]
        [Datetime] $date
    
    )

    Begin
    { 
        $query = "INSERT INTO [dbo].[APP_FAILURE] ([FAILURE_TIME]) VALUES ('$date')"

    }
    Process
    {
          
        Invoke-Sqlcmd2 @SqlCredHash -Query $query
              
    }
    End
    {   
             
    }

