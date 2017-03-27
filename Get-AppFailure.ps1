Function Get-AppFailure {

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
        $query = "select *  from [dbo].[APP_FAILURE] where failure_time ='$date'"
          
    }
    Process
    {
          
        $Result = Invoke-Sqlcmd2 @SqlCredHash -Query $query
              
    }
    End
    {   
         return  $Result  
    }

}
