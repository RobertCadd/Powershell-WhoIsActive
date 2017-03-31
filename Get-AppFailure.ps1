Function Get-AppFailure {
[CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash,

        [Parameter(Mandatory=$true)]
        [Datetime] 
        $Date
    )
    
    process {
    
        $query = "select *  from [dbo].[APP_FAILURE] where failure_time ='$Date'"  
        
        $result = Invoke-Sqlcmd2 @SqlCredHash -Query $query

        return  $result             
    }
}
