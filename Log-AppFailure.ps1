Function Log-AppFailure {

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
           
        $query = "INSERT INTO [dbo].[APP_FAILURE] ([FAILURE_TIME]) VALUES ('$Date')"  
        
        Invoke-Sqlcmd2 @SqlCredHash -Query $query             
    }

}
