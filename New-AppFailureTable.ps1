Function New-AppFailureTable {

    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
  
    )
    begin {
    
        $Query = @"
        CREATE TABLE [dbo].[APP_FAILURE](
            [RECORD_NUMBER] [int] IDENTITY(1,1) NOT NULL,
            [FAILURE_TIME] [datetime] NOT NULL,
            PRIMARY KEY CLUSTERED 
            (
            [RECORD_NUMBER] ASC
            )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
        ) ON [PRIMARY]
"@

    }
    process {
   
      
         Invoke-Sqlcmd2 @SqlCredHash -Query $query

    } 

}
