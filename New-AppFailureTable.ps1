Function New-AppFailureTable {

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
    Process
    {
      
         Invoke-Sqlcmd2 @SqlCredHash -Query $query

    } 
    #} 

}
