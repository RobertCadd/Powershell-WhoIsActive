
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

              
                CREATE TABLE [dbo].[WHOISACTIVE_AppLock](
	                [WIA_Running] [int] NULL,
	                [Lock_Acquired] [datetime] NULL
                ) ON [PRIMARY]

                INSERT INTO [dbo].[WHOISACTIVE_AppLock] ([WIA_Running])  VALUES (0)

"@


    }
    Process
    {
      
        Invoke-Sqlcmd2 @SqlCredHash -Query $query

    } 
     

