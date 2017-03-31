Function New-WhoIsActiveAppLockTable {

    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
  
    )
    begin
    {
        $Query = @"              
        CREATE TABLE [dbo].[WHOISACTIVE_AppLock](
	        [WIA_Running] [int] NULL,
	        [Lock_Acquired] [datetime] NULL
        ) ON [PRIMARY]

        INSERT INTO [dbo].[WHOISACTIVE_AppLock] ([WIA_Running])  VALUES (0)

"@

    }

    process {
       
        Invoke-Sqlcmd2 @SqlCredHash -Query $query

    }     
}
