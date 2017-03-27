Function New-WhoIsActiveTable {

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

                CREATE TABLE [dbo].[WHOISACTIVE](
	                [Record_number] [int] NULL,
	                [FAILURE_TIME] [datetime] NOT NULL,
	                [dd hh:mm:ss.mss] [varchar](8000) NULL,
	                [session_id] [smallint] NOT NULL,
	                [sql_text] [xml] NULL,
	                [login_name] [nvarchar](128) NOT NULL,
	                [wait_info] [nvarchar](4000) NULL,
	                [tran_log_writes] [nvarchar](4000) NULL,
	                [CPU] [varchar](30) NULL,
	                [tempdb_allocations] [varchar](30) NULL,
	                [tempdb_current] [varchar](30) NULL,
	                [blocking_session_id] [smallint] NULL,
	                [reads] [varchar](30) NULL,
	                [writes] [varchar](30) NULL,
	                [physical_reads] [varchar](30) NULL,
	                [query_plan] [xml] NULL,
	                [used_memory] [varchar](30) NULL,
	                [status] [varchar](30) NOT NULL,
	                [tran_start_time] [datetime] NULL,
	                [open_tran_count] [varchar](30) NULL,
	                [percent_complete] [varchar](30) NULL,
	                [host_name] [nvarchar](128) NULL,
	                [database_name] [nvarchar](128) NULL,
	                [program_name] [nvarchar](128) NULL,
	                [start_time] [datetime] NOT NULL,
	                [login_time] [datetime] NULL,
	                [request_id] [int] NULL,
	                [collection_time] [datetime] NOT NULL,
	
                ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

"@

    }
    Process
    {
      
        Invoke-Sqlcmd2 @SqlCredHash -Query $query

    } 
    #} 

}