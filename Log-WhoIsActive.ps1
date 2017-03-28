Function Log-WhoIsActive {

    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash,
        [Parameter(Mandatory=$true)]
        [PSCustomObject] 
        $dataObject,
        [Parameter(Mandatory=$true)]
        [Datetime] 
        $date,
        [Parameter(Mandatory=$true)]
        [int] 
        $recnum
    )

    begin {
    
        if($dataObject.Count -eq 0){

            Return

        }                      
    }

    process { 
                       
        $queries = @() 

        foreach($datarow in $dataObject){

            if($datarow.sql_text.Contains("sp_server_diagnostics")) { continue }
                    
            $sql_textCleanup = $datarow.sql_text.Replace("'","''")
            			
			$queries += @"
			INSERT [dbo].[whoisactive] ([Record_number],[FAILURE_TIME], [dd hh:mm:ss.mss], [session_id], [sql_text], [login_name], [wait_info], [CPU], [tempdb_allocations], [tempdb_current], [blocking_session_id], [reads], [writes], [physical_reads], [used_memory], [status], [open_tran_count], [percent_complete], [host_name], [database_name], [program_name], [start_time], [login_time], [request_id], [collection_time]) VALUES ('$recnum','$date','$($datarow.'dd hh:mm:ss.mss')','$($datarow.session_id)','$sql_textCleanup','$($datarow.login_name)','$($datarow.wait_info)','$($datarow.CPU)','$($datarow.tempdb_allocations)','$($datarow.tempdb_current)','$($datarow.blocking_session_id)','$($datarow.reads)','$($datarow.writes)','$($datarow.physical_reads)','$($datarow.used_memory)','$($datarow.status)','$($datarow.open_tran_count)','$($datarow.percent_complete)','$($datarow.host_name)','$($datarow.database_name)','$($datarow.program_name)','$($datarow.start_time)','$($datarow.login_time)','$($datarow.request_id)','$($datarow.collection_time)')
"@
        }
        
        foreach($query in  $queries) {
        
            Invoke-Sqlcmd2 @SqlCredHash -Query $query 
        
        }
    }

}
