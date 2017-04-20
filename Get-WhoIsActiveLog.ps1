function Get-WhoIsActiveLog {
[CmdletBinding()]
    Param
    (             
        [Parameter(Mandatory=$true)]     
        [hashtable] 
        $SqlCredHash,

        [Parameter()]     
        [switch] 
        $desc,

        [Parameter()]     
        [switch] 
        $WaitDuration
    )

    process {
                
        $query = "select * from WHOISACTIVE order by record_number,collection_time"

        if($desc){ $query = "select * from WHOISACTIVE order by record_number desc,collection_time desc" }

        if($WaitDuration) { $query =  "select * from WHOISACTIVE order by [dd hh:mm:ss.mss] desc" }

        $result = Invoke-Sqlcmd2 @SqlCredHash -Query $query 

        return $result                
    }

}
