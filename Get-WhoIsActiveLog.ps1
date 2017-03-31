Function Get-WhoIsActiveLog {


    [CmdletBinding()]
    Param
    (             
        [Parameter(Mandatory=$true)]     
        [hashtable] 
        $SqlCredHash,

        [Parameter()]     
        [switch] 
        $desc
    )

    process {
                
        $query = "select * from WHOISACTIVE order by record_number,collection_time"

        if($desc){ $query = "select * from WHOISACTIVE order by record_number,collection_time desc" }

        $result = Invoke-Sqlcmd2 @SqlCredHash -Query $query 

        return $result                
    }
}
