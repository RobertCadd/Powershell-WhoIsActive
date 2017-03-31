Function Get-WhoIsActive {
[CmdletBinding()]
    Param
    (             
        [Parameter(Mandatory=$true)]     
        [hashtable] 
        $SqlCredHash
    )

    process {
           
        $query = "exec sp_whoisactive"

        $result = Invoke-Sqlcmd2 @SqlCredHash -Query $query -As PSObject

        return $result                
    }
}
