Function Setup-WhoisActive {

    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
    )

    process {
        
        if(-Not(Test-WhoIsActivePresent -SqlCredHash $SqlCredHash)) { Install-WhoIsActive -SqlCredHash $SqlCredHash }

        New-AppFailureTable -SqlCredHash $SqlCredHash

        New-WhoIsActiveTable -SqlCredHash $SqlCredHash

        New-WhoIsActiveAppLockTable -SqlCredHash $SqlCredHash
              
    }

}
