Function Remove-AllWhoIsActiveTables {


    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
  
    )
    process {
   
        Remove-AppFailureTable -SqlCredHash $SqlCredHash

        Remove-WhoisActiveTable -SqlCredHash $SqlCredHash

        Remove-WhoisActiveAppLockTable -SqlCredHash $SqlCredHash

    }
    

}
