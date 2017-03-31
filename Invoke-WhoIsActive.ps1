Function Invoke-WhoIsActive {

    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]     
        [hashtable] 
        $SqlCredHash,
        
        [ValidateScript({ ($_ -gt 0) -or ($_ = $null) })]
        [Parameter()]    
        [int] 
        $Minutes = 1

    )

    process {

        $timeRange = 1..$Minutes
        
        foreach($minute in $timeRange){

            Run-WhoIsActive -SqlCredHash $SqlCredHash

        }

    }

}
