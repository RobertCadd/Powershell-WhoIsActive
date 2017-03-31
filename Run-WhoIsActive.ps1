Function Run-WhoIsActive {

    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]     
        [hashtable] 
        $SqlCredHash
    )

    process {
   
        $date = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")

        Log-AppFailure -SqlCredHash $SqlCredHash -Date $date 
        
        $locked = Get-WhoIsActiveLock -SqlCredHash $SqlCredHash
        
        if($locked.WIA_Running -eq 0) {
    
            Lock-WhoIsActiveLock -SqlCredHash $SqlCredHash 
    
            $whoIsActiveData = @()
    
            $count = 0 

            Do
            {
                $data =  Get-WhoIsActive -SqlCredHash $SqlCredHash
        
                $whoIsActiveData += $Data

                start-sleep -seconds 5

                $count++

            } 
            While ($Count -lt 12)

            $recordNumber = (get-AppFailure -SqlCredHash $SqlCredHash -date $date).RECORD_NUMBER
     
            Log-WhoIsActive -SqlCredHash $SqlCredHash -dataObject $whoIsActiveData -date $date -recnum $recordNumber 

            Release-WhoIsActiveLock -SqlCredHash $SqlCredHash
        }
        else {
        
            Write-host "can not get lock for sp_whoisactive on $($SqlCredHash.ServerInstance), sp_whoisactive is already running"
            
        }        
    }

}
