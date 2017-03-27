Function Run-WhoIsActive {

    [CmdletBinding()]
    Param
    (       [Parameter(Mandatory=$true,
                   ValueFromPipelineByPropertyName=$true,
                   Position=0)]
            [hashtable] $SqlCredHash

    )

    Begin
    { 
        
          
    }
    Process
    {
        $date = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")

        Log-AppFailure -SqlCredHash $SqlCredHash -date $date 
        
        $locked = Get-WhoIsActiveLock -SqlCredHash $SqlCredHash
        
        if($locked.WIA_Running -eq 0){
    
            Lock-WhoIsActiveLock -SqlCredHash $SqlCredHash 
    
            $WIAData = @()
    
            $Count = 0 

            DO
            {
                $Data =  Get-WhoIsActive -SqlCredHash $SqlCredHash
        
                $WIAData += $Data

                start-sleep -seconds 5

                $Count++

            } While ($Count -le 10)

            $recNum = (get-AppFailure -SqlCredHash $SqlCredHash -date $date).RECORD_NUMBER
     
            Log-WhoIsActive -SqlCredHash $SqlCredHash -dataObject $WIAData -date $date -recnum $recNum 

            Release-WhoIsActiveLock -SqlCredHash $SqlCredHash
        }
        else{
        
            Write-host "can not get lock for sp_whoisactive on $($SqlCredHash.ServerInstance), SP_Whoisacitve is already running"
            
        }        
    }
    End
    {
        
    }

}
