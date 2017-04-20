
function Invoke-Parallel 
{
    <#
    .SYNOPSIS
        function to control parallel processing using runspaces

    .DESCRIPTION
        function to control parallel processing using runspaces

            Note that each runspace will not have access to variables and commands loaded in your session or in other runspaces by default.  
            This behaviour can be changed with parameters.

    .PARAMETER ScriptFile
        File to run against all input objects.  Must include parameter to take in the input object, or use $args.  Optionally, include parameter to take in parameter.  Example: C:\script.ps1

    .PARAMETER ScriptBlock
        Scriptblock to run against all computers.

        You may use $Using:<Variable> language in PowerShell 3 and later.
        
            The parameter block is added for you, allowing behaviour similar to foreach-object:
                Refer to the input object as $_.
                Refer to the parameter parameter as $parameter

    .PARAMETER InputObject
        Run script against these specified objects.

    .PARAMETER Parameter
        This object is passed to every script block.  You can use it to pass information to the script block; for example, the path to a logging folder
        
            Reference this object as $parameter if using the scriptblock parameterset.

    .PARAMETER ImportVariables
        If specified, get user session variables and add them to the initial session state

    .PARAMETER ImportModules
        If specified, get loaded modules and pssnapins, add them to the initial session state

    .PARAMETER Throttle
        Maximum number of threads to run at a single time.

    .PARAMETER SleepTimer
        Milliseconds to sleep after checking for completed runspaces and in a few other spots.  I would not recommend dropping below 200 or increasing above 500

    .PARAMETER RunspaceTimeout
        Maximum time in seconds a single thread can run.  If execution of your code takes longer than this, it is disposed.  Default: 0 (seconds)

        WARNING:  Using this parameter requires that maxQueue be set to throttle (it will be by default) for accurate timing.  Details here:
        http://gallery.technet.microsoft.com/Run-Parallel-Parallel-377fd430

    .PARAMETER NoCloseOnTimeout
		Do not dispose of timed out tasks or attempt to close the runspace if threads have timed out. This will prevent the script from hanging in certain situations where threads become non-responsive, at the expense of leaking memory within the PowerShell host.

    .PARAMETER MaxQueue
        Maximum number of powershell instances to add to runspace pool.  If this is higher than $throttle, $timeout will be inaccurate
        
        If this is equal or less than throttle, there will be a performance impact

        The default value is $throttle times 3, if $runspaceTimeout is not specified
        The default value is $throttle, if $runspaceTimeout is specified

    .PARAMETER LogFile
        Path to a file where we can log results, including run time for each thread, whether it completes, completes with errors, or times out.

	.PARAMETER Quiet
		Disable progress bar.

    .EXAMPLE
        Each example uses Test-ForPacs.ps1 which includes the following code:
            param($computer)

            if(test-connection $computer -count 1 -quiet -BufferSize 16){
                $object = [pscustomobject] @{
                    Computer=$computer;
                    Available=1;
                    Kodak=$(
                        if((test-path "\\$computer\c$\users\public\desktop\Kodak Direct View Pacs.url") -or (test-path "\\$computer\c$\documents and settings\all users

        \desktop\Kodak Direct View Pacs.url") ){"1"}else{"0"}
                    )
                }
            }
            else{
                $object = [pscustomobject] @{
                    Computer=$computer;
                    Available=0;
                    Kodak="NA"
                }
            }

            $object

    .EXAMPLE
        Invoke-Parallel -scriptfile C:\public\Test-ForPacs.ps1 -inputobject $(get-content C:\pcs.txt) -runspaceTimeout 10 -throttle 10

            Pulls list of PCs from C:\pcs.txt,
            Runs Test-ForPacs against each
            If any query takes longer than 10 seconds, it is disposed
            Only run 10 threads at a time

    .EXAMPLE
        Invoke-Parallel -scriptfile C:\public\Test-ForPacs.ps1 -inputobject c-is-ts-91, c-is-ts-95

            Runs against c-is-ts-91, c-is-ts-95 (-computername)
            Runs Test-ForPacs against each

    .EXAMPLE
        $stuff = [pscustomobject] @{
            ContentFile = "windows\system32\drivers\etc\hosts"
            Logfile = "C:\temp\log.txt"
        }
    
        $computers | Invoke-Parallel -parameter $stuff {
            $contentFile = join-path "\\$_\c$" $parameter.contentfile
            Get-Content $contentFile |
                set-content $parameter.logfile
        }

        This example uses the parameter argument.  This parameter is a single object.  To pass multiple items into the script block, we create a custom object (using a PowerShell v3 language) with properties we want to pass in.

        Inside the script block, $parameter is used to reference this parameter object.  This example sets a content file, gets content from that file, and sets it to a predefined log file.

    .EXAMPLE
        $test = 5
        1..2 | Invoke-Parallel -ImportVariables {$_ * $test}

        Add variables from the current session to the session state.  Without -ImportVariables $Test would not be accessible

    .EXAMPLE
        $test = 5
        1..2 | Invoke-Parallel {$_ * $Using:test}

        Reference a variable from the current session with the $Using:<Variable> syntax.  Requires PowerShell 3 or later. Note that -ImportVariables parameter is no longer necessary.

    .functionALITY
        PowerShell Language

    .NOTES
        Credit to Boe Prox for the base runspace code and $Using implementation
            http://learn-powershell.net/2012/05/10/speedy-network-information-query-using-powershell/
            http://gallery.technet.microsoft.com/scriptcenter/Speedy-Network-Information-5b1406fb#content
            https://github.com/proxb/PoshRSJob/

        Credit to T Bryce Yehl for the Quiet and NoCloseOnTimeout implementations

        Credit to Sergei Vorobev for the many ideas and contributions that have improved functionality, reliability, and ease of use

    .LINK
        https://github.com/RamblingCookieMonster/Invoke-Parallel
    #>
    [cmdletbinding(DefaultParameterSetName='ScriptBlock')]
    Param (   
        [Parameter(Mandatory=$false,position=0,ParameterSetName='ScriptBlock')]
            [System.Management.Automation.ScriptBlock]$ScriptBlock,

        [Parameter(Mandatory=$false,ParameterSetName='ScriptFile')]
        [ValidateScript({test-path $_ -pathtype leaf})]
            $ScriptFile,

        [Parameter(Mandatory=$true,ValueFromPipeline=$true)]
        [Alias('CN','__Server','IPAddress','Server','ComputerName')]    
            [PSObject]$InputObject,

            [PSObject]$Parameter,

            [switch]$ImportVariables,

            [switch]$ImportModules,

            [int]$Throttle = 20,

            [int]$SleepTimer = 200,

            [int]$RunspaceTimeout = 0,

			[switch]$NoCloseOnTimeout = $false,

            [int]$MaxQueue,

        [validatescript({Test-Path (Split-Path $_ -parent)})]
            [string]$LogFile = "C:\temp\log.log",

			[switch] $Quiet = $false
    )
    
    begin {
                
        #No max queue specified?  Estimate one.
        #We use the script scope to resolve an odd PowerShell 2 issue where MaxQueue isn't seen later in the function
        if( -not $PSBoundParameters.ContainsKey('MaxQueue') )
        {
            if($RunspaceTimeout -ne 0){ $script:MaxQueue = $Throttle }
            else{ $script:MaxQueue = $Throttle * 3 }
        }
        else
        {
            $script:MaxQueue = $MaxQueue
        }

        Write-Verbose "Throttle: '$throttle' SleepTimer '$sleepTimer' runSpaceTimeout '$runspaceTimeout' maxQueue '$maxQueue' logFile '$logFile'"

        #If they want to import variables or modules, create a clean runspace, get loaded items, use those to exclude items
        if ($ImportVariables -or $ImportModules)
        {
            $StandardUserEnv = [powershell]::Create().addscript({

                #Get modules and snapins in this clean runspace
                $Modules = Get-Module | Select -ExpandProperty Name
                $Snapins = Get-PSSnapin | Select -ExpandProperty Name

                #Get variables in this clean runspace
                #Called last to get vars like $? into session
                $Variables = Get-Variable | Select -ExpandProperty Name
                
                #Return a hashtable where we can access each.
                @{
                    Variables = $Variables
                    Modules = $Modules
                    Snapins = $Snapins
                }
            }).invoke()[0]
            
            if ($ImportVariables) {
                #Exclude common parameters, bound parameters, and automatic variables
                function _temp {[cmdletbinding()] param() }
                $VariablesToExclude = @( (Get-Command _temp | Select -ExpandProperty parameters).Keys + $PSBoundParameters.Keys + $StandardUserEnv.Variables )
                Write-Verbose "Excluding variables $( ($VariablesToExclude | sort ) -join ", ")"

                # we don't use 'Get-Variable -Exclude', because it uses regexps. 
                # One of the veriables that we pass is '$?'. 
                # There could be other variables with such problems.
                # Scope 2 required if we move to a real module
                $UserVariables = @( Get-Variable | Where { -not ($VariablesToExclude -contains $_.Name) } ) 
                Write-Verbose "Found variables to import: $( ($UserVariables | Select -expandproperty Name | Sort ) -join ", " | Out-String).`n"

            }

            if ($ImportModules) 
            {
                $UserModules = @( Get-Module | Where {$StandardUserEnv.Modules -notcontains $_.Name -and (Test-Path $_.Path -ErrorAction SilentlyContinue)} | Select -ExpandProperty Path )
                $UserSnapins = @( Get-PSSnapin | Select -ExpandProperty Name | Where {$StandardUserEnv.Snapins -notcontains $_ } ) 
            }
        }

        #region functions
            
            function Get-RunspaceData {
                [cmdletbinding()]
                param( [switch]$Wait )

                #loop through runspaces
                #if $wait is specified, keep looping until all complete
                Do {

                    #set more to false for tracking completion
                    $more = $false

                    #Progress bar if we have inputobject count (bound parameter)
                    if (-not $Quiet) {
						Write-Progress  -Activity "Running Query" -Status "Starting threads"`
							-CurrentOperation "$startedCount threads defined - $totalCount input objects - $script:completedCount input objects processed"`
							-PercentComplete $( Try { $script:completedCount / $totalCount * 100 } Catch {0} )
					}

                    #run through each runspace.           
                    Foreach($runspace in $runspaces) {
                    
                        #get the duration - inaccurate
                        $currentdate = Get-Date
                        $runtime = $currentdate - $runspace.startTime
                        $runMin = [math]::Round( $runtime.totalminutes ,2 )

                        #set up log object
                        $log = "" | select Date, Action, Runtime, Status, Details
                        $log.Action = "Removing:'$($runspace.object)'"
                        $log.Date = $currentdate
                        $log.Runtime = "$runMin minutes"

                        #If runspace completed, end invoke, dispose, recycle, counter++
                        If ($runspace.Runspace.isCompleted) {
                            
                            $script:completedCount++
                        
                            #check if there were errors
                            if($runspace.powershell.Streams.Error.Count -gt 0) {
                                
                                #set the logging info and move the file to completed
                                $log.status = "CompletedWithErrors"
                                Write-Verbose ($log | ConvertTo-Csv -Delimiter ";" -NoTypeInformation)[1]
                                foreach($ErrorRecord in $runspace.powershell.Streams.Error) {
                                    Write-Error -ErrorRecord $ErrorRecord
                                }
                            }
                            else {
                                
                                #add logging details and cleanup
                                $log.status = "Completed"
                                Write-Verbose ($log | ConvertTo-Csv -Delimiter ";" -NoTypeInformation)[1]
                            }

                            #everything is logged, clean up the runspace
                            $runspace.powershell.endInvoke($runspace.Runspace)
                            $runspace.powershell.dispose()
                            $runspace.Runspace = $null
                            $runspace.powershell = $null

                        }

                        #If runtime exceeds max, dispose the runspace
                        ElseIf ( $runspaceTimeout -ne 0 -and $runtime.totalseconds -gt $runspaceTimeout) {
                            
                            $script:completedCount++
                            $timedOutTasks = $true
                            
							#add logging details and cleanup
                            $log.status = "TimedOut"
                            Write-Verbose ($log | ConvertTo-Csv -Delimiter ";" -NoTypeInformation)[1]
                            Write-Error "Runspace timed out at $($runtime.totalseconds) seconds for the object:`n$($runspace.object | out-string)"

                            #Depending on how it hangs, we could still get stuck here as dispose calls a synchronous method on the powershell instance
                            if (!$noCloseOnTimeout) { $runspace.powershell.dispose() }
                            $runspace.Runspace = $null
                            $runspace.powershell = $null
                            $completedCount++

                        }
                   
                        #If runspace isn't null set more to true  
                        ElseIf ($runspace.Runspace -ne $null ) {
                            $log = $null
                            $more = $true
                        }

                        #log the results if a log file was indicated
                        if($logFile -and $log){
                            ($log | ConvertTo-Csv -Delimiter ";" -NoTypeInformation)[1] | out-file $LogFile -append
                        }
                    }

                    #Clean out unused runspace jobs
                    $temphash = $runspaces.clone()
                    $temphash | Where { $_.runspace -eq $Null } | ForEach {
                        $Runspaces.remove($_)
                    }

                    #sleep for a bit if we will loop again
                    if($PSBoundParameters['Wait']){ Start-Sleep -milliseconds $SleepTimer }

                #Loop again only if -wait parameter and there are more runspaces to process
                } while ($more -and $PSBoundParameters['Wait'])
                
            #end of runspace function
            }

        #endregion functions
        
        #region Init

            if($PSCmdlet.ParameterSetName -eq 'ScriptFile')
            {
                $ScriptBlock = [scriptblock]::Create( $(Get-Content $ScriptFile | out-string) )
            }
            elseif($PSCmdlet.ParameterSetName -eq 'ScriptBlock')
            {
                #Start building parameter names for the param block
                [string[]]$ParamsToAdd = '$_'
                if( $PSBoundParameters.ContainsKey('Parameter') )
                {
                    $ParamsToAdd += '$Parameter'
                }

                $UsingVariableData = $Null
                

                # This code enables $Using support through the AST.
                # This is entirely from  Boe Prox, and his https://github.com/proxb/PoshRSJob module; all credit to Boe!
                
                if($PSVersionTable.PSVersion.Major -gt 2)
                {
                    #Extract using references
                    $UsingVariables = $ScriptBlock.ast.FindAll({$args[0] -is [System.Management.Automation.Language.UsingExpressionAst]},$True)    

                    If ($UsingVariables)
                    {
                        $List = New-Object 'System.Collections.Generic.List`1[System.Management.Automation.Language.VariableExpressionAst]'
                        ForEach ($Ast in $UsingVariables)
                        {
                            [void]$list.Add($Ast.SubExpression)
                        }

                        $UsingVar = $UsingVariables | Group SubExpression | ForEach {$_.Group | Select -First 1}
        
                        #Extract the name, value, and create replacements for each
                        $UsingVariableData = ForEach ($Var in $UsingVar) {
                            Try
                            {
                                $Value = Get-Variable -Name $Var.SubExpression.VariablePath.UserPath -ErrorAction Stop
                                [pscustomobject]@{
                                    Name = $Var.SubExpression.Extent.Text
                                    Value = $Value.Value
                                    NewName = ('$__using_{0}' -f $Var.SubExpression.VariablePath.UserPath)
                                    NewVarName = ('__using_{0}' -f $Var.SubExpression.VariablePath.UserPath)
                                }
                            }
                            Catch
                            {
                                Write-Error "$($Var.SubExpression.Extent.Text) is not a valid Using: variable!"
                            }
                        }
                        $ParamsToAdd += $UsingVariableData | Select -ExpandProperty NewName -Unique

                        $NewParams = $UsingVariableData.NewName -join ', '
                        $Tuple = [Tuple]::Create($list, $NewParams)
                        $bindingFlags = [Reflection.BindingFlags]"Default,NonPublic,Instance"
                        $GetWithInputHandlingForInvokeCommandImpl = ($ScriptBlock.ast.gettype().GetMethod('GetWithInputHandlingForInvokeCommandImpl',$bindingFlags))
        
                        $StringScriptBlock = $GetWithInputHandlingForInvokeCommandImpl.Invoke($ScriptBlock.ast,@($Tuple))

                        $ScriptBlock = [scriptblock]::Create($StringScriptBlock)

                        Write-Verbose $StringScriptBlock
                    }
                }
                
                $ScriptBlock = $ExecutionContext.InvokeCommand.NewScriptBlock("param($($ParamsToAdd -Join ", "))`r`n" + $Scriptblock.ToString())
            }
            else
            {
                Throw "Must provide ScriptBlock or ScriptFile"; Break
            }

            Write-Debug "`$ScriptBlock: $($ScriptBlock | Out-String)"
            Write-Verbose "Creating runspace pool and session states"

            #If specified, add variables and modules/snapins to session state
            $sessionstate = [System.Management.Automation.Runspaces.InitialSessionState]::CreateDefault()
            if ($ImportVariables)
            {
                if($UserVariables.count -gt 0)
                {
                    foreach($Variable in $UserVariables)
                    {
                        $sessionstate.Variables.Add( (New-Object -TypeName System.Management.Automation.Runspaces.SessionStateVariableEntry -ArgumentList $Variable.Name, $Variable.Value, $null) )
                    }
                }
            }
            if ($ImportModules)
            {
                if($UserModules.count -gt 0)
                {
                    foreach($ModulePath in $UserModules)
                    {
                        $sessionstate.ImportPSModule($ModulePath)
                    }
                }
                if($UserSnapins.count -gt 0)
                {
                    foreach($PSSnapin in $UserSnapins)
                    {
                        [void]$sessionstate.ImportPSSnapIn($PSSnapin, [ref]$null)
                    }
                }
            }

            #Create runspace pool
            $runspacepool = [runspacefactory]::CreateRunspacePool(1, $Throttle, $sessionstate, $Host)
            $runspacepool.Open() 

            Write-Verbose "Creating empty collection to hold runspace jobs"
            $Script:runspaces = New-Object System.Collections.ArrayList        
        
            #If inputObject is bound get a total count and set bound to true
            $bound = $PSBoundParameters.keys -contains "InputObject"
            if(-not $bound)
            {
                [System.Collections.ArrayList]$allObjects = @()
            }

            #Set up log file if specified
            if( $LogFile ){
                New-Item -ItemType file -path $logFile -force | Out-Null
                ("" | Select Date, Action, Runtime, Status, Details | ConvertTo-Csv -NoTypeInformation -Delimiter ";")[0] | Out-File $LogFile
            }

            #write initial log entry
            $log = "" | Select Date, Action, Runtime, Status, Details
                $log.Date = Get-Date
                $log.Action = "Batch processing started"
                $log.Runtime = $null
                $log.Status = "Started"
                $log.Details = $null
                if($logFile) {
                    ($log | convertto-csv -Delimiter ";" -NoTypeInformation)[1] | Out-File $LogFile -Append
                }

			$timedOutTasks = $false

        #endregion INIT
    }

    process {

        #add piped objects to all objects or set all objects to bound input object parameter
        if($bound)
        {
            $allObjects = $InputObject
        }
        Else
        {
            [void]$allObjects.add( $InputObject )
        }
    }

    end {
        
        #Use Try/Finally to catch Ctrl+C and clean up.
        Try
        {
            #counts for progress
            $totalCount = $allObjects.count
            $script:completedCount = 0
            $startedCount = 0

            foreach($object in $allObjects){
        
                #region add scripts to runspace pool
                    
                    #Create the powershell instance, set verbose if needed, supply the scriptblock and parameters
                    $powershell = [powershell]::Create()
                    
                    if ($VerbosePreference -eq 'Continue')
                    {
                        [void]$PowerShell.AddScript({$VerbosePreference = 'Continue'})
                    }

                    [void]$PowerShell.AddScript($ScriptBlock).AddArgument($object)

                    if ($parameter)
                    {
                        [void]$PowerShell.AddArgument($parameter)
                    }

                    # $Using support from Boe Prox
                    if ($UsingVariableData)
                    {
                        Foreach($UsingVariable in $UsingVariableData) {
                            Write-Verbose "Adding $($UsingVariable.Name) with value: $($UsingVariable.Value)"
                            [void]$PowerShell.AddArgument($UsingVariable.Value)
                        }
                    }

                    #Add the runspace into the powershell instance
                    $powershell.RunspacePool = $runspacepool
    
                    #Create a temporary collection for each runspace
                    $temp = "" | Select-Object PowerShell, StartTime, object, Runspace
                    $temp.PowerShell = $powershell
                    $temp.StartTime = Get-Date
                    $temp.object = $object
    
                    #Save the handle output when calling beginInvoke() that will be used later to end the runspace
                    $temp.Runspace = $powershell.beginInvoke()
                    $startedCount++

                    #Add the temp tracking info to $runspaces collection
                    Write-Verbose ( "Adding {0} to collection at {1}" -f $temp.object, $temp.starttime.tostring() )
                    $runspaces.Add($temp) | Out-Null
            
                    #loop through existing runspaces one time
                    Get-RunspaceData

                    #If we have more running than max queue (used to control timeout accuracy)
                    #Script scope resolves odd PowerShell 2 issue
                    $firstRun = $true
                    while ($runspaces.count -ge $Script:MaxQueue) {

                        #give verbose output
                        if($firstRun){
                            Write-Verbose "$($runspaces.count) items running - exceeded $Script:MaxQueue limit."
                        }
                        $firstRun = $false
                    
                        #run get-runspace data and sleep for a short while
                        Get-RunspaceData
                        Start-Sleep -Milliseconds $sleepTimer
                    
                    }

                #endregion add scripts to runspace pool
            }
                     
            Write-Verbose ( "Finish processing the remaining runspace jobs: {0}" -f ( @($runspaces | Where {$_.Runspace -ne $Null}).Count) )
            Get-RunspaceData -wait

            if (-not $quiet) {
			    Write-Progress -Activity "Running Query" -Status "Starting threads" -Completed
		    }
        }
        Finally
        {
            #Close the runspace pool, unless we specified no close on timeout and something timed out
            if ( ($timedOutTasks -eq $false) -or ( ($timedOutTasks -eq $true) -and ($noCloseOnTimeout -eq $false) ) ) {
	            Write-Verbose "Closing the runspace pool"
			    $runspacepool.close()
            }

            #collect garbage
            [gc]::Collect()
        }       
    }
}


function Invoke-Sqlcmd2 
{
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                <# 
    .SYNOPSIS 
        Runs a T-SQL script. 
    .DESCRIPTION 
        Runs a T-SQL script. Invoke-Sqlcmd2 only returns message output, such as the output of PRINT statements when -verbose parameter is specified.
        Paramaterized queries are supported. 
        Help details below borrowed from Invoke-Sqlcmd
    .PARAMETER ServerInstance
        One or more ServerInstances to query. For default instances, only specify the computer name: "MyComputer". For named instances, use the format "ComputerName\InstanceName".
    .PARAMETER Database
        A character string specifying the name of a database. Invoke-Sqlcmd2 connects to this database in the instance that is specified in -ServerInstance.
        If a SQLConnection is provided, we explicitly switch to this database
    .PARAMETER Query
        Specifies one or more queries to be run. The queries can be Transact-SQL (? or XQuery statements, or sqlcmd commands. Multiple queries separated by a semicolon can be specified. Do not specify the sqlcmd GO separator. Escape any double quotation marks included in the string ?). Consider using bracketed identifiers such as [MyTable] instead of quoted identifiers such as "MyTable".
    .PARAMETER InputFile
        Specifies a file to be used as the query input to Invoke-Sqlcmd2. The file can contain Transact-SQL statements, (? XQuery statements, and sqlcmd commands and scripting variables ?). Specify the full path to the file.
    .PARAMETER Credential
        Specifies A PSCredential for SQL Server Authentication connection to an instance of the Database Engine.
        
        If -Credential is not specified, Invoke-Sqlcmd attempts a Windows Authentication connection using the Windows account running the PowerShell session.
        
        SECURITY NOTE: If you use the -Debug switch, the connectionstring including plain text password will be sent to the debug stream.
    .PARAMETER QueryTimeout
        Specifies the number of seconds before the queries time out.
    .PARAMETER ConnectionTimeout
        Specifies the number of seconds when Invoke-Sqlcmd2 times out if it cannot successfully connect to an instance of the Database Engine. The timeout value must be an integer between 0 and 65534. If 0 is specified, connection attempts do not time out.
    .PARAMETER As
        Specifies output type - DataSet, DataTable, array of DataRow, PSObject or Single Value 
        PSObject output introduces overhead but adds flexibility for working with results: http://powershell.org/wp/forums/topic/dealing-with-dbnull/
    .PARAMETER SqlParameters
        Hashtable of parameters for parameterized SQL queries.  http://blog.codinghorror.com/give-me-parameterized-sql-or-give-me-death/
        Example:
            -Query "SELECT ServerName FROM tblServerInfo WHERE ServerName LIKE @ServerName"
            -SqlParameters @{"ServerName = "c-is-hyperv-1"}
    .PARAMETER AppendServerInstance
        If specified, append the server instance to PSObject and DataRow output
    .PARAMETER SQLConnection
        If specified, use an existing SQLConnection.
            We attempt to open this connection if it is closed
    .INPUTS 
        None 
            You cannot pipe objects to Invoke-Sqlcmd2 
    .OUTPUTS
       As PSObject:     System.Management.Automation.PSCustomObject
       As DataRow:      System.Data.DataRow
       As DataTable:    System.Data.DataTable
       As DataSet:      System.Data.DataTableCollectionSystem.Data.DataSet
       As SingleValue:  Dependent on data type in first column.
    .EXAMPLE 
        Invoke-Sqlcmd2 -ServerInstance "MyComputer\MyInstance" -Query "SELECT login_time AS 'StartTime' FROM sysprocesses WHERE spid = 1" 
    
        This example connects to a named instance of the Database Engine on a computer and runs a basic T-SQL query. 
        StartTime 
        ----------- 
        2010-08-12 21:21:03.593 
    .EXAMPLE 
        Invoke-Sqlcmd2 -ServerInstance "MyComputer\MyInstance" -InputFile "C:\MyFolder\tsqlscript.sql" | Out-File -filePath "C:\MyFolder\tsqlscript.rpt" 
    
        This example reads a file containing T-SQL statements, runs the file, and writes the output to another file. 
    .EXAMPLE 
        Invoke-Sqlcmd2  -ServerInstance "MyComputer\MyInstance" -Query "PRINT 'hello world'" -Verbose 
        This example uses the PowerShell -Verbose parameter to return the message output of the PRINT command. 
        VERBOSE: hello world 
    .EXAMPLE
        Invoke-Sqlcmd2 -ServerInstance MyServer\MyInstance -Query "SELECT ServerName, VCNumCPU FROM tblServerInfo" -as PSObject | ?{$_.VCNumCPU -gt 8}
        Invoke-Sqlcmd2 -ServerInstance MyServer\MyInstance -Query "SELECT ServerName, VCNumCPU FROM tblServerInfo" -as PSObject | ?{$_.VCNumCPU}
        This example uses the PSObject output type to allow more flexibility when working with results.
        
        If we used DataRow rather than PSObject, we would see the following behavior:
            Each row where VCNumCPU does not exist would produce an error in the first example
            Results would include rows where VCNumCPU has DBNull value in the second example
    .EXAMPLE
        'Instance1', 'Server1/Instance1', 'Server2' | Invoke-Sqlcmd2 -query "Sp_databases" -as psobject -AppendServerInstance
        This example lists databases for each instance.  It includes a column for the ServerInstance in question.
            DATABASE_NAME          DATABASE_SIZE REMARKS        ServerInstance                                                     
            -------------          ------------- -------        --------------                                                     
            REDACTED                       88320                Instance1                                                      
            master                         17920                Instance1                                                      
            ...                                                                                              
            msdb                          618112                Server1/Instance1                                                                                                              
            tempdb                        563200                Server1/Instance1
            ...                                                     
            OperationsManager           20480000                Server2                                                            
    .EXAMPLE
        #Construct a query using SQL parameters
            $Query = "SELECT ServerName, VCServerClass, VCServerContact FROM tblServerInfo WHERE VCServerContact LIKE @VCServerContact AND VCServerClass LIKE @VCServerClass"
        #Run the query, specifying values for SQL parameters
            Invoke-Sqlcmd2 -ServerInstance SomeServer\NamedInstance -Database ServerDB -query $query -SqlParameters @{ VCServerContact="%cookiemonster%"; VCServerClass="Prod" }
            
            ServerName    VCServerClass VCServerContact        
            ----------    ------------- ---------------        
            SomeServer1   Prod          cookiemonster, blah                 
            SomeServer2   Prod          cookiemonster                 
            SomeServer3   Prod          blah, cookiemonster                 
    .EXAMPLE
        Invoke-Sqlcmd2 -SQLConnection $Conn -Query "SELECT login_time AS 'StartTime' FROM sysprocesses WHERE spid = 1" 
    
        This example uses an existing SQLConnection and runs a basic T-SQL query against it
        StartTime 
        ----------- 
        2010-08-12 21:21:03.593 
    .NOTES 
        Version History 
        poshcode.org - http://poshcode.org/4967
        v1.0         - Chad Miller - Initial release 
        v1.1         - Chad Miller - Fixed Issue with connection closing 
        v1.2         - Chad Miller - Added inputfile, SQL auth support, connectiontimeout and output message handling. Updated help documentation 
        v1.3         - Chad Miller - Added As parameter to control DataSet, DataTable or array of DataRow Output type 
        v1.4         - Justin Dearing <zippy1981 _at_ gmail.com> - Added the ability to pass parameters to the query.
        v1.4.1       - Paul Bryson <atamido _at_ gmail.com> - Added fix to check for null values in parameterized queries and replace with [DBNull]
        v1.5         - Joel Bennett - add SingleValue output option
        v1.5.1       - RamblingCookieMonster - Added ParameterSets, set Query and InputFile to mandatory
        v1.5.2       - RamblingCookieMonster - Added DBNullToNull switch and code from Dave Wyatt. Added parameters to comment based help (need someone with SQL expertise to verify these)
                 
        github.com   - https://github.com/RamblingCookieMonster/PowerShell
        v1.5.3       - RamblingCookieMonster - Replaced DBNullToNull param with PSObject Output option. Added credential support. Added pipeline support for ServerInstance.  Added to GitHub
                                             - Added AppendServerInstance switch.
                                             - Updated OutputType attribute, comment based help, parameter attributes (thanks supersobbie), removed username/password params
                                             - Added help for sqlparameter parameter.
                                             - Added ErrorAction SilentlyContinue handling to Fill method
        v1.6.0                               - Added SQLConnection parameter and handling.  Is there a more efficient way to handle the parameter sets?
                                             - Fixed SQLConnection handling so that it is not closed (we now only close connections we create)
    .LINK
        https://github.com/RamblingCookieMonster/PowerShell
    .LINK
        New-SQLConnection
    .LINK
        Invoke-SQLBulkCopy
    .LINK
        Out-DataTable
    .functionALITY
        SQL
    #>

                [CmdletBinding( DefaultParameterSetName='Ins-Que' )]
                [OutputType([System.Management.Automation.PSCustomObject],[System.Data.DataRow],[System.Data.DataTable],[System.Data.DataTableCollection],[System.Data.DataSet])]
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    param(
        [Parameter( ParameterSetName='Ins-Que',
                    Position=0,
                    Mandatory=$true,
                    ValueFromPipeline=$true,
                    ValueFromPipelineByPropertyName=$true,
                    ValueFromRemainingArguments=$false,
                    HelpMessage='SQL Server Instance required...' )]
        [Parameter( ParameterSetName='Ins-Fil',
                    Position=0,
                    Mandatory=$true,
                    ValueFromPipeline=$true,
                    ValueFromPipelineByPropertyName=$true,
                    ValueFromRemainingArguments=$false,
                    HelpMessage='SQL Server Instance required...' )]
        [Alias( 'Instance', 'Instances', 'ComputerName', 'Server', 'Servers' )]
        [ValidateNotNullOrEmpty()]
        [string[]]
        $ServerInstance,

        [Parameter( Position=1,
                    Mandatory=$false,
                    ValueFromPipelineByPropertyName=$true,
                    ValueFromRemainingArguments=$false)]
        [string]
        $Database,
    
        [Parameter( ParameterSetName='Ins-Que',
                    Position=2,
                    Mandatory=$true,
                    ValueFromPipelineByPropertyName=$true,
                    ValueFromRemainingArguments=$false )]
        [Parameter( ParameterSetName='Con-Que',
                    Position=2,
                    Mandatory=$true,
                    ValueFromPipelineByPropertyName=$true,
                    ValueFromRemainingArguments=$false )]
        [string]
        $Query,
        
        [Parameter( ParameterSetName='Ins-Fil',
                    Position=2,
                    Mandatory=$true,
                    ValueFromPipelineByPropertyName=$true,
                    ValueFromRemainingArguments=$false )]
        [Parameter( ParameterSetName='Con-Fil',
                    Position=2,
                    Mandatory=$true,
                    ValueFromPipelineByPropertyName=$true,
                    ValueFromRemainingArguments=$false )]
        [ValidateScript({ Test-Path $_ })]
        [string]
        $InputFile,
        
        [Parameter( ParameterSetName='Ins-Que',
                    Position=3,
                    Mandatory=$false,
                    ValueFromPipelineByPropertyName=$true,
                    ValueFromRemainingArguments=$false)]
        [Parameter( ParameterSetName='Ins-Fil',
                    Position=3,
                    Mandatory=$false,
                    ValueFromPipelineByPropertyName=$true,
                    ValueFromRemainingArguments=$false)]
        [System.Management.Automation.PSCredential]
        $Credential,

        [Parameter( Position=4,
                    Mandatory=$false,
                    ValueFromPipelineByPropertyName=$true,
                    ValueFromRemainingArguments=$false )]
        [Int32]
        $QueryTimeout=600,
    
        [Parameter( ParameterSetName='Ins-Fil',
                    Position=5,
                    Mandatory=$false,
                    ValueFromPipelineByPropertyName=$true,
                    ValueFromRemainingArguments=$false )]
        [Parameter( ParameterSetName='Ins-Que',
                    Position=5,
                    Mandatory=$false,
                    ValueFromPipelineByPropertyName=$true,
                    ValueFromRemainingArguments=$false )]
        [Int32]
        $ConnectionTimeout=15,
    
        [Parameter( Position=6,
                    Mandatory=$false,
                    ValueFromPipelineByPropertyName=$true,
                    ValueFromRemainingArguments=$false )]
        [ValidateSet("DataSet", "DataTable", "DataRow","PSObject","SingleValue")]
        [string]
        $As="DataRow",
    
        [Parameter( Position=7,
                    Mandatory=$false,
                    ValueFromPipelineByPropertyName=$true,
                    ValueFromRemainingArguments=$false )]
        [System.Collections.IDictionary]
        $SqlParameters,

        [Parameter( Position=8,
                    Mandatory=$false )]
        [switch]
        $AppendServerInstance,

        [Parameter( ParameterSetName = 'Con-Que',
                    Position=9,
                    Mandatory=$false,
                    ValueFromPipeline=$false,
                    ValueFromPipelineByPropertyName=$false,
                    ValueFromRemainingArguments=$false )]
        [Parameter( ParameterSetName = 'Con-Fil',
                    Position=9,
                    Mandatory=$false,
                    ValueFromPipeline=$false,
                    ValueFromPipelineByPropertyName=$false,
                    ValueFromRemainingArguments=$false )]
        [Alias( 'Connection', 'Conn' )]
        [ValidateNotNullOrEmpty()]
        [System.Data.SqlClient.SQLConnection]
        $SQLConnection
    ) 

                begin
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                {
        if ($InputFile) 
        { 
            $filePath = $(Resolve-Path $InputFile).path 
            $Query =  [System.IO.File]::ReadAllText("$filePath") 
        }

        Write-Verbose "Running Invoke-Sqlcmd2 with ParameterSet '$($PSCmdlet.ParameterSetName)'.  Performing query '$Query'"

        If($As -eq "PSObject")
        {
            #This code scrubs DBNulls.  Props to Dave Wyatt
            $cSharp = @'
                using System;
                using System.Data;
                using System.Management.Automation;
                public class DBNullScrubber
                {
                    public static PSObject DataRowToPSObject(DataRow row)
                    {
                        PSObject psObject = new PSObject();
                        if (row != null && (row.RowState & DataRowState.Detached) != DataRowState.Detached)
                        {
                            foreach (DataColumn column in row.Table.Columns)
                            {
                                Object value = null;
                                if (!row.IsNull(column))
                                {
                                    value = row[column];
                                }
                                psObject.Properties.Add(new PSNoteProperty(column.ColumnName, value));
                            }
                        }
                        return psObject;
                    }
                }
'@

            Try
            {
                Add-Type -TypeDefinition $cSharp -ReferencedAssemblies 'System.Data','System.Xml' -ErrorAction stop
            }
            Catch
            {
                If(-not $_.ToString() -like "*The type name 'DBNullScrubber' already exists*")
                {
                    Write-Warning "Could not load DBNullScrubber.  Defaulting to DataRow output: $_"
                    $As = "Datarow"
                }
            }
        }

        #Handle existing connections
        if($PSBoundParameters.ContainsKey('SQLConnection'))
        {
            if($SQLConnection.State -notlike "Open")
            {
                Try
                {
                    Write-Verbose "Opening connection from '$($SQLConnection.State)' state"
                    $SQLConnection.Open()
                }
                Catch
                {
                    Throw $_
                }
            }

            if($Database -and $SQLConnection.Database -notlike $Database)
            {
                Try
                {
                    Write-Verbose "Changing SQLConnection database from '$($SQLConnection.Database)' to $Database"
                    $SQLConnection.ChangeDatabase($Database)
                }
                Catch
                {
                    Throw "Could not change Connection database '$($SQLConnection.Database)' to $Database`: $_"
                }
            }

            if($SQLConnection.state -like "Open")
            {
                $ServerInstance = @($SQLConnection.DataSource)
            }
            else
            {
                Throw "SQLConnection is not open"
            }
        }

    }
                process
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    {
        foreach($SQLInstance in $ServerInstance)
        {
            Write-Verbose "Querying ServerInstance '$SQLInstance'"

            if($PSBoundParameters.Keys -contains "SQLConnection")
            {
                $Conn = $SQLConnection
            }
            else
            {
                if ($Credential) 
                {
                    $ConnectionString = "Server={0};Database={1};User ID={2};Password=`"{3}`";Trusted_Connection=False;Connect Timeout={4}" -f $SQLInstance,$Database,$Credential.UserName,$Credential.GetNetworkCredential().Password,$ConnectionTimeout
                }
                else 
                {
                    $ConnectionString = "Server={0};Database={1};Integrated Security=True;Connect Timeout={2}" -f $SQLInstance,$Database,$ConnectionTimeout
                } 
            
                $conn = New-Object System.Data.SqlClient.SQLConnection
                $conn.ConnectionString = $ConnectionString 
                Write-Debug "ConnectionString $ConnectionString"

                Try
                {
                    $conn.Open() 
                }
                Catch
                {
                    Write-Error $_
                    continue
                }
            }

            #Following EventHandler is used for PRINT and RAISERROR T-SQL statements. Executed when -Verbose parameter specified by caller 
            if ($PSBoundParameters.Verbose) 
            { 
                $conn.FireInfoMessageEventOnUserErrors=$true 
                $handler = [System.Data.SqlClient.SqlInfoMessageEventHandler] { Write-Verbose "$($_)" } 
                $conn.add_InfoMessage($handler) 
            }
    
            $cmd = New-Object system.Data.SqlClient.SqlCommand($Query,$conn) 
            $cmd.CommandTimeout=$QueryTimeout

            if ($SqlParameters -ne $null)
            {
                $SqlParameters.GetEnumerator() |
                    ForEach-Object {
                        If ($_.Value -ne $null)
                        { $cmd.Parameters.AddWithValue($_.Key, $_.Value) }
                        Else
                        { $cmd.Parameters.AddWithValue($_.Key, [DBNull]::Value) }
                    } > $null
            }
    
            $ds = New-Object system.Data.DataSet 
            $da = New-Object system.Data.SqlClient.SqlDataAdapter($cmd) 
    
            Try
            {
                [void]$da.fill($ds)
                if(-not $PSBoundParameters.ContainsKey('SQLConnection'))
                {
                    $conn.Close()
                }
            }
            Catch
            { 
                $Err = $_
                if(-not $PSBoundParameters.ContainsKey('SQLConnection'))
                {
                    $conn.Close()
                }

                switch ($ErrorActionPreference.tostring())
                {
                    {'SilentlyContinue','Ignore' -contains $_} {}
                    'Stop' {     Throw $Err }
                    'Continue' { Write-Error $Err}
                    Default {    Write-Error $Err}
                }              
            }

            if($AppendServerInstance)
            {
                #Basics from Chad Miller
                $Column =  New-Object Data.DataColumn
                $Column.ColumnName = "ServerInstance"
                $ds.Tables[0].Columns.Add($Column)
                Foreach($row in $ds.Tables[0])
                {
                    $row.ServerInstance = $SQLInstance
                }
            }

            switch ($As) 
            { 
                'DataSet' 
                {
                    $ds
                } 
                'DataTable'
                {
                    $ds.Tables
                } 
                'DataRow'
                {
                    $ds.Tables[0]
                }
                'PSObject'
                {
                    #Scrub DBNulls - Provides convenient results you can use comparisons with
                    #Introduces overhead (e.g. ~2000 rows w/ ~80 columns went from .15 Seconds to .65 Seconds - depending on your data could be much more!)
                    foreach ($row in $ds.Tables[0].Rows)
                    {
                        [DBNullScrubber]::DataRowToPSObject($row)
                    }
                }
                'SingleValue'
                {
                    $ds.Tables[0] | Select-Object -ExpandProperty $ds.Tables[0].Columns[0].ColumnName
                }
            }
        }
    }
            }


function Get-SqlCredHash 
{
    [cmdletbinding()]
    Param 
    (            
        [Parameter()]
        [string] 
        $Connection,
        
        [Parameter()]
        [string] 
        $File 
    )

    begin {
            
        $credObject = @{
            Credential = ''
            ServerInstance = ''
            Database = ''
        }
    }

    process {
                          
        if($Connection) {
                 
            $builder = New-Object System.Data.SqlClient.SqlConnectionStringBuilder($Connection)

            $secpasswd = ConvertTo-SecureString $builder.Password -AsPlainText -Force
                          
            $credObject.Credential = New-Object System.Management.Automation.PSCredential ($builder.UserID, $secpasswd)
    
            $credObject.ServerInstance = $builder.DataSource

            $credObject.Database =  $builder.InitialCatalog
        }
        elseif(test-path $file) { 
                                  
            Write-Host "using $file"
                    
            $bootini  = Get-IniContent -FilePath $file
            
            $connStringElement  = $bootini["boot"]["connection string"]
                   
            $builder = New-Object System.Data.SqlClient.SqlConnectionStringBuilder($connStringElement)

            $secpasswd = ConvertTo-SecureString $builder.Password -AsPlainText -Force
            
            $credObject.Credential = New-Object System.Management.Automation.PSCredential ($builder.UserID, $secpasswd)
    
            $credObject.ServerInstance = $builder.DataSource

            $credObject.Database =  $builder.InitialCatalog      
        }                       
        else { 
                       
            write-host "Get-SqlCredHash did not build parameters"
        }          
    }
                       
    end {
        
        return $CredObject   
    }
}


function Log-AppFailure 
{
    [CmdletBinding()]
    Param
    (              
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash,

        [Parameter(Mandatory=$true)]
        [Datetime] 
        $Date    
    )
    
    process { 
           
        $query = "INSERT INTO [dbo].[APP_FAILURE] ([FAILURE_TIME]) VALUES ('$Date')"  
        
        Invoke-Sqlcmd2 @SqlCredHash -Query $query             
    }
}


function Get-AppFailure 
{
    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash,

        [Parameter(Mandatory=$true)]
        [Datetime] 
        $Date
    )
    
    process {
    
        $query = "select *  from [dbo].[APP_FAILURE] where failure_time ='$Date'"  
        
        $result = Invoke-Sqlcmd2 @SqlCredHash -Query $query

        return  $result             
    }
}


function Get-WhoIsActiveLock 
{
    [CmdletBinding()]
    Param
    (   
        [Parameter(Mandatory=$true)]     
        [hashtable] 
        $SqlCredHash
    )
    
    process {
          
        $query = "SELECT [WIA_Running] FROM [WHOISACTIVE_AppLock]"

        $result = Invoke-Sqlcmd2 @SqlCredHash -Query $query

        return $result      
    }
}


function Lock-WhoIsActiveLock 
{
    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
    )
    
    process {
           
        $date = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
        
        $query = "update [WHOISACTIVE_AppLock] set [WIA_Running] = '-1',Lock_Acquired = '$date'"  
        
        Invoke-Sqlcmd2 @SqlCredHash -Query $query             
    }
}


function Release-WhoIsActiveLock 
{
    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
    )
    
    process {
           
        $query = "update [WHOISACTIVE_AppLock] set [WIA_Running] = '0'"
        
        Invoke-Sqlcmd2 @SqlCredHash -Query $query             
    }
}


function Get-WhoIsActive 
{
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


function Log-WhoIsActive 
{
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

            if(($datarow.sql_text -eq $null) -or ($datarow.sql_text.Contains("sp_server_diagnostics"))) { continue }
                    
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


function Invoke-WhoIsActive 
{
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

           
        Try {

            $timeRange = 1..$Minutes
        
            foreach($minute in $timeRange){

                Run-WhoIsActive -SqlCredHash $SqlCredHash

                Write-Verbose "$minute complete" 

            }
        }
        Finally {

            Release-WhoIsActiveLock -SqlCredHash $SqlCredHash

        }
    }
}


function Run-WhoIsActive 
{
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


function New-AppFailureTable
{
    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
  
    )
    begin {
    
        $Query = @"
        CREATE TABLE [dbo].[APP_FAILURE](
            [RECORD_NUMBER] [int] IDENTITY(1,1) NOT NULL,
            [FAILURE_TIME] [datetime] NOT NULL,
            PRIMARY KEY CLUSTERED 
            (
            [RECORD_NUMBER] ASC
            )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
        ) ON [PRIMARY]
"@

    }
    process {
   
      
         Invoke-Sqlcmd2 @SqlCredHash -Query $query

    } 
}


function New-WhoIsActiveTable
{
    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
  
    )
    begin {
    
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
    
    process {
       
        Invoke-Sqlcmd2 @SqlCredHash -Query $query

    }    
}


function New-WhoIsActiveAppLockTable
{
    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
  
    )
    begin
    {
        $Query = @"              
        CREATE TABLE [dbo].[WHOISACTIVE_AppLock](
	        [WIA_Running] [int] NULL,
	        [Lock_Acquired] [datetime] NULL
        ) ON [PRIMARY]

        INSERT INTO [dbo].[WHOISACTIVE_AppLock] ([WIA_Running])  VALUES (0)

"@

    }

    process {
       
        Invoke-Sqlcmd2 @SqlCredHash -Query $query

    }     
}


function Setup-WhoisActive 
{
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


function Install-WhoIsActive
{
    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
  
    )
    begin
    {
       
        $query = @"
        CREATE PROC [dbo].[sp_WhoIsActive]
        (
        --~
	    --Filters--Both inclusive and exclusive
	    --Set either filter to '' to disable
	    --Valid filter types are: session, program, database, login, and host
	    --Session is a session ID, and either 0 or '' can be used to indicate "all" sessions
	    --All other filter types support % or _ as wildcards
	    @filter sysname = '',
	    @filter_type VARCHAR(10) = 'session',
	    @not_filter sysname = '',
	    @not_filter_type VARCHAR(10) = 'session',

	    --Retrieve data about the calling session?
	    @show_own_spid BIT = 0,

	    --Retrieve data about system sessions?
	    @show_system_spids BIT = 0,

	    --Controls how sleeping SPIDs are handled, based on the idea of levels of interest
	    --0 does not pull any sleeping SPIDs
	    --1 pulls only those sleeping SPIDs that also have an open transaction
	    --2 pulls all sleeping SPIDs
	    @show_sleeping_spids TINYINT = 1,

	    --If 1, gets the full stored procedure or running batch, when available
	    --If 0, gets only the actual statement that is currently running in the batch or procedure
	    @get_full_inner_text BIT = 0,

	    --Get associated query plans for running tasks, if available
	    --If @get_plans = 1, gets the plan based on the request's statement offset
	    --If @get_plans = 2, gets the entire plan based on the request's plan_handle
	    @get_plans TINYINT = 0,

	    --Get the associated outer ad hoc query or stored procedure call, if available
	    @get_outer_command BIT = 0,

	    --Enables pulling transaction log write info and transaction duration
	    @get_transaction_info BIT = 0,

	    --Get information on active tasks, based on three interest levels
	    --Level 0 does not pull any task-related information
	    --Level 1 is a lightweight mode that pulls the top non-CXPACKET wait, giving preference to blockers
	    --Level 2 pulls all available task-based metrics, including: 
	    --number of active tasks, current wait stats, physical I/O, context switches, and blocker information
	    @get_task_info TINYINT = 1,

	    --Gets associated locks for each request, aggregated in an XML format
	    @get_locks BIT = 0,

	    --Get average time for past runs of an active query
	    --(based on the combination of plan handle, sql handle, and offset)
	    @get_avg_time BIT = 0,

	    --Get additional non-performance-related information about the session or request
	    --text_size, language, date_format, date_first, quoted_identifier, arithabort, ansi_null_dflt_on, 
	    --ansi_defaults, ansi_warnings, ansi_padding, ansi_nulls, concat_null_yields_null, 
	    --transaction_isolation_level, lock_timeout, deadlock_priority, row_count, command_type
	    --
	    --If a SQL Agent job is running, an subnode called agent_info will be populated with some or all of
	    --the following: job_id, job_name, step_id, step_name, msdb_query_error (in the event of an error)
	    --
	    --If @get_task_info is set to 2 and a lock wait is detected, a subnode called block_info will be
	    --populated with some or all of the following: lock_type, database_name, object_id, file_id, hobt_id, 
	    --applock_hash, metadata_resource, metadata_class_id, object_name, schema_name
	    @get_additional_info BIT = 0,

	    --Walk the blocking chain and count the number of 
	    --total SPIDs blocked all the way down by a given session
	    --Also enables task_info Level 1, if @get_task_info is set to 0
	    @find_block_leaders BIT = 0,

	    --Pull deltas on various metrics
	    --Interval in seconds to wait before doing the second data pull
	    @delta_interval TINYINT = 0,

	    --List of desired output columns, in desired order
	    --Note that the final output will be the intersection of all enabled features and all 
	    --columns in the list. Therefore, only columns associated with enabled features will 
	    --actually appear in the output. Likewise, removing columns from this list may effectively
	    --disable features, even if they are turned on
	    --
	    --Each element in this list must be one of the valid output column names. Names must be
	    --delimited by square brackets. White space, formatting, and additional characters are
	    --allowed, as long as the list contains exact matches of delimited valid column names.
	    @output_column_list VARCHAR(8000) = '[dd%][session_id][sql_text][sql_command][login_name][wait_info][tasks][tran_log%][cpu%][temp%][block%][reads%][writes%][context%][physical%][query_plan][locks][%]',

	    --Column(s) by which to sort output, optionally with sort directions. 
		    --Valid column choices:
		    --session_id, physical_io, reads, physical_reads, writes, tempdb_allocations,
		    --tempdb_current, CPU, context_switches, used_memory, physical_io_delta, 
		    --reads_delta, physical_reads_delta, writes_delta, tempdb_allocations_delta, 
		    --tempdb_current_delta, CPU_delta, context_switches_delta, used_memory_delta, 
		    --tasks, tran_start_time, open_tran_count, blocking_session_id, blocked_session_count,
		    --percent_complete, host_name, login_name, database_name, start_time, login_time
		    --
		    --Note that column names in the list must be bracket-delimited. Commas and/or white
		    --space are not required. 
	    @sort_order VARCHAR(500) = '[start_time] ASC',

	    --Formats some of the output columns in a more "human readable" form
	    --0 disables outfput format
	    --1 formats the output for variable-width fonts
	    --2 formats the output for fixed-width fonts
	    @format_output TINYINT = 1,

	    --If set to a non-blank value, the script will attempt to insert into the specified 
	    --destination table. Please note that the script will not verify that the table exists, 
	    --or that it has the correct schema, before doing the insert.
	    --Table can be specified in one, two, or three-part format
	    @destination_table VARCHAR(4000) = '',

	    --If set to 1, no data collection will happen and no result set will be returned; instead,
	    --a CREATE TABLE statement will be returned via the @schema parameter, which will match 
	    --the schema of the result set that would be returned by using the same collection of the
	    --rest of the parameters. The CREATE TABLE statement will have a placeholder token of 
	    --<table_name> in place of an actual table name.
	    @return_schema BIT = 0,
	    @schema VARCHAR(MAX) = NULL OUTPUT,

	    --Help! What do I do?
	    @help BIT = 0
    --~
    )
    AS
    BEGIN;
	    SET NOCOUNT ON; 
	    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	    SET QUOTED_IDENTIFIER ON;
	    SET ANSI_PADDING ON;
	    SET CONCAT_NULL_YIELDS_NULL ON;
	    SET ANSI_WARNINGS ON;
	    SET NUMERIC_ROUNDABORT OFF;
	    SET ARITHABORT ON;

	    IF
		    @filter IS NULL
		    OR @filter_type IS NULL
		    OR @not_filter IS NULL
		    OR @not_filter_type IS NULL
		    OR @show_own_spid IS NULL
		    OR @show_system_spids IS NULL
		    OR @show_sleeping_spids IS NULL
		    OR @get_full_inner_text IS NULL
		    OR @get_plans IS NULL
		    OR @get_outer_command IS NULL
		    OR @get_transaction_info IS NULL
		    OR @get_task_info IS NULL
		    OR @get_locks IS NULL
		    OR @get_avg_time IS NULL
		    OR @get_additional_info IS NULL
		    OR @find_block_leaders IS NULL
		    OR @delta_interval IS NULL
		    OR @format_output IS NULL
		    OR @output_column_list IS NULL
		    OR @sort_order IS NULL
		    OR @return_schema IS NULL
		    OR @destination_table IS NULL
		    OR @help IS NULL
	    BEGIN;
		    RAISERROR('Input parameters cannot be NULL', 16, 1);
		    RETURN;
	    END;
	
	    IF @filter_type NOT IN ('session', 'program', 'database', 'login', 'host')
	    BEGIN;
		    RAISERROR('Valid filter types are: session, program, database, login, host', 16, 1);
		    RETURN;
	    END;
	
	    IF @filter_type = 'session' AND @filter LIKE '%[^0123456789]%'
	    BEGIN;
		    RAISERROR('Session filters must be valid integers', 16, 1);
		    RETURN;
	    END;
	
	    IF @not_filter_type NOT IN ('session', 'program', 'database', 'login', 'host')
	    BEGIN;
		    RAISERROR('Valid filter types are: session, program, database, login, host', 16, 1);
		    RETURN;
	    END;
	
	    IF @not_filter_type = 'session' AND @not_filter LIKE '%[^0123456789]%'
	    BEGIN;
		    RAISERROR('Session filters must be valid integers', 16, 1);
		    RETURN;
	    END;
	
	    IF @show_sleeping_spids NOT IN (0, 1, 2)
	    BEGIN;
		    RAISERROR('Valid values for @show_sleeping_spids are: 0, 1, or 2', 16, 1);
		    RETURN;
	    END;
	
	    IF @get_plans NOT IN (0, 1, 2)
	    BEGIN;
		    RAISERROR('Valid values for @get_plans are: 0, 1, or 2', 16, 1);
		    RETURN;
	    END;

	    IF @get_task_info NOT IN (0, 1, 2)
	    BEGIN;
		    RAISERROR('Valid values for @get_task_info are: 0, 1, or 2', 16, 1);
		    RETURN;
	    END;

	    IF @format_output NOT IN (0, 1, 2)
	    BEGIN;
		    RAISERROR('Valid values for @format_output are: 0, 1, or 2', 16, 1);
		    RETURN;
	    END;
	
	    IF @help = 1
	    BEGIN;
		    DECLARE 
			    @header VARCHAR(MAX),
			    @params VARCHAR(MAX),
			    @outputs VARCHAR(MAX);

		    SELECT 
			    @header =
				    REPLACE
				    (
					    REPLACE
					    (
						    CONVERT
						    (
							    VARCHAR(MAX),
							    SUBSTRING
							    (
								    t.text, 
								    CHARINDEX('/' + REPLICATE('*', 93), t.text) + 94,
								    CHARINDEX(REPLICATE('*', 93) + '/', t.text) - (CHARINDEX('/' + REPLICATE('*', 93), t.text) + 94)
							    )
						    ),
						    CHAR(13)+CHAR(10),
						    CHAR(13)
					    ),
					    '	',
					    ''
				    ),
			    @params =
				    CHAR(13) +
					    REPLACE
					    (
						    REPLACE
						    (
							    CONVERT
							    (
								    VARCHAR(MAX),
								    SUBSTRING
								    (
									    t.text, 
									    CHARINDEX('--~', t.text) + 5, 
									    CHARINDEX('--~', t.text, CHARINDEX('--~', t.text) + 5) - (CHARINDEX('--~', t.text) + 5)
								    )
							    ),
							    CHAR(13)+CHAR(10),
							    CHAR(13)
						    ),
						    '	',
						    ''
					    ),
				    @outputs = 
					    CHAR(13) +
						    REPLACE
						    (
							    REPLACE
							    (
								    REPLACE
								    (
									    CONVERT
									    (
										    VARCHAR(MAX),
										    SUBSTRING
										    (
											    t.text, 
											    CHARINDEX('OUTPUT COLUMNS'+CHAR(13)+CHAR(10)+'--------------', t.text) + 32,
											    CHARINDEX('*/', t.text, CHARINDEX('OUTPUT COLUMNS'+CHAR(13)+CHAR(10)+'--------------', t.text) + 32) - (CHARINDEX('OUTPUT COLUMNS'+CHAR(13)+CHAR(10)+'--------------', t.text) + 32)
										    )
									    ),
									    CHAR(9),
									    CHAR(255)
								    ),
								    CHAR(13)+CHAR(10),
								    CHAR(13)
							    ),
							    '	',
							    ''
						    ) +
						    CHAR(13)
			    FROM sys.dm_exec_requests AS r
			    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
			    WHERE
				    r.session_id = @@SPID;

		    WITH
		    a0 AS
		    (SELECT 1 AS n UNION ALL SELECT 1),
		    a1 AS
		    (SELECT 1 AS n FROM a0 AS a, a0 AS b),
		    a2 AS
		    (SELECT 1 AS n FROM a1 AS a, a1 AS b),
		    a3 AS
		    (SELECT 1 AS n FROM a2 AS a, a2 AS b),
		    a4 AS
		    (SELECT 1 AS n FROM a3 AS a, a3 AS b),
		    numbers AS
		    (
			    SELECT TOP(LEN(@header) - 1)
				    ROW_NUMBER() OVER
				    (
					    ORDER BY (SELECT NULL)
				    ) AS number
			    FROM a4
			    ORDER BY
				    number
		    )
		    SELECT
			    RTRIM(LTRIM(
				    SUBSTRING
				    (
					    @header,
					    number + 1,
					    CHARINDEX(CHAR(13), @header, number + 1) - number - 1
				    )
			    )) AS [------header---------------------------------------------------------------------------------------------------------------]
		    FROM numbers
		    WHERE
			    SUBSTRING(@header, number, 1) = CHAR(13);

		    WITH
		    a0 AS
		    (SELECT 1 AS n UNION ALL SELECT 1),
		    a1 AS
		    (SELECT 1 AS n FROM a0 AS a, a0 AS b),
		    a2 AS
		    (SELECT 1 AS n FROM a1 AS a, a1 AS b),
		    a3 AS
		    (SELECT 1 AS n FROM a2 AS a, a2 AS b),
		    a4 AS
		    (SELECT 1 AS n FROM a3 AS a, a3 AS b),
		    numbers AS
		    (
			    SELECT TOP(LEN(@params) - 1)
				    ROW_NUMBER() OVER
				    (
					    ORDER BY (SELECT NULL)
				    ) AS number
			    FROM a4
			    ORDER BY
				    number
		    ),
		    tokens AS
		    (
			    SELECT 
				    RTRIM(LTRIM(
					    SUBSTRING
					    (
						    @params,
						    number + 1,
						    CHARINDEX(CHAR(13), @params, number + 1) - number - 1
					    )
				    )) AS token,
				    number,
				    CASE
					    WHEN SUBSTRING(@params, number + 1, 1) = CHAR(13) THEN number
					    ELSE COALESCE(NULLIF(CHARINDEX(',' + CHAR(13) + CHAR(13), @params, number), 0), LEN(@params)) 
				    END AS param_group,
				    ROW_NUMBER() OVER
				    (
					    PARTITION BY
						    CHARINDEX(',' + CHAR(13) + CHAR(13), @params, number),
						    SUBSTRING(@params, number+1, 1)
					    ORDER BY 
						    number
				    ) AS group_order
			    FROM numbers
			    WHERE
				    SUBSTRING(@params, number, 1) = CHAR(13)
		    ),
		    parsed_tokens AS
		    (
			    SELECT
				    MIN
				    (
					    CASE
						    WHEN token LIKE '@%' THEN token
						    ELSE NULL
					    END
				    ) AS parameter,
				    MIN
				    (
					    CASE
						    WHEN token LIKE '--%' THEN RIGHT(token, LEN(token) - 2)
						    ELSE NULL
					    END
				    ) AS description,
				    param_group,
				    group_order
			    FROM tokens
			    WHERE
				    NOT 
				    (
					    token = '' 
					    AND group_order > 1
				    )
			    GROUP BY
				    param_group,
				    group_order
		    )
		    SELECT
			    CASE
				    WHEN description IS NULL AND parameter IS NULL THEN '-------------------------------------------------------------------------'
				    WHEN param_group = MAX(param_group) OVER() THEN parameter
				    ELSE COALESCE(LEFT(parameter, LEN(parameter) - 1), '')
			    END AS [------parameter----------------------------------------------------------],
			    CASE
				    WHEN description IS NULL AND parameter IS NULL THEN '----------------------------------------------------------------------------------------------------------------------'
				    ELSE COALESCE(description, '')
			    END AS [------description-----------------------------------------------------------------------------------------------------]
		    FROM parsed_tokens
		    ORDER BY
			    param_group, 
			    group_order;
		
		    WITH
		    a0 AS
		    (SELECT 1 AS n UNION ALL SELECT 1),
		    a1 AS
		    (SELECT 1 AS n FROM a0 AS a, a0 AS b),
		    a2 AS
		    (SELECT 1 AS n FROM a1 AS a, a1 AS b),
		    a3 AS
		    (SELECT 1 AS n FROM a2 AS a, a2 AS b),
		    a4 AS
		    (SELECT 1 AS n FROM a3 AS a, a3 AS b),
		    numbers AS
		    (
			    SELECT TOP(LEN(@outputs) - 1)
				    ROW_NUMBER() OVER
				    (
					    ORDER BY (SELECT NULL)
				    ) AS number
			    FROM a4
			    ORDER BY
				    number
		    ),
		    tokens AS
		    (
			    SELECT 
				    RTRIM(LTRIM(
					    SUBSTRING
					    (
						    @outputs,
						    number + 1,
						    CASE
							    WHEN 
								    COALESCE(NULLIF(CHARINDEX(CHAR(13) + 'Formatted', @outputs, number + 1), 0), LEN(@outputs)) < 
								    COALESCE(NULLIF(CHARINDEX(CHAR(13) + CHAR(255) COLLATE Latin1_General_Bin2, @outputs, number + 1), 0), LEN(@outputs))
								    THEN COALESCE(NULLIF(CHARINDEX(CHAR(13) + 'Formatted', @outputs, number + 1), 0), LEN(@outputs)) - number - 1
							    ELSE
								    COALESCE(NULLIF(CHARINDEX(CHAR(13) + CHAR(255) COLLATE Latin1_General_Bin2, @outputs, number + 1), 0), LEN(@outputs)) - number - 1
						    END
					    )
				    )) AS token,
				    number,
				    COALESCE(NULLIF(CHARINDEX(CHAR(13) + 'Formatted', @outputs, number + 1), 0), LEN(@outputs)) AS output_group,
				    ROW_NUMBER() OVER
				    (
					    PARTITION BY 
						    COALESCE(NULLIF(CHARINDEX(CHAR(13) + 'Formatted', @outputs, number + 1), 0), LEN(@outputs))
					    ORDER BY
						    number
				    ) AS output_group_order
			    FROM numbers
			    WHERE
				    SUBSTRING(@outputs, number, 10) = CHAR(13) + 'Formatted'
				    OR SUBSTRING(@outputs, number, 2) = CHAR(13) + CHAR(255) COLLATE Latin1_General_Bin2
		    ),
		    output_tokens AS
		    (
			    SELECT 
				    *,
				    CASE output_group_order
					    WHEN 2 THEN MAX(CASE output_group_order WHEN 1 THEN token ELSE NULL END) OVER (PARTITION BY output_group)
					    ELSE ''
				    END COLLATE Latin1_General_Bin2 AS column_info
			    FROM tokens
		    )
		    SELECT
			    CASE output_group_order
				    WHEN 1 THEN '-----------------------------------'
				    WHEN 2 THEN 
					    CASE
						    WHEN CHARINDEX('Formatted/Non:', column_info) = 1 THEN
							    SUBSTRING(column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info)+1, CHARINDEX(']', column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info)+2) - CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info))
						    ELSE
							    SUBSTRING(column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info)+2, CHARINDEX(']', column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info)+2) - CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info)-1)
					    END
				    ELSE ''
			    END AS formatted_column_name,
			    CASE output_group_order
				    WHEN 1 THEN '-----------------------------------'
				    WHEN 2 THEN 
					    CASE
						    WHEN CHARINDEX('Formatted/Non:', column_info) = 1 THEN
							    SUBSTRING(column_info, CHARINDEX(']', column_info)+2, LEN(column_info))
						    ELSE
							    SUBSTRING(column_info, CHARINDEX(']', column_info)+2, CHARINDEX('Non-Formatted:', column_info, CHARINDEX(']', column_info)+2) - CHARINDEX(']', column_info)-3)
					    END
				    ELSE ''
			    END AS formatted_column_type,
			    CASE output_group_order
				    WHEN 1 THEN '---------------------------------------'
				    WHEN 2 THEN 
					    CASE
						    WHEN CHARINDEX('Formatted/Non:', column_info) = 1 THEN ''
						    ELSE
							    CASE
								    WHEN SUBSTRING(column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info, CHARINDEX('Non-Formatted:', column_info))+1, 1) = '<' THEN
									    SUBSTRING(column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info, CHARINDEX('Non-Formatted:', column_info))+1, CHARINDEX('>', column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info, CHARINDEX('Non-Formatted:', column_info))+1) - CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info, CHARINDEX('Non-Formatted:', column_info)))
								    ELSE
									    SUBSTRING(column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info, CHARINDEX('Non-Formatted:', column_info))+1, CHARINDEX(']', column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info, CHARINDEX('Non-Formatted:', column_info))+1) - CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info, CHARINDEX('Non-Formatted:', column_info)))
							    END
					    END
				    ELSE ''
			    END AS unformatted_column_name,
			    CASE output_group_order
				    WHEN 1 THEN '---------------------------------------'
				    WHEN 2 THEN 
					    CASE
						    WHEN CHARINDEX('Formatted/Non:', column_info) = 1 THEN ''
						    ELSE
							    CASE
								    WHEN SUBSTRING(column_info, CHARINDEX(CHAR(255) COLLATE Latin1_General_Bin2, column_info, CHARINDEX('Non-Formatted:', column_info))+1, 1) = '<' THEN ''
								    ELSE
									    SUBSTRING(column_info, CHARINDEX(']', column_info, CHARINDEX('Non-Formatted:', column_info))+2, CHARINDEX('Non-Formatted:', column_info, CHARINDEX(']', column_info)+2) - CHARINDEX(']', column_info)-3)
							    END
					    END
				    ELSE ''
			    END AS unformatted_column_type,
			    CASE output_group_order
				    WHEN 1 THEN '----------------------------------------------------------------------------------------------------------------------'
				    ELSE REPLACE(token, CHAR(255) COLLATE Latin1_General_Bin2, '')
			    END AS [------description-----------------------------------------------------------------------------------------------------]
		    FROM output_tokens
		    WHERE
			    NOT 
			    (
				    output_group_order = 1 
				    AND output_group = LEN(@outputs)
			    )
		    ORDER BY
			    output_group,
			    CASE output_group_order
				    WHEN 1 THEN 99
				    ELSE output_group_order
			    END;

		    RETURN;
	    END;

	    WITH
	    a0 AS
	    (SELECT 1 AS n UNION ALL SELECT 1),
	    a1 AS
	    (SELECT 1 AS n FROM a0 AS a, a0 AS b),
	    a2 AS
	    (SELECT 1 AS n FROM a1 AS a, a1 AS b),
	    a3 AS
	    (SELECT 1 AS n FROM a2 AS a, a2 AS b),
	    a4 AS
	    (SELECT 1 AS n FROM a3 AS a, a3 AS b),
	    numbers AS
	    (
		    SELECT TOP(LEN(@output_column_list))
			    ROW_NUMBER() OVER
			    (
				    ORDER BY (SELECT NULL)
			    ) AS number
		    FROM a4
		    ORDER BY
			    number
	    ),
	    tokens AS
	    (
		    SELECT 
			    '|[' +
				    SUBSTRING
				    (
					    @output_column_list,
					    number + 1,
					    CHARINDEX(']', @output_column_list, number) - number - 1
				    ) + '|]' AS token,
			    number
		    FROM numbers
		    WHERE
			    SUBSTRING(@output_column_list, number, 1) = '['
	    ),
	    ordered_columns AS
	    (
		    SELECT
			    x.column_name,
			    ROW_NUMBER() OVER
			    (
				    PARTITION BY
					    x.column_name
				    ORDER BY
					    tokens.number,
					    x.default_order
			    ) AS r,
			    ROW_NUMBER() OVER
			    (
				    ORDER BY
					    tokens.number,
					    x.default_order
			    ) AS s
		    FROM tokens
		    JOIN
		    (
			    SELECT '[session_id]' AS column_name, 1 AS default_order
			    UNION ALL
			    SELECT '[dd hh:mm:ss.mss]', 2
			    WHERE
				    @format_output = 1
			    UNION ALL
			    SELECT '[dd hh:mm:ss.mss (avg)]', 3
			    WHERE
				    @format_output = 1
				    AND @get_avg_time = 1
			    UNION ALL
			    SELECT '[avg_elapsed_time]', 4
			    WHERE
				    @format_output = 0
				    AND @get_avg_time = 1
			    UNION ALL
			    SELECT '[physical_io]', 5
			    WHERE
				    @get_task_info = 2
			    UNION ALL
			    SELECT '[reads]', 6
			    UNION ALL
			    SELECT '[physical_reads]', 7
			    UNION ALL
			    SELECT '[writes]', 8
			    UNION ALL
			    SELECT '[tempdb_allocations]', 9
			    UNION ALL
			    SELECT '[tempdb_current]', 10
			    UNION ALL
			    SELECT '[CPU]', 11
			    UNION ALL
			    SELECT '[context_switches]', 12
			    WHERE
				    @get_task_info = 2
			    UNION ALL
			    SELECT '[used_memory]', 13
			    UNION ALL
			    SELECT '[physical_io_delta]', 14
			    WHERE
				    @delta_interval > 0	
				    AND @get_task_info = 2
			    UNION ALL
			    SELECT '[reads_delta]', 15
			    WHERE
				    @delta_interval > 0
			    UNION ALL
			    SELECT '[physical_reads_delta]', 16
			    WHERE
				    @delta_interval > 0
			    UNION ALL
			    SELECT '[writes_delta]', 17
			    WHERE
				    @delta_interval > 0
			    UNION ALL
			    SELECT '[tempdb_allocations_delta]', 18
			    WHERE
				    @delta_interval > 0
			    UNION ALL
			    SELECT '[tempdb_current_delta]', 19
			    WHERE
				    @delta_interval > 0
			    UNION ALL
			    SELECT '[CPU_delta]', 20
			    WHERE
				    @delta_interval > 0
			    UNION ALL
			    SELECT '[context_switches_delta]', 21
			    WHERE
				    @delta_interval > 0
				    AND @get_task_info = 2
			    UNION ALL
			    SELECT '[used_memory_delta]', 22
			    WHERE
				    @delta_interval > 0
			    UNION ALL
			    SELECT '[tasks]', 23
			    WHERE
				    @get_task_info = 2
			    UNION ALL
			    SELECT '[status]', 24
			    UNION ALL
			    SELECT '[wait_info]', 25
			    WHERE
				    @get_task_info > 0
				    OR @find_block_leaders = 1
			    UNION ALL
			    SELECT '[locks]', 26
			    WHERE
				    @get_locks = 1
			    UNION ALL
			    SELECT '[tran_start_time]', 27
			    WHERE
				    @get_transaction_info = 1
			    UNION ALL
			    SELECT '[tran_log_writes]', 28
			    WHERE
				    @get_transaction_info = 1
			    UNION ALL
			    SELECT '[open_tran_count]', 29
			    UNION ALL
			    SELECT '[sql_command]', 30
			    WHERE
				    @get_outer_command = 1
			    UNION ALL
			    SELECT '[sql_text]', 31
			    UNION ALL
			    SELECT '[query_plan]', 32
			    WHERE
				    @get_plans >= 1
			    UNION ALL
			    SELECT '[blocking_session_id]', 33
			    WHERE
				    @get_task_info > 0
				    OR @find_block_leaders = 1
			    UNION ALL
			    SELECT '[blocked_session_count]', 34
			    WHERE
				    @find_block_leaders = 1
			    UNION ALL
			    SELECT '[percent_complete]', 35
			    UNION ALL
			    SELECT '[host_name]', 36
			    UNION ALL
			    SELECT '[login_name]', 37
			    UNION ALL
			    SELECT '[database_name]', 38
			    UNION ALL
			    SELECT '[program_name]', 39
			    UNION ALL
			    SELECT '[additional_info]', 40
			    WHERE
				    @get_additional_info = 1
			    UNION ALL
			    SELECT '[start_time]', 41
			    UNION ALL
			    SELECT '[login_time]', 42
			    UNION ALL
			    SELECT '[request_id]', 43
			    UNION ALL
			    SELECT '[collection_time]', 44
		    ) AS x ON 
			    x.column_name LIKE token ESCAPE '|'
	    )
	    SELECT
		    @output_column_list =
			    STUFF
			    (
				    (
					    SELECT
						    ',' + column_name as [text()]
					    FROM ordered_columns
					    WHERE
						    r = 1
					    ORDER BY
						    s
					    FOR XML
						    PATH('')
				    ),
				    1,
				    1,
				    ''
			    );
	
	    IF COALESCE(RTRIM(@output_column_list), '') = ''
	    BEGIN;
		    RAISERROR('No valid column matches found in @output_column_list or no columns remain due to selected options.', 16, 1);
		    RETURN;
	    END;
	
	    IF @destination_table <> ''
	    BEGIN;
		    SET @destination_table = 
			    --database
			    COALESCE(QUOTENAME(PARSENAME(@destination_table, 3)) + '.', '') +
			    --schema
			    COALESCE(QUOTENAME(PARSENAME(@destination_table, 2)) + '.', '') +
			    --table
			    COALESCE(QUOTENAME(PARSENAME(@destination_table, 1)), '');
			
		    IF COALESCE(RTRIM(@destination_table), '') = ''
		    BEGIN;
			    RAISERROR('Destination table not properly formatted.', 16, 1);
			    RETURN;
		    END;
	    END;

	    WITH
	    a0 AS
	    (SELECT 1 AS n UNION ALL SELECT 1),
	    a1 AS
	    (SELECT 1 AS n FROM a0 AS a, a0 AS b),
	    a2 AS
	    (SELECT 1 AS n FROM a1 AS a, a1 AS b),
	    a3 AS
	    (SELECT 1 AS n FROM a2 AS a, a2 AS b),
	    a4 AS
	    (SELECT 1 AS n FROM a3 AS a, a3 AS b),
	    numbers AS
	    (
		    SELECT TOP(LEN(@sort_order))
			    ROW_NUMBER() OVER
			    (
				    ORDER BY (SELECT NULL)
			    ) AS number
		    FROM a4
		    ORDER BY
			    number
	    ),
	    tokens AS
	    (
		    SELECT 
			    '|[' +
				    SUBSTRING
				    (
					    @sort_order,
					    number + 1,
					    CHARINDEX(']', @sort_order, number) - number - 1
				    ) + '|]' AS token,
			    SUBSTRING
			    (
				    @sort_order,
				    CHARINDEX(']', @sort_order, number) + 1,
				    COALESCE(NULLIF(CHARINDEX('[', @sort_order, CHARINDEX(']', @sort_order, number)), 0), LEN(@sort_order)) - CHARINDEX(']', @sort_order, number)
			    ) AS next_chunk,
			    number
		    FROM numbers
		    WHERE
			    SUBSTRING(@sort_order, number, 1) = '['
	    ),
	    ordered_columns AS
	    (
		    SELECT
			    x.column_name +
				    CASE
					    WHEN tokens.next_chunk LIKE '%asc%' THEN ' ASC'
					    WHEN tokens.next_chunk LIKE '%desc%' THEN ' DESC'
					    ELSE ''
				    END AS column_name,
			    ROW_NUMBER() OVER
			    (
				    PARTITION BY
					    x.column_name
				    ORDER BY
					    tokens.number
			    ) AS r,
			    tokens.number
		    FROM tokens
		    JOIN
		    (
			    SELECT '[session_id]' AS column_name
			    UNION ALL
			    SELECT '[physical_io]'
			    UNION ALL
			    SELECT '[reads]'
			    UNION ALL
			    SELECT '[physical_reads]'
			    UNION ALL
			    SELECT '[writes]'
			    UNION ALL
			    SELECT '[tempdb_allocations]'
			    UNION ALL
			    SELECT '[tempdb_current]'
			    UNION ALL
			    SELECT '[CPU]'
			    UNION ALL
			    SELECT '[context_switches]'
			    UNION ALL
			    SELECT '[used_memory]'
			    UNION ALL
			    SELECT '[physical_io_delta]'
			    UNION ALL
			    SELECT '[reads_delta]'
			    UNION ALL
			    SELECT '[physical_reads_delta]'
			    UNION ALL
			    SELECT '[writes_delta]'
			    UNION ALL
			    SELECT '[tempdb_allocations_delta]'
			    UNION ALL
			    SELECT '[tempdb_current_delta]'
			    UNION ALL
			    SELECT '[CPU_delta]'
			    UNION ALL
			    SELECT '[context_switches_delta]'
			    UNION ALL
			    SELECT '[used_memory_delta]'
			    UNION ALL
			    SELECT '[tasks]'
			    UNION ALL
			    SELECT '[tran_start_time]'
			    UNION ALL
			    SELECT '[open_tran_count]'
			    UNION ALL
			    SELECT '[blocking_session_id]'
			    UNION ALL
			    SELECT '[blocked_session_count]'
			    UNION ALL
			    SELECT '[percent_complete]'
			    UNION ALL
			    SELECT '[host_name]'
			    UNION ALL
			    SELECT '[login_name]'
			    UNION ALL
			    SELECT '[database_name]'
			    UNION ALL
			    SELECT '[start_time]'
			    UNION ALL
			    SELECT '[login_time]'
		    ) AS x ON 
			    x.column_name LIKE token ESCAPE '|'
	    )
	    SELECT
		    @sort_order = COALESCE(z.sort_order, '')
	    FROM
	    (
		    SELECT
			    STUFF
			    (
				    (
					    SELECT
						    ',' + column_name as [text()]
					    FROM ordered_columns
					    WHERE
						    r = 1
					    ORDER BY
						    number
					    FOR XML
						    PATH('')
				    ),
				    1,
				    1,
				    ''
			    ) AS sort_order
	    ) AS z;

	    CREATE TABLE #sessions
	    (
		    recursion SMALLINT NOT NULL,
		    session_id SMALLINT NOT NULL,
		    request_id INT NOT NULL,
		    session_number INT NOT NULL,
		    elapsed_time INT NOT NULL,
		    avg_elapsed_time INT NULL,
		    physical_io BIGINT NULL,
		    reads BIGINT NULL,
		    physical_reads BIGINT NULL,
		    writes BIGINT NULL,
		    tempdb_allocations BIGINT NULL,
		    tempdb_current BIGINT NULL,
		    CPU INT NULL,
		    thread_CPU_snapshot BIGINT NULL,
		    context_switches BIGINT NULL,
		    used_memory BIGINT NOT NULL, 
		    tasks SMALLINT NULL,
		    status VARCHAR(30) NOT NULL,
		    wait_info NVARCHAR(4000) NULL,
		    locks XML NULL,
		    transaction_id BIGINT NULL,
		    tran_start_time DATETIME NULL,
		    tran_log_writes NVARCHAR(4000) NULL,
		    open_tran_count SMALLINT NULL,
		    sql_command XML NULL,
		    sql_handle VARBINARY(64) NULL,
		    statement_start_offset INT NULL,
		    statement_end_offset INT NULL,
		    sql_text XML NULL,
		    plan_handle VARBINARY(64) NULL,
		    query_plan XML NULL,
		    blocking_session_id SMALLINT NULL,
		    blocked_session_count SMALLINT NULL,
		    percent_complete REAL NULL,
		    host_name sysname NULL,
		    login_name sysname NOT NULL,
		    database_name sysname NULL,
		    program_name sysname NULL,
		    additional_info XML NULL,
		    start_time DATETIME NOT NULL,
		    login_time DATETIME NULL,
		    last_request_start_time DATETIME NULL,
		    PRIMARY KEY CLUSTERED (session_id, request_id, recursion) WITH (IGNORE_DUP_KEY = ON),
		    UNIQUE NONCLUSTERED (transaction_id, session_id, request_id, recursion) WITH (IGNORE_DUP_KEY = ON)
	    );

	    IF @return_schema = 0
	    BEGIN;
		    --Disable unnecessary autostats on the table
		    CREATE STATISTICS s_session_id ON #sessions (session_id)
		    WITH SAMPLE 0 ROWS, NORECOMPUTE;
		    CREATE STATISTICS s_request_id ON #sessions (request_id)
		    WITH SAMPLE 0 ROWS, NORECOMPUTE;
		    CREATE STATISTICS s_transaction_id ON #sessions (transaction_id)
		    WITH SAMPLE 0 ROWS, NORECOMPUTE;
		    CREATE STATISTICS s_session_number ON #sessions (session_number)
		    WITH SAMPLE 0 ROWS, NORECOMPUTE;
		    CREATE STATISTICS s_status ON #sessions (status)
		    WITH SAMPLE 0 ROWS, NORECOMPUTE;
		    CREATE STATISTICS s_start_time ON #sessions (start_time)
		    WITH SAMPLE 0 ROWS, NORECOMPUTE;
		    CREATE STATISTICS s_last_request_start_time ON #sessions (last_request_start_time)
		    WITH SAMPLE 0 ROWS, NORECOMPUTE;
		    CREATE STATISTICS s_recursion ON #sessions (recursion)
		    WITH SAMPLE 0 ROWS, NORECOMPUTE;

		    DECLARE @recursion SMALLINT;
		    SET @recursion = 
			    CASE @delta_interval
				    WHEN 0 THEN 1
				    ELSE -1
			    END;

		    DECLARE @first_collection_ms_ticks BIGINT;
		    DECLARE @last_collection_start DATETIME;

		    --Used for the delta pull
		    REDO:;
		
		    IF 
			    @get_locks = 1 
			    AND @recursion = 1
			    AND @output_column_list LIKE '%|[locks|]%' ESCAPE '|'
		    BEGIN;
			    SELECT
				    y.resource_type,
				    y.database_name,
				    y.object_id,
				    y.file_id,
				    y.page_type,
				    y.hobt_id,
				    y.allocation_unit_id,
				    y.index_id,
				    y.schema_id,
				    y.principal_id,
				    y.request_mode,
				    y.request_status,
				    y.session_id,
				    y.resource_description,
				    y.request_count,
				    s.request_id,
				    s.start_time,
				    CONVERT(sysname, NULL) AS object_name,
				    CONVERT(sysname, NULL) AS index_name,
				    CONVERT(sysname, NULL) AS schema_name,
				    CONVERT(sysname, NULL) AS principal_name,
				    CONVERT(NVARCHAR(2048), NULL) AS query_error
			    INTO #locks
			    FROM
			    (
				    SELECT
					    sp.spid AS session_id,
					    CASE sp.status
						    WHEN 'sleeping' THEN CONVERT(INT, 0)
						    ELSE sp.request_id
					    END AS request_id,
					    CASE sp.status
						    WHEN 'sleeping' THEN sp.last_batch
						    ELSE COALESCE(req.start_time, sp.last_batch)
					    END AS start_time,
					    sp.dbid
				    FROM sys.sysprocesses AS sp
				    OUTER APPLY
				    (
					    SELECT TOP(1)
						    CASE
							    WHEN 
							    (
								    sp.hostprocess > ''
								    OR r.total_elapsed_time < 0
							    ) THEN
								    r.start_time
							    ELSE
								    DATEADD
								    (
									    ms, 
									    1000 * (DATEPART(ms, DATEADD(second, -(r.total_elapsed_time / 1000), GETDATE())) / 500) - DATEPART(ms, DATEADD(second, -(r.total_elapsed_time / 1000), GETDATE())), 
									    DATEADD(second, -(r.total_elapsed_time / 1000), GETDATE())
								    )
						    END AS start_time
					    FROM sys.dm_exec_requests AS r
					    WHERE
						    r.session_id = sp.spid
						    AND r.request_id = sp.request_id
				    ) AS req
				    WHERE
					    --Process inclusive filter
					    1 =
						    CASE
							    WHEN @filter <> '' THEN
								    CASE @filter_type
									    WHEN 'session' THEN
										    CASE
											    WHEN
												    CONVERT(SMALLINT, @filter) = 0
												    OR sp.spid = CONVERT(SMALLINT, @filter)
													    THEN 1
											    ELSE 0
										    END
									    WHEN 'program' THEN
										    CASE
											    WHEN sp.program_name LIKE @filter THEN 1
											    ELSE 0
										    END
									    WHEN 'login' THEN
										    CASE
											    WHEN sp.loginame LIKE @filter THEN 1
											    ELSE 0
										    END
									    WHEN 'host' THEN
										    CASE
											    WHEN sp.hostname LIKE @filter THEN 1
											    ELSE 0
										    END
									    WHEN 'database' THEN
										    CASE
											    WHEN DB_NAME(sp.dbid) LIKE @filter THEN 1
											    ELSE 0
										    END
									    ELSE 0
								    END
							    ELSE 1
						    END
					    --Process exclusive filter
					    AND 0 =
						    CASE
							    WHEN @not_filter <> '' THEN
								    CASE @not_filter_type
									    WHEN 'session' THEN
										    CASE
											    WHEN sp.spid = CONVERT(SMALLINT, @not_filter) THEN 1
											    ELSE 0
										    END
									    WHEN 'program' THEN
										    CASE
											    WHEN sp.program_name LIKE @not_filter THEN 1
											    ELSE 0
										    END
									    WHEN 'login' THEN
										    CASE
											    WHEN sp.loginame LIKE @not_filter THEN 1
											    ELSE 0
										    END
									    WHEN 'host' THEN
										    CASE
											    WHEN sp.hostname LIKE @not_filter THEN 1
											    ELSE 0
										    END
									    WHEN 'database' THEN
										    CASE
											    WHEN DB_NAME(sp.dbid) LIKE @not_filter THEN 1
											    ELSE 0
										    END
									    ELSE 0
								    END
							    ELSE 0
						    END
					    AND 
					    (
						    @show_own_spid = 1
						    OR sp.spid <> @@SPID
					    )
					    AND 
					    (
						    @show_system_spids = 1
						    OR sp.hostprocess > ''
					    )
					    AND sp.ecid = 0
			    ) AS s
			    INNER HASH JOIN
			    (
				    SELECT
					    x.resource_type,
					    x.database_name,
					    x.object_id,
					    x.file_id,
					    CASE
						    WHEN x.page_no = 1 OR x.page_no % 8088 = 0 THEN 'PFS'
						    WHEN x.page_no = 2 OR x.page_no % 511232 = 0 THEN 'GAM'
						    WHEN x.page_no = 3 OR x.page_no % 511233 = 0 THEN 'SGAM'
						    WHEN x.page_no = 6 OR x.page_no % 511238 = 0 THEN 'DCM'
						    WHEN x.page_no = 7 OR x.page_no % 511239 = 0 THEN 'BCM'
						    WHEN x.page_no IS NOT NULL THEN '*'
						    ELSE NULL
					    END AS page_type,
					    x.hobt_id,
					    x.allocation_unit_id,
					    x.index_id,
					    x.schema_id,
					    x.principal_id,
					    x.request_mode,
					    x.request_status,
					    x.session_id,
					    x.request_id,
					    CASE
						    WHEN COALESCE(x.object_id, x.file_id, x.hobt_id, x.allocation_unit_id, x.index_id, x.schema_id, x.principal_id) IS NULL THEN NULLIF(resource_description, '')
						    ELSE NULL
					    END AS resource_description,
					    COUNT(*) AS request_count
				    FROM
				    (
					    SELECT
						    tl.resource_type +
							    CASE
								    WHEN tl.resource_subtype = '' THEN ''
								    ELSE '.' + tl.resource_subtype
							    END AS resource_type,
						    COALESCE(DB_NAME(tl.resource_database_id), N'(null)') AS database_name,
						    CONVERT
						    (
							    INT,
							    CASE
								    WHEN tl.resource_type = 'OBJECT' THEN tl.resource_associated_entity_id
								    WHEN tl.resource_description LIKE '%object_id = %' THEN
									    (
										    SUBSTRING
										    (
											    tl.resource_description, 
											    (CHARINDEX('object_id = ', tl.resource_description) + 12), 
											    COALESCE
											    (
												    NULLIF
												    (
													    CHARINDEX(',', tl.resource_description, CHARINDEX('object_id = ', tl.resource_description) + 12),
													    0
												    ), 
												    DATALENGTH(tl.resource_description)+1
											    ) - (CHARINDEX('object_id = ', tl.resource_description) + 12)
										    )
									    )
								    ELSE NULL
							    END
						    ) AS object_id,
						    CONVERT
						    (
							    INT,
							    CASE 
								    WHEN tl.resource_type = 'FILE' THEN CONVERT(INT, tl.resource_description)
								    WHEN tl.resource_type IN ('PAGE', 'EXTENT', 'RID') THEN LEFT(tl.resource_description, CHARINDEX(':', tl.resource_description)-1)
								    ELSE NULL
							    END
						    ) AS file_id,
						    CONVERT
						    (
							    INT,
							    CASE
								    WHEN tl.resource_type IN ('PAGE', 'EXTENT', 'RID') THEN 
									    SUBSTRING
									    (
										    tl.resource_description, 
										    CHARINDEX(':', tl.resource_description) + 1, 
										    COALESCE
										    (
											    NULLIF
											    (
												    CHARINDEX(':', tl.resource_description, CHARINDEX(':', tl.resource_description) + 1), 
												    0
											    ), 
											    DATALENGTH(tl.resource_description)+1
										    ) - (CHARINDEX(':', tl.resource_description) + 1)
									    )
								    ELSE NULL
							    END
						    ) AS page_no,
						    CASE
							    WHEN tl.resource_type IN ('PAGE', 'KEY', 'RID', 'HOBT') THEN tl.resource_associated_entity_id
							    ELSE NULL
						    END AS hobt_id,
						    CASE
							    WHEN tl.resource_type = 'ALLOCATION_UNIT' THEN tl.resource_associated_entity_id
							    ELSE NULL
						    END AS allocation_unit_id,
						    CONVERT
						    (
							    INT,
							    CASE
								    WHEN
									    /*TODO: Deal with server principals*/ 
									    tl.resource_subtype <> 'SERVER_PRINCIPAL' 
									    AND tl.resource_description LIKE '%index_id or stats_id = %' THEN
									    (
										    SUBSTRING
										    (
											    tl.resource_description, 
											    (CHARINDEX('index_id or stats_id = ', tl.resource_description) + 23), 
											    COALESCE
											    (
												    NULLIF
												    (
													    CHARINDEX(',', tl.resource_description, CHARINDEX('index_id or stats_id = ', tl.resource_description) + 23), 
													    0
												    ), 
												    DATALENGTH(tl.resource_description)+1
											    ) - (CHARINDEX('index_id or stats_id = ', tl.resource_description) + 23)
										    )
									    )
								    ELSE NULL
							    END 
						    ) AS index_id,
						    CONVERT
						    (
							    INT,
							    CASE
								    WHEN tl.resource_description LIKE '%schema_id = %' THEN
									    (
										    SUBSTRING
										    (
											    tl.resource_description, 
											    (CHARINDEX('schema_id = ', tl.resource_description) + 12), 
											    COALESCE
											    (
												    NULLIF
												    (
													    CHARINDEX(',', tl.resource_description, CHARINDEX('schema_id = ', tl.resource_description) + 12), 
													    0
												    ), 
												    DATALENGTH(tl.resource_description)+1
											    ) - (CHARINDEX('schema_id = ', tl.resource_description) + 12)
										    )
									    )
								    ELSE NULL
							    END 
						    ) AS schema_id,
						    CONVERT
						    (
							    INT,
							    CASE
								    WHEN tl.resource_description LIKE '%principal_id = %' THEN
									    (
										    SUBSTRING
										    (
											    tl.resource_description, 
											    (CHARINDEX('principal_id = ', tl.resource_description) + 15), 
											    COALESCE
											    (
												    NULLIF
												    (
													    CHARINDEX(',', tl.resource_description, CHARINDEX('principal_id = ', tl.resource_description) + 15), 
													    0
												    ), 
												    DATALENGTH(tl.resource_description)+1
											    ) - (CHARINDEX('principal_id = ', tl.resource_description) + 15)
										    )
									    )
								    ELSE NULL
							    END
						    ) AS principal_id,
						    tl.request_mode,
						    tl.request_status,
						    tl.request_session_id AS session_id,
						    tl.request_request_id AS request_id,

						    /*TODO: Applocks, other resource_descriptions*/
						    RTRIM(tl.resource_description) AS resource_description,
						    tl.resource_associated_entity_id
						    /*********************************************/
					    FROM 
					    (
						    SELECT 
							    request_session_id,
							    CONVERT(VARCHAR(120), resource_type) COLLATE Latin1_General_Bin2 AS resource_type,
							    CONVERT(VARCHAR(120), resource_subtype) COLLATE Latin1_General_Bin2 AS resource_subtype,
							    resource_database_id,
							    CONVERT(VARCHAR(512), resource_description) COLLATE Latin1_General_Bin2 AS resource_description,
							    resource_associated_entity_id,
							    CONVERT(VARCHAR(120), request_mode) COLLATE Latin1_General_Bin2 AS request_mode,
							    CONVERT(VARCHAR(120), request_status) COLLATE Latin1_General_Bin2 AS request_status,
							    request_request_id
						    FROM sys.dm_tran_locks
					    ) AS tl
				    ) AS x
				    GROUP BY
					    x.resource_type,
					    x.database_name,
					    x.object_id,
					    x.file_id,
					    CASE
						    WHEN x.page_no = 1 OR x.page_no % 8088 = 0 THEN 'PFS'
						    WHEN x.page_no = 2 OR x.page_no % 511232 = 0 THEN 'GAM'
						    WHEN x.page_no = 3 OR x.page_no % 511233 = 0 THEN 'SGAM'
						    WHEN x.page_no = 6 OR x.page_no % 511238 = 0 THEN 'DCM'
						    WHEN x.page_no = 7 OR x.page_no % 511239 = 0 THEN 'BCM'
						    WHEN x.page_no IS NOT NULL THEN '*'
						    ELSE NULL
					    END,
					    x.hobt_id,
					    x.allocation_unit_id,
					    x.index_id,
					    x.schema_id,
					    x.principal_id,
					    x.request_mode,
					    x.request_status,
					    x.session_id,
					    x.request_id,
					    CASE
						    WHEN COALESCE(x.object_id, x.file_id, x.hobt_id, x.allocation_unit_id, x.index_id, x.schema_id, x.principal_id) IS NULL THEN NULLIF(resource_description, '')
						    ELSE NULL
					    END
			    ) AS y ON
				    y.session_id = s.session_id
				    AND y.request_id = s.request_id
			    OPTION (HASH GROUP);

			    --Disable unnecessary autostats on the table
			    CREATE STATISTICS s_database_name ON #locks (database_name)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_object_id ON #locks (object_id)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_hobt_id ON #locks (hobt_id)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_allocation_unit_id ON #locks (allocation_unit_id)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_index_id ON #locks (index_id)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_schema_id ON #locks (schema_id)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_principal_id ON #locks (principal_id)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_request_id ON #locks (request_id)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_start_time ON #locks (start_time)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_resource_type ON #locks (resource_type)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_object_name ON #locks (object_name)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_schema_name ON #locks (schema_name)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_page_type ON #locks (page_type)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_request_mode ON #locks (request_mode)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_request_status ON #locks (request_status)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_resource_description ON #locks (resource_description)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_index_name ON #locks (index_name)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_principal_name ON #locks (principal_name)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
		    END;
		
		    DECLARE 
			    @sql VARCHAR(MAX), 
			    @sql_n NVARCHAR(MAX);

		    SET @sql = 
			    CONVERT(VARCHAR(MAX), '') +
			    'DECLARE @blocker BIT; ' +
			    'SET @blocker = 0; ' +
			    'DECLARE @i INT; ' +
			    'SET @i = 2147483647; ' +
			    '' +
			    'DECLARE @sessions TABLE ' +
			    '( ' +
				    'session_id SMALLINT NOT NULL, ' +
				    'request_id INT NOT NULL, ' +
				    'login_time DATETIME, ' +
				    'last_request_end_time DATETIME, ' +
				    'status VARCHAR(30), ' +
				    'statement_start_offset INT, ' +
				    'statement_end_offset INT, ' +
				    'sql_handle BINARY(20), ' +
				    'host_name NVARCHAR(128), ' +
				    'login_name NVARCHAR(128), ' +
				    'program_name NVARCHAR(128), ' +
				    'database_id SMALLINT, ' +
				    'memory_usage INT, ' +
				    'open_tran_count SMALLINT, ' +
				    CASE
					    WHEN 
					    (
						    @get_task_info <> 0 
						    OR @find_block_leaders = 1 
					    ) THEN
						    'wait_type NVARCHAR(32), ' +
						    'wait_resource NVARCHAR(256), ' +
						    'wait_time BIGINT, '
					    ELSE ''
				    END +
				    'blocked SMALLINT, ' +
				    'is_user_process BIT, ' +
				    'cmd VARCHAR(32), ' +
				    'PRIMARY KEY CLUSTERED (session_id, request_id) WITH (IGNORE_DUP_KEY = ON) ' +
			    '); ' +
			    '' +
			    'DECLARE @blockers TABLE ' +
			    '( ' +
				    'session_id INT NOT NULL PRIMARY KEY ' +
			    '); ' +
			    '' +
			    'BLOCKERS:; ' +
			    '' +
			    'INSERT @sessions ' +
			    '( ' +
				    'session_id, ' +
				    'request_id, ' +
				    'login_time, ' +
				    'last_request_end_time, ' +
				    'status, ' +
				    'statement_start_offset, ' +
				    'statement_end_offset, ' +
				    'sql_handle, ' +
				    'host_name, ' +
				    'login_name, ' +
				    'program_name, ' +
				    'database_id, ' +
				    'memory_usage, ' +
				    'open_tran_count, ' +
				    CASE
					    WHEN 
					    (
						    @get_task_info <> 0
						    OR @find_block_leaders = 1 
					    ) THEN
						    'wait_type, ' +
						    'wait_resource, ' +
						    'wait_time, '
					    ELSE
						    ''
				    END +
				    'blocked, ' +
				    'is_user_process, ' +
				    'cmd ' +
			    ') ' +
			    'SELECT TOP(@i) ' +
				    'spy.session_id, ' +
				    'spy.request_id, ' +
				    'spy.login_time, ' +
				    'spy.last_request_end_time, ' +
				    'spy.status, ' +
				    'spy.statement_start_offset, ' +
				    'spy.statement_end_offset, ' +
				    'spy.sql_handle, ' +
				    'spy.host_name, ' +
				    'spy.login_name, ' +
				    'spy.program_name, ' +
				    'spy.database_id, ' +
				    'spy.memory_usage, ' +
				    'spy.open_tran_count, ' +
				    CASE
					    WHEN 
					    (
						    @get_task_info <> 0  
						    OR @find_block_leaders = 1 
					    ) THEN
						    'spy.wait_type, ' +
						    'CASE ' +
							    'WHEN ' +
								    'spy.wait_type LIKE N''PAGE%LATCH_%'' ' +
								    'OR spy.wait_type = N''CXPACKET'' ' +
								    'OR spy.wait_type LIKE N''LATCH[_]%'' ' +
								    'OR spy.wait_type = N''OLEDB'' THEN ' +
									    'spy.wait_resource ' +
							    'ELSE ' +
								    'NULL ' +
						    'END AS wait_resource, ' +
						    'spy.wait_time, '
					    ELSE ''
				    END +
				    'spy.blocked, ' +
				    'spy.is_user_process, ' +
				    'spy.cmd ' +
			    'FROM ' +
			    '( ' +
				    'SELECT TOP(@i) ' +
					    'spx.*, ' +
					    CASE
						    WHEN 
						    (
							    @get_task_info <> 0 
							    OR @find_block_leaders = 1 
						    ) THEN
							    'ROW_NUMBER() OVER ' +
							    '( ' +
								    'PARTITION BY ' +
									    'spx.session_id, ' +
									    'spx.request_id ' +
								    'ORDER BY ' +
									    'CASE ' +
										    'WHEN spx.wait_type LIKE N''LCK[_]%'' THEN 1 ' +
										    'ELSE 99 ' +
									    'END, ' +
									    'spx.wait_time DESC, ' +
									    'spx.blocked DESC ' +
							    ') AS r '
						    ELSE '1 AS r '
					    END +
				    'FROM ' +
				    '( ' +
					    'SELECT TOP(@i) ' +
						    'sp0.session_id, ' +
						    'sp0.request_id, ' +
						    'sp0.login_time, ' +
						    'sp0.last_request_end_time, ' +
						    'LOWER(sp0.status) AS status, ' +
						    'CASE ' +
							    'WHEN sp0.cmd = ''CREATE INDEX'' THEN 0 ' +
							    'ELSE sp0.stmt_start ' +
						    'END AS statement_start_offset, ' +
						    'CASE ' +
							    'WHEN sp0.cmd = N''CREATE INDEX'' THEN -1 ' +
							    'ELSE COALESCE(NULLIF(sp0.stmt_end, 0), -1) ' +
						    'END AS statement_end_offset, ' +
						    'sp0.sql_handle, ' +
						    'sp0.host_name, ' +
						    'sp0.login_name, ' +
						    'sp0.program_name, ' +
						    'sp0.database_id, ' +
						    'sp0.memory_usage, ' +
						    'sp0.open_tran_count, ' +
						    CASE
							    WHEN 
							    (
								    @get_task_info <> 0 
								    OR @find_block_leaders = 1 
							    ) THEN
								    'CASE ' +
									    'WHEN sp0.wait_time > 0 AND sp0.wait_type <> N''CXPACKET'' THEN sp0.wait_type ' +
									    'ELSE NULL ' +
								    'END AS wait_type, ' +
								    'CASE ' +
									    'WHEN sp0.wait_time > 0 AND sp0.wait_type <> N''CXPACKET'' THEN sp0.wait_resource ' +
									    'ELSE NULL ' +
								    'END AS wait_resource, ' +
								    'CASE ' +
									    'WHEN sp0.wait_type <> N''CXPACKET'' THEN sp0.wait_time ' +
									    'ELSE 0 ' +
								    'END AS wait_time, '
							    ELSE ''
						    END +
						    'sp0.blocked, ' +
						    'sp0.is_user_process, ' +
						    'sp0.cmd ' +
					    'FROM ' +
					    '( ' +
						    'SELECT TOP(@i) ' +
							    'sp1.session_id, ' +
							    'sp1.request_id, ' +
							    'sp1.login_time, ' +
							    'sp1.last_request_end_time, ' +
							    'sp1.status, ' +
							    'sp1.cmd, ' +
							    'sp1.stmt_start, ' +
							    'sp1.stmt_end, ' +
							    'MAX(NULLIF(sp1.sql_handle, 0x00)) OVER (PARTITION BY sp1.session_id, sp1.request_id) AS sql_handle, ' +
							    'sp1.host_name, ' +
							    'MAX(sp1.login_name) OVER (PARTITION BY sp1.session_id, sp1.request_id) AS login_name, ' +
							    'sp1.program_name, ' +
							    'sp1.database_id, ' +
							    'MAX(sp1.memory_usage)  OVER (PARTITION BY sp1.session_id, sp1.request_id) AS memory_usage, ' +
							    'MAX(sp1.open_tran_count)  OVER (PARTITION BY sp1.session_id, sp1.request_id) AS open_tran_count, ' +
							    'sp1.wait_type, ' +
							    'sp1.wait_resource, ' +
							    'sp1.wait_time, ' +
							    'sp1.blocked, ' +
							    'sp1.hostprocess, ' +
							    'sp1.is_user_process ' +
						    'FROM ' +
						    '( ' +
							    'SELECT TOP(@i) ' +
								    'sp2.spid AS session_id, ' +
								    'CASE sp2.status ' +
									    'WHEN ''sleeping'' THEN CONVERT(INT, 0) ' +
									    'ELSE sp2.request_id ' +
								    'END AS request_id, ' +
								    'MAX(sp2.login_time) AS login_time, ' +
								    'MAX(sp2.last_batch) AS last_request_end_time, ' +
								    'MAX(CONVERT(VARCHAR(30), RTRIM(sp2.status)) COLLATE Latin1_General_Bin2) AS status, ' +
								    'MAX(CONVERT(VARCHAR(32), RTRIM(sp2.cmd)) COLLATE Latin1_General_Bin2) AS cmd, ' +
								    'MAX(sp2.stmt_start) AS stmt_start, ' +
								    'MAX(sp2.stmt_end) AS stmt_end, ' +
								    'MAX(sp2.sql_handle) AS sql_handle, ' +
								    'MAX(CONVERT(sysname, RTRIM(sp2.hostname)) COLLATE SQL_Latin1_General_CP1_CI_AS) AS host_name, ' +
								    'MAX(CONVERT(sysname, RTRIM(sp2.loginame)) COLLATE SQL_Latin1_General_CP1_CI_AS) AS login_name, ' +
								    'MAX ' +
								    '( ' +
									    'CASE ' +
										    'WHEN blk.queue_id IS NOT NULL THEN ' + 
											    'N''Service Broker ' +
												    'database_id: '' + CONVERT(NVARCHAR, blk.database_id) + ' +
												    'N'' queue_id: '' + CONVERT(NVARCHAR, blk.queue_id)' +
										    'ELSE ' +
											    'CONVERT ' +
											    '( ' +
												    'sysname, ' +
												    'RTRIM(sp2.program_name) ' +
											    ') ' +
									    'END COLLATE SQL_Latin1_General_CP1_CI_AS ' +
								    ') AS program_name, ' +
								    'MAX(sp2.dbid) AS database_id, ' +
								    'MAX(sp2.memusage) AS memory_usage, ' +
								    'MAX(sp2.open_tran) AS open_tran_count, ' +
								    'RTRIM(sp2.lastwaittype) AS wait_type, ' +
								    'RTRIM(sp2.waitresource) AS wait_resource, ' +
								    'MAX(sp2.waittime) AS wait_time, ' +
								    'COALESCE(NULLIF(sp2.blocked, sp2.spid), 0) AS blocked, ' +
								    'MAX ' +
								    '( ' +
									    'CASE ' +
										    'WHEN blk.session_id = sp2.spid THEN ' +
											    '''blocker'' ' +
										    'ELSE ' +
											    'RTRIM(sp2.hostprocess) ' +
									    'END ' +
								    ') AS hostprocess, ' +
								    'CONVERT ' +
								    '( ' +
									    'BIT, ' +
									    'MAX ' +
									    '( ' +
										    'CASE ' +
											    'WHEN sp2.hostprocess > '''' THEN ' +
												    '1 ' +
											    'ELSE ' +
												    '0 ' +
										    'END ' +
									    ') ' +
								    ') AS is_user_process ' +
							    'FROM ' +
							    '( ' +
								    'SELECT TOP(@i) ' +
									    'session_id, ' +
									    'CONVERT(INT, NULL) AS queue_id, ' +
									    'CONVERT(INT, NULL) AS database_id ' +
								    'FROM @blockers ' +
								    '' +
								    'UNION ALL ' +
								    '' +
								    'SELECT TOP(@i) ' +
									    'CONVERT(SMALLINT, 0), ' +
									    'CONVERT(INT, NULL) AS queue_id, ' +
									    'CONVERT(INT, NULL) AS database_id ' +
								    'WHERE ' +
									    '@blocker = 0 ' +
								    '' +
								    'UNION ALL ' +
								    '' +
								    'SELECT TOP(@i) ' +
									    'CONVERT(SMALLINT, spid), ' +
									    'queue_id, ' +
									    'database_id ' +
								    'FROM sys.dm_broker_activated_tasks ' +
								    'WHERE ' +
									    '@blocker = 0 ' +
							    ') AS blk ' +
							    'INNER JOIN sys.sysprocesses AS sp2 ON ' +
								    'sp2.spid = blk.session_id ' +
								    'OR ' +
								    '( ' +
									    'blk.session_id = 0 ' +
									    'AND @blocker = 0 ' +
								    ') ' +
							    CASE 
								    WHEN 
								    (
									    @get_task_info = 0 
									    AND @find_block_leaders = 0
								    ) THEN
									    'WHERE ' +
										    'sp2.ecid = 0 ' 
								    ELSE ''
							    END +
							    'GROUP BY ' +
								    'sp2.spid, ' +
								    'CASE sp2.status ' +
									    'WHEN ''sleeping'' THEN CONVERT(INT, 0) ' +
									    'ELSE sp2.request_id ' +
								    'END, ' +
								    'RTRIM(sp2.lastwaittype), ' +
								    'RTRIM(sp2.waitresource), ' +
								    'COALESCE(NULLIF(sp2.blocked, sp2.spid), 0) ' +
						    ') AS sp1 ' +
					    ') AS sp0 ' +
					    'WHERE ' +
						    '@blocker = 1 ' +
						    'OR ' +
						    '(1=1 ' +
							    --inclusive filter
							    CASE
								    WHEN @filter <> '' THEN
									    CASE @filter_type
										    WHEN 'session' THEN
											    CASE
												    WHEN CONVERT(SMALLINT, @filter) <> 0 THEN
													    'AND sp0.session_id = CONVERT(SMALLINT, @filter) '
												    ELSE ''
											    END
										    WHEN 'program' THEN
											    'AND sp0.program_name LIKE @filter '
										    WHEN 'login' THEN
											    'AND sp0.login_name LIKE @filter '
										    WHEN 'host' THEN
											    'AND sp0.host_name LIKE @filter '
										    WHEN 'database' THEN
											    'AND DB_NAME(sp0.database_id) LIKE @filter '
										    ELSE ''
									    END
								    ELSE ''
							    END +
							    --exclusive filter
							    CASE
								    WHEN @not_filter <> '' THEN
									    CASE @not_filter_type
										    WHEN 'session' THEN
											    CASE
												    WHEN CONVERT(SMALLINT, @not_filter) <> 0 THEN
													    'AND sp0.session_id <> CONVERT(SMALLINT, @not_filter) '
												    ELSE ''
											    END
										    WHEN 'program' THEN
											    'AND sp0.program_name NOT LIKE @not_filter '
										    WHEN 'login' THEN
											    'AND sp0.login_name NOT LIKE @not_filter '
										    WHEN 'host' THEN
											    'AND sp0.host_name NOT LIKE @not_filter '
										    WHEN 'database' THEN
											    'AND DB_NAME(sp0.database_id) NOT LIKE @not_filter '
										    ELSE ''
									    END
								    ELSE ''
							    END +
							    CASE @show_own_spid
								    WHEN 1 THEN ''
								    ELSE
									    'AND sp0.session_id <> @@spid '
							    END +
							    CASE 
								    WHEN @show_system_spids = 0 THEN
									    'AND sp0.hostprocess > '''' ' 
								    ELSE ''
							    END +
							    CASE @show_sleeping_spids
								    WHEN 0 THEN
									    'AND sp0.status <> ''sleeping'' '
								    WHEN 1 THEN
									    'AND ' +
									    '( ' +
										    'sp0.status <> ''sleeping'' ' +
										    'OR sp0.open_tran_count > 0 ' +
									    ') '
								    ELSE ''
							    END +
						    ') ' +
				    ') AS spx ' +
			    ') AS spy ' +
			    'WHERE ' +
				    'spy.r = 1; ' + 
			    CASE @recursion
				    WHEN 1 THEN 
					    'IF @@ROWCOUNT > 0 ' +
					    'BEGIN; ' +
						    'INSERT @blockers ' +
						    '( ' +
							    'session_id ' +
						    ') ' +
						    'SELECT TOP(@i) ' +
							    'blocked ' +
						    'FROM @sessions ' +
						    'WHERE ' +
							    'NULLIF(blocked, 0) IS NOT NULL ' +
						    '' +
						    'EXCEPT ' +
						    '' +
						    'SELECT TOP(@i) ' +
							    'session_id ' +
						    'FROM @sessions; ' +
						    '' +
						    CASE
							    WHEN
							    (
								    @get_task_info > 0
								    OR @find_block_leaders = 1
							    ) THEN
								    'IF @@ROWCOUNT > 0 ' +
								    'BEGIN; ' +
									    'SET @blocker = 1; ' +
									    'GOTO BLOCKERS; ' +
								    'END; '
							    ELSE ''
						    END +
					    'END; '
				    ELSE ''
			    END +
			    'SELECT TOP(@i) ' +
				    '@recursion AS recursion, ' +
				    'x.session_id, ' +
				    'x.request_id, ' +
				    'DENSE_RANK() OVER  ' +
				    '( ' +
					    'ORDER BY ' +
						    'x.session_id ' +
				    ') AS session_number, ' +
				    CASE
					    WHEN @output_column_list LIKE '%|[dd hh:mm:ss.mss|]%' ESCAPE '|' THEN 'x.elapsed_time '
					    ELSE '0 '
				    END + 'AS elapsed_time, ' +
				    CASE
					    WHEN
						    (
							    @output_column_list LIKE '%|[dd hh:mm:ss.mss (avg)|]%' ESCAPE '|' OR 
							    @output_column_list LIKE '%|[avg_elapsed_time|]%' ESCAPE '|'
						    )
						    AND @recursion = 1
							    THEN 'x.avg_elapsed_time / 1000 '
					    ELSE 'NULL '
				    END + 'AS avg_elapsed_time, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[physical_io|]%' ESCAPE '|'
						    OR @output_column_list LIKE '%|[physical_io_delta|]%' ESCAPE '|'
							    THEN 'x.physical_io '
					    ELSE 'NULL '
				    END + 'AS physical_io, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[reads|]%' ESCAPE '|'
						    OR @output_column_list LIKE '%|[reads_delta|]%' ESCAPE '|'
							    THEN 'x.reads '
					    ELSE '0 '
				    END + 'AS reads, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[physical_reads|]%' ESCAPE '|'
						    OR @output_column_list LIKE '%|[physical_reads_delta|]%' ESCAPE '|'
							    THEN 'x.physical_reads '
					    ELSE '0 '
				    END + 'AS physical_reads, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[writes|]%' ESCAPE '|'
						    OR @output_column_list LIKE '%|[writes_delta|]%' ESCAPE '|'
							    THEN 'x.writes '
					    ELSE '0 '
				    END + 'AS writes, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[tempdb_allocations|]%' ESCAPE '|'
						    OR @output_column_list LIKE '%|[tempdb_allocations_delta|]%' ESCAPE '|'
							    THEN 'x.tempdb_allocations '
					    ELSE '0 '
				    END + 'AS tempdb_allocations, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[tempdb_current|]%' ESCAPE '|'
						    OR @output_column_list LIKE '%|[tempdb_current_delta|]%' ESCAPE '|'
							    THEN 'x.tempdb_current '
					    ELSE '0 '
				    END + 'AS tempdb_current, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[CPU|]%' ESCAPE '|'
						    OR @output_column_list LIKE '%|[CPU_delta|]%' ESCAPE '|'
							    THEN 'x.CPU '
					    ELSE '0 '
				    END + 'AS CPU, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[CPU_delta|]%' ESCAPE '|'
						    AND @get_task_info = 2
							    THEN 'x.thread_CPU_snapshot '
					    ELSE '0 '
				    END + 'AS thread_CPU_snapshot, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[context_switches|]%' ESCAPE '|'
						    OR @output_column_list LIKE '%|[context_switches_delta|]%' ESCAPE '|'
							    THEN 'x.context_switches '
					    ELSE 'NULL '
				    END + 'AS context_switches, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[used_memory|]%' ESCAPE '|'
						    OR @output_column_list LIKE '%|[used_memory_delta|]%' ESCAPE '|'
							    THEN 'x.used_memory '
					    ELSE '0 '
				    END + 'AS used_memory, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[tasks|]%' ESCAPE '|'
						    AND @recursion = 1
							    THEN 'x.tasks '
					    ELSE 'NULL '
				    END + 'AS tasks, ' +
				    CASE
					    WHEN 
						    (
							    @output_column_list LIKE '%|[status|]%' ESCAPE '|' 
							    OR @output_column_list LIKE '%|[sql_command|]%' ESCAPE '|'
						    )
						    AND @recursion = 1
							    THEN 'x.status '
					    ELSE ''''' '
				    END + 'AS status, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[wait_info|]%' ESCAPE '|' 
						    AND @recursion = 1
							    THEN 
								    CASE @get_task_info
									    WHEN 2 THEN 'COALESCE(x.task_wait_info, x.sys_wait_info) '
									    ELSE 'x.sys_wait_info '
								    END
					    ELSE 'NULL '
				    END + 'AS wait_info, ' +
				    CASE
					    WHEN 
						    (
							    @output_column_list LIKE '%|[tran_start_time|]%' ESCAPE '|' 
							    OR @output_column_list LIKE '%|[tran_log_writes|]%' ESCAPE '|' 
						    )
						    AND @recursion = 1
							    THEN 
							    'x.transaction_id '
					    ELSE 'NULL '
				    END + 'AS transaction_id, ' +					
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[open_tran_count|]%' ESCAPE '|' 
						    AND @recursion = 1
							    THEN 'x.open_tran_count '
					    ELSE 'NULL '
				    END + 'AS open_tran_count, ' + 
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[sql_text|]%' ESCAPE '|' 
						    AND @recursion = 1
							    THEN 'x.sql_handle '
					    ELSE 'NULL '
				    END + 'AS sql_handle, ' +
				    CASE
					    WHEN 
						    (
							    @output_column_list LIKE '%|[sql_text|]%' ESCAPE '|' 
							    OR @output_column_list LIKE '%|[query_plan|]%' ESCAPE '|' 
						    )
						    AND @recursion = 1
							    THEN 'x.statement_start_offset '
					    ELSE 'NULL '
				    END + 'AS statement_start_offset, ' +
				    CASE
					    WHEN 
						    (
							    @output_column_list LIKE '%|[sql_text|]%' ESCAPE '|' 
							    OR @output_column_list LIKE '%|[query_plan|]%' ESCAPE '|' 
						    )
						    AND @recursion = 1
							    THEN 'x.statement_end_offset '
					    ELSE 'NULL '
				    END + 'AS statement_end_offset, ' +
				    'NULL AS sql_text, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[query_plan|]%' ESCAPE '|' 
						    AND @recursion = 1
							    THEN 'x.plan_handle '
					    ELSE 'NULL '
				    END + 'AS plan_handle, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[blocking_session_id|]%' ESCAPE '|' 
						    AND @recursion = 1
							    THEN 'NULLIF(x.blocking_session_id, 0) '
					    ELSE 'NULL '
				    END + 'AS blocking_session_id, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[percent_complete|]%' ESCAPE '|'
						    AND @recursion = 1
							    THEN 'x.percent_complete '
					    ELSE 'NULL '
				    END + 'AS percent_complete, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[host_name|]%' ESCAPE '|' 
						    AND @recursion = 1
							    THEN 'x.host_name '
					    ELSE ''''' '
				    END + 'AS host_name, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[login_name|]%' ESCAPE '|' 
						    AND @recursion = 1
							    THEN 'x.login_name '
					    ELSE ''''' '
				    END + 'AS login_name, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[database_name|]%' ESCAPE '|' 
						    AND @recursion = 1
							    THEN 'DB_NAME(x.database_id) '
					    ELSE 'NULL '
				    END + 'AS database_name, ' +
				    CASE
					    WHEN 
						    @output_column_list LIKE '%|[program_name|]%' ESCAPE '|' 
						    AND @recursion = 1
							    THEN 'x.program_name '
					    ELSE ''''' '
				    END + 'AS program_name, ' +
				    CASE
					    WHEN
						    @output_column_list LIKE '%|[additional_info|]%' ESCAPE '|'
						    AND @recursion = 1
							    THEN
								    '( ' +
									    'SELECT TOP(@i) ' +
										    'text_size, ' +
										    'language, ' +
										    'date_format, ' +
										    'date_first, ' +
										    'CASE quoted_identifier ' +
											    'WHEN 0 THEN ''OFF'' ' +
											    'WHEN 1 THEN ''ON'' ' +
										    'END AS quoted_identifier, ' +
										    'CASE arithabort ' +
											    'WHEN 0 THEN ''OFF'' ' +
											    'WHEN 1 THEN ''ON'' ' +
										    'END AS arithabort, ' +
										    'CASE ansi_null_dflt_on ' +
											    'WHEN 0 THEN ''OFF'' ' +
											    'WHEN 1 THEN ''ON'' ' +
										    'END AS ansi_null_dflt_on, ' +
										    'CASE ansi_defaults ' +
											    'WHEN 0 THEN ''OFF'' ' +
											    'WHEN 1 THEN ''ON'' ' +
										    'END AS ansi_defaults, ' +
										    'CASE ansi_warnings ' +
											    'WHEN 0 THEN ''OFF'' ' +
											    'WHEN 1 THEN ''ON'' ' +
										    'END AS ansi_warnings, ' +
										    'CASE ansi_padding ' +
											    'WHEN 0 THEN ''OFF'' ' +
											    'WHEN 1 THEN ''ON'' ' +
										    'END AS ansi_padding, ' +
										    'CASE ansi_nulls ' +
											    'WHEN 0 THEN ''OFF'' ' +
											    'WHEN 1 THEN ''ON'' ' +
										    'END AS ansi_nulls, ' +
										    'CASE concat_null_yields_null ' +
											    'WHEN 0 THEN ''OFF'' ' +
											    'WHEN 1 THEN ''ON'' ' +
										    'END AS concat_null_yields_null, ' +
										    'CASE transaction_isolation_level ' +
											    'WHEN 0 THEN ''Unspecified'' ' +
											    'WHEN 1 THEN ''ReadUncomitted'' ' +
											    'WHEN 2 THEN ''ReadCommitted'' ' +
											    'WHEN 3 THEN ''Repeatable'' ' +
											    'WHEN 4 THEN ''Serializable'' ' +
											    'WHEN 5 THEN ''Snapshot'' ' +
										    'END AS transaction_isolation_level, ' +
										    'lock_timeout, ' +
										    'deadlock_priority, ' +
										    'row_count, ' +
										    'command_type, ' +
										    CASE
											    WHEN @output_column_list LIKE '%|[program_name|]%' ESCAPE '|' THEN
												    '( ' +
													    'SELECT TOP(1) ' +
														    'CONVERT(uniqueidentifier, CONVERT(XML, '''').value(''xs:hexBinary( substring(sql:column("agent_info.job_id_string"), 0) )'', ''binary(16)'')) AS job_id, ' +
														    'agent_info.step_id, ' +
														    '( ' +
															    'SELECT TOP(1) ' +
																    'NULL ' +
															    'FOR XML ' +
																    'PATH(''job_name''), ' +
																    'TYPE ' +
														    '), ' +
														    '( ' +
															    'SELECT TOP(1) ' +
																    'NULL ' +
															    'FOR XML ' +
																    'PATH(''step_name''), ' +
																    'TYPE ' +
														    ') ' +
													    'FROM ' +
													    '( ' +
														    'SELECT TOP(1) ' +
															    'SUBSTRING(x.program_name, CHARINDEX(''0x'', x.program_name) + 2, 32) AS job_id_string, ' +
															    'SUBSTRING(x.program_name, CHARINDEX('': Step '', x.program_name) + 7, CHARINDEX('')'', x.program_name, CHARINDEX('': Step '', x.program_name)) - (CHARINDEX('': Step '', x.program_name) + 7)) AS step_id ' +
														    'WHERE '+
															    'x.program_name LIKE N''SQLAgent - TSQL JobStep (Job 0x%'' ' +
													    ') AS agent_info ' +
													    'FOR XML ' +
														    'PATH(''agent_job_info''), ' +
														    'TYPE ' +
												    ') '
											    ELSE ''
										    END +
										    CASE
											    WHEN @get_task_info = 2 THEN
												    ', CONVERT(XML, x.block_info) AS block_info '
											    ELSE
												    ''
										    END +
									    'FOR XML ' +
										    'PATH(''additional_info''), ' +
										    'TYPE ' +
								    ') '
					    ELSE 'NULL '
				    END + 'AS additional_info, ' +
				    'x.start_time, ' +
				    CASE
					    WHEN
						    @output_column_list LIKE '%|[login_time|]%' ESCAPE '|'
						    AND @recursion = 1
							    THEN
								    'x.login_time '
					    ELSE 'NULL '
				    END + 'AS login_time, ' +
				    'x.last_request_start_time ' +
			    'FROM ' +
			    '( ' +
				    'SELECT TOP(@i) ' +
					    'y.*, ' +
					    'CASE ' +
						    --if there are more than 24 days, return a negative number of seconds rather than
						    --positive milliseconds, in order to avoid overflow errors
						    'WHEN DATEDIFF(day, y.start_time, GETDATE()) > 24 THEN ' +
							    'DATEDIFF(second, GETDATE(), y.start_time) ' +
						    'ELSE DATEDIFF(ms, y.start_time, GETDATE()) ' +
					    'END AS elapsed_time, ' +
					    'COALESCE(tempdb_info.tempdb_allocations, 0) AS tempdb_allocations, ' +
					    'COALESCE ' +
					    '( ' +
						    'CASE ' +
							    'WHEN tempdb_info.tempdb_current < 0 THEN 0 ' +
							    'ELSE tempdb_info.tempdb_current ' + 
						    'END, ' +
						    '0 ' +
					    ') AS tempdb_current, ' +
					    CASE
						    WHEN 
							    (
								    @get_task_info <> 0
								    OR @find_block_leaders = 1
							    ) THEN
								    'N''('' + CONVERT(NVARCHAR, y.wait_duration_ms) + N''ms)'' + ' +
									    'y.wait_type + ' +
										    --TODO: What else can be pulled from the resource_description?
										    'CASE ' +
											    'WHEN y.wait_type LIKE N''PAGE%LATCH_%'' THEN ' +
												    'N'':'' + ' +
												    --database name
												    'COALESCE(DB_NAME(CONVERT(INT, LEFT(y.resource_description, CHARINDEX(N'':'', y.resource_description) - 1))), N''(null)'') + ' +
												    'N'':'' + ' +
												    --file id
												    'SUBSTRING(y.resource_description, CHARINDEX(N'':'', y.resource_description) + 1, LEN(y.resource_description) - CHARINDEX(N'':'', REVERSE(y.resource_description)) - CHARINDEX(N'':'', y.resource_description)) + ' +
												    --page # for special pages
												    'N''('' + ' +
													    'CASE ' +
														    'WHEN ' +
															    'CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) = 1 OR ' +
															    'CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) % 8088 = 0 THEN N''PFS'' ' +
														    'WHEN ' +
															    'CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) = 2 OR ' +
															    'CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) % 511232 = 0 THEN N''GAM'' ' +
														    'WHEN ' +
															    'CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) = 3 OR ' +
															    'CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) % 511233 = 0 THEN N''SGAM'' ' +
														    'WHEN ' +
															    'CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) = 6 OR ' +
															    'CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) % 511238 = 0 THEN N''DCM'' ' +
														    'WHEN ' +
															    'CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) = 7 OR ' +
															    'CONVERT(INT, RIGHT(y.resource_description, CHARINDEX(N'':'', REVERSE(y.resource_description)) - 1)) % 511239 = 0 THEN N''BCM'' ' +
														    'ELSE N''*'' ' +
													    'END + ' +
												    'N'')'' ' +
											    'WHEN y.wait_type = N''CXPACKET'' THEN ' +
												    'N'':'' + SUBSTRING(y.resource_description, CHARINDEX(N''nodeId'', y.resource_description) + 7, 4)' +
											    'WHEN y.wait_type LIKE N''LATCH[_]%'' THEN ' +
												    'N'' ['' + LEFT(y.resource_description, COALESCE(NULLIF(CHARINDEX(N'' '', y.resource_description), 0), LEN(y.resource_description) + 1) - 1) + N'']'' ' +
											    'WHEN ' +
												    'y.wait_type = N''OLEDB'' ' +
												    'AND y.resource_description LIKE N''%(SPID=%)'' THEN ' +
													    'N''['' + LEFT(y.resource_description, CHARINDEX(N''(SPID='', y.resource_description) - 2) + ' +
														    'N'':'' + SUBSTRING(y.resource_description, CHARINDEX(N''(SPID='', y.resource_description) + 6, CHARINDEX(N'')'', y.resource_description, (CHARINDEX(N''(SPID='', y.resource_description) + 6)) - (CHARINDEX(N''(SPID='', y.resource_description) + 6)) + '']'' ' +
											    'ELSE N'''' ' +
										    'END COLLATE Latin1_General_Bin2 AS sys_wait_info, '
						    ELSE
							    ''
						    END +
						    CASE
							    WHEN @get_task_info = 2 THEN
								    'tasks.physical_io, ' +
								    'tasks.context_switches, ' + 
								    'tasks.tasks, ' +
								    'tasks.block_info, ' +
								    'tasks.wait_info AS task_wait_info, ' +
								    'tasks.thread_CPU_snapshot, '
						    ELSE
							    '' 
					    END +
					    CASE 
						    WHEN NOT (@get_avg_time = 1 AND @recursion = 1) THEN 'CONVERT(INT, NULL) '
						    ELSE 'qs.total_elapsed_time / qs.execution_count '
					    END + 'AS avg_elapsed_time ' +
				    'FROM ' +
				    '( ' +
					    'SELECT TOP(@i) ' +
						    'sp.session_id, ' +
						    'sp.request_id, ' +
						    'COALESCE(r.logical_reads, s.logical_reads) AS reads, ' +
						    'COALESCE(r.reads, s.reads) AS physical_reads, ' +
						    'COALESCE(r.writes, s.writes) AS writes, ' +
						    'COALESCE(r.CPU_time, s.CPU_time) AS CPU, ' +
						    'sp.memory_usage + COALESCE(r.granted_query_memory, 0) AS used_memory, ' +
						    'LOWER(sp.status) AS status, ' +
						    'COALESCE(r.sql_handle, sp.sql_handle) AS sql_handle, ' +
						    'COALESCE(r.statement_start_offset, sp.statement_start_offset) AS statement_start_offset, ' +
						    'COALESCE(r.statement_end_offset, sp.statement_end_offset) AS statement_end_offset, ' +
						    CASE
							    WHEN 
							    (
								    @get_task_info <> 0
								    OR @find_block_leaders = 1 
							    ) THEN
								    'sp.wait_type COLLATE Latin1_General_Bin2 AS wait_type, ' +
								    'sp.wait_resource COLLATE Latin1_General_Bin2 AS resource_description, ' +
								    'sp.wait_time AS wait_duration_ms, '
							    ELSE ''
						    END +
						    'NULLIF(sp.blocked, 0) AS blocking_session_id, ' +
						    'r.plan_handle, ' +
						    'NULLIF(r.percent_complete, 0) AS percent_complete, ' +
						    'sp.host_name, ' +
						    'sp.login_name, ' +
						    'sp.program_name, ' +
						    'COALESCE(r.text_size, s.text_size) AS text_size, ' +
						    'COALESCE(r.language, s.language) AS language, ' +
						    'COALESCE(r.date_format, s.date_format) AS date_format, ' +
						    'COALESCE(r.date_first, s.date_first) AS date_first, ' +
						    'COALESCE(r.quoted_identifier, s.quoted_identifier) AS quoted_identifier, ' +
						    'COALESCE(r.arithabort, s.arithabort) AS arithabort, ' +
						    'COALESCE(r.ansi_null_dflt_on, s.ansi_null_dflt_on) AS ansi_null_dflt_on, ' +
						    'COALESCE(r.ansi_defaults, s.ansi_defaults) AS ansi_defaults, ' +
						    'COALESCE(r.ansi_warnings, s.ansi_warnings) AS ansi_warnings, ' +
						    'COALESCE(r.ansi_padding, s.ansi_padding) AS ansi_padding, ' +
						    'COALESCE(r.ansi_nulls, s.ansi_nulls) AS ansi_nulls, ' +
						    'COALESCE(r.concat_null_yields_null, s.concat_null_yields_null) AS concat_null_yields_null, ' +
						    'COALESCE(r.transaction_isolation_level, s.transaction_isolation_level) AS transaction_isolation_level, ' +
						    'COALESCE(r.lock_timeout, s.lock_timeout) AS lock_timeout, ' +
						    'COALESCE(r.deadlock_priority, s.deadlock_priority) AS deadlock_priority, ' +
						    'COALESCE(r.row_count, s.row_count) AS row_count, ' +
						    'COALESCE(r.command, sp.cmd) AS command_type, ' +
						    'COALESCE ' +
						    '( ' +
							    'CASE ' +
								    'WHEN ' +
								    '( ' +
									    's.is_user_process = 0 ' +
									    'AND r.total_elapsed_time >= 0 ' +
								    ') THEN ' +
									    'DATEADD ' +
									    '( ' +
										    'ms, ' +
										    '1000 * (DATEPART(ms, DATEADD(second, -(r.total_elapsed_time / 1000), GETDATE())) / 500) - DATEPART(ms, DATEADD(second, -(r.total_elapsed_time / 1000), GETDATE())), ' +
										    'DATEADD(second, -(r.total_elapsed_time / 1000), GETDATE()) ' +
									    ') ' +
							    'END, ' +
							    'NULLIF(COALESCE(r.start_time, sp.last_request_end_time), CONVERT(DATETIME, ''19000101'', 112)), ' +
							    '( ' +
								    'SELECT TOP(1) ' +
									    'DATEADD(second, -(ms_ticks / 1000), GETDATE()) ' +
								    'FROM sys.dm_os_sys_info ' +
							    ') ' +
						    ') AS start_time, ' +
						    'sp.login_time, ' +
						    'CASE ' +
							    'WHEN s.is_user_process = 1 THEN ' +
								    's.last_request_start_time ' +
							    'ELSE ' +
								    'COALESCE ' +
								    '( ' +
									    'DATEADD ' +
									    '( ' +
										    'ms, ' +
										    '1000 * (DATEPART(ms, DATEADD(second, -(r.total_elapsed_time / 1000), GETDATE())) / 500) - DATEPART(ms, DATEADD(second, -(r.total_elapsed_time / 1000), GETDATE())), ' +
										    'DATEADD(second, -(r.total_elapsed_time / 1000), GETDATE()) ' +
									    '), ' +
									    's.last_request_start_time ' +
								    ') ' +
						    'END AS last_request_start_time, ' +
						    'r.transaction_id, ' +
						    'sp.database_id, ' +
						    'sp.open_tran_count ' +
					    'FROM @sessions AS sp ' +
					    'LEFT OUTER LOOP JOIN sys.dm_exec_sessions AS s ON ' +
						    's.session_id = sp.session_id ' +
						    'AND s.login_time = sp.login_time ' +
					    'LEFT OUTER LOOP JOIN sys.dm_exec_requests AS r ON ' +
						    'sp.status <> ''sleeping'' ' +
						    'AND r.session_id = sp.session_id ' +
						    'AND r.request_id = sp.request_id ' +
						    'AND ' +
						    '( ' +
							    '( ' +
								    's.is_user_process = 0 ' +
								    'AND sp.is_user_process = 0 ' +
							    ') ' +
							    'OR ' +
							    '( ' +
								    'r.start_time = s.last_request_start_time ' +
								    'AND s.last_request_end_time = sp.last_request_end_time ' +
							    ') ' +
						    ') ' +
				    ') AS y ' + 
				    CASE 
					    WHEN @get_task_info = 2 THEN
						    CONVERT(VARCHAR(MAX), '') +
						    'LEFT OUTER HASH JOIN ' +
						    '( ' +
							    'SELECT TOP(@i) ' +
								    'task_nodes.task_node.value(''(session_id/text())[1]'', ''SMALLINT'') AS session_id, ' +
								    'task_nodes.task_node.value(''(request_id/text())[1]'', ''INT'') AS request_id, ' +
								    'task_nodes.task_node.value(''(physical_io/text())[1]'', ''BIGINT'') AS physical_io, ' +
								    'task_nodes.task_node.value(''(context_switches/text())[1]'', ''BIGINT'') AS context_switches, ' +
								    'task_nodes.task_node.value(''(tasks/text())[1]'', ''INT'') AS tasks, ' +
								    'task_nodes.task_node.value(''(block_info/text())[1]'', ''NVARCHAR(4000)'') AS block_info, ' +
								    'task_nodes.task_node.value(''(waits/text())[1]'', ''NVARCHAR(4000)'') AS wait_info, ' +
								    'task_nodes.task_node.value(''(thread_CPU_snapshot/text())[1]'', ''BIGINT'') AS thread_CPU_snapshot ' +
							    'FROM ' +
							    '( ' +
								    'SELECT TOP(@i) ' +
									    'CONVERT ' +
									    '( ' +
										    'XML, ' +
										    'REPLACE ' +
										    '( ' +
											    'CONVERT(NVARCHAR(MAX), tasks_raw.task_xml_raw) COLLATE Latin1_General_Bin2, ' +
											    'N''</waits></tasks><tasks><waits>'', ' +
											    'N'', '' ' +
										    ') ' +
									    ') AS task_xml ' +
								    'FROM ' +
								    '( ' +
									    'SELECT TOP(@i) ' +
										    'CASE waits.r ' +
											    'WHEN 1 THEN waits.session_id ' +
											    'ELSE NULL ' +
										    'END AS [session_id], ' +
										    'CASE waits.r ' +
											    'WHEN 1 THEN waits.request_id ' +
											    'ELSE NULL ' +
										    'END AS [request_id], ' +											
										    'CASE waits.r ' +
											    'WHEN 1 THEN waits.physical_io ' +
											    'ELSE NULL ' +
										    'END AS [physical_io], ' +
										    'CASE waits.r ' +
											    'WHEN 1 THEN waits.context_switches ' +
											    'ELSE NULL ' +
										    'END AS [context_switches], ' +
										    'CASE waits.r ' +
											    'WHEN 1 THEN waits.thread_CPU_snapshot ' +
											    'ELSE NULL ' +
										    'END AS [thread_CPU_snapshot], ' +
										    'CASE waits.r ' +
											    'WHEN 1 THEN waits.tasks ' +
											    'ELSE NULL ' +
										    'END AS [tasks], ' +
										    'CASE waits.r ' +
											    'WHEN 1 THEN waits.block_info ' +
											    'ELSE NULL ' +
										    'END AS [block_info], ' +
										    'REPLACE ' +
										    '( ' +
											    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
											    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
											    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
												    'CONVERT ' +
												    '( ' +
													    'NVARCHAR(MAX), ' +
													    'N''('' + ' +
														    'CONVERT(NVARCHAR, num_waits) + N''x: '' + ' +
														    'CASE num_waits ' +
															    'WHEN 1 THEN CONVERT(NVARCHAR, min_wait_time) + N''ms'' ' +
															    'WHEN 2 THEN ' +
																    'CASE ' +
																	    'WHEN min_wait_time <> max_wait_time THEN CONVERT(NVARCHAR, min_wait_time) + N''/'' + CONVERT(NVARCHAR, max_wait_time) + N''ms'' ' +
																	    'ELSE CONVERT(NVARCHAR, max_wait_time) + N''ms'' ' +
																    'END ' +
															    'ELSE ' +
																    'CASE ' +
																	    'WHEN min_wait_time <> max_wait_time THEN CONVERT(NVARCHAR, min_wait_time) + N''/'' + CONVERT(NVARCHAR, avg_wait_time) + N''/'' + CONVERT(NVARCHAR, max_wait_time) + N''ms'' ' +
																	    'ELSE CONVERT(NVARCHAR, max_wait_time) + N''ms'' ' +
																    'END ' +
														    'END + ' +
													    'N'')'' + wait_type COLLATE Latin1_General_Bin2 ' +
												    '), ' +
												    'NCHAR(31),N''?''),NCHAR(30),N''?''),NCHAR(29),N''?''),NCHAR(28),N''?''),NCHAR(27),N''?''),NCHAR(26),N''?''),NCHAR(25),N''?''),NCHAR(24),N''?''),NCHAR(23),N''?''),NCHAR(22),N''?''), ' +
												    'NCHAR(21),N''?''),NCHAR(20),N''?''),NCHAR(19),N''?''),NCHAR(18),N''?''),NCHAR(17),N''?''),NCHAR(16),N''?''),NCHAR(15),N''?''),NCHAR(14),N''?''),NCHAR(12),N''?''), ' +
												    'NCHAR(11),N''?''),NCHAR(8),N''?''),NCHAR(7),N''?''),NCHAR(6),N''?''),NCHAR(5),N''?''),NCHAR(4),N''?''),NCHAR(3),N''?''),NCHAR(2),N''?''),NCHAR(1),N''?''), ' +
											    'NCHAR(0), ' +
											    'N'''' ' +
										    ') AS [waits] ' +
									    'FROM ' +
									    '( ' +
										    'SELECT TOP(@i) ' +
											    'w1.*, ' +
											    'ROW_NUMBER() OVER ' +
											    '( ' +
												    'PARTITION BY ' +
													    'w1.session_id, ' +
													    'w1.request_id ' +
												    'ORDER BY ' +
													    'w1.block_info DESC, ' +
													    'w1.num_waits DESC, ' +
													    'w1.wait_type ' +
											    ') AS r ' +
										    'FROM ' +
										    '( ' +
											    'SELECT TOP(@i) ' +
												    'task_info.session_id, ' +
												    'task_info.request_id, ' +
												    'task_info.physical_io, ' +
												    'task_info.context_switches, ' +
												    'task_info.thread_CPU_snapshot, ' +
												    'task_info.num_tasks AS tasks, ' +
												    'CASE ' +
													    'WHEN task_info.runnable_time IS NOT NULL THEN ' +
														    '''RUNNABLE'' ' +
													    'ELSE ' +
														    'wt2.wait_type ' +
												    'END AS wait_type, ' +
												    'NULLIF(COUNT(COALESCE(task_info.runnable_time, wt2.waiting_task_address)), 0) AS num_waits, ' +
												    'MIN(COALESCE(task_info.runnable_time, wt2.wait_duration_ms)) AS min_wait_time, ' +
												    'AVG(COALESCE(task_info.runnable_time, wt2.wait_duration_ms)) AS avg_wait_time, ' +
												    'MAX(COALESCE(task_info.runnable_time, wt2.wait_duration_ms)) AS max_wait_time, ' +
												    'MAX(wt2.block_info) AS block_info ' +
											    'FROM ' +
											    '( ' +
												    'SELECT TOP(@i) ' +
													    't.session_id, ' +
													    't.request_id, ' +
													    'SUM(CONVERT(BIGINT, t.pending_io_count)) OVER (PARTITION BY t.session_id, t.request_id) AS physical_io, ' +
													    'SUM(CONVERT(BIGINT, t.context_switches_count)) OVER (PARTITION BY t.session_id, t.request_id) AS context_switches, ' +
													    CASE
														    WHEN @output_column_list LIKE '%|[CPU_delta|]%' ESCAPE '|'
															    THEN
																    'SUM(tr.usermode_time + tr.kernel_time) OVER (PARTITION BY t.session_id, t.request_id) '
														    ELSE
															    'CONVERT(BIGINT, NULL) '
													    END + ' AS thread_CPU_snapshot, ' +
													    'COUNT(*) OVER (PARTITION BY t.session_id, t.request_id) AS num_tasks, ' +
													    't.task_address, ' +
													    't.task_state, ' +
													    'CASE ' +
														    'WHEN ' +
															    't.task_state = ''RUNNABLE'' ' +
															    'AND w.runnable_time > 0 THEN ' +
																    'w.runnable_time ' +
														    'ELSE ' +
															    'NULL ' +
													    'END AS runnable_time ' +
												    'FROM sys.dm_os_tasks AS t ' +
												    'CROSS APPLY ' +
												    '( ' +
													    'SELECT TOP(1) ' +
														    'sp2.session_id ' +
													    'FROM @sessions AS sp2 ' +
													    'WHERE ' +
														    'sp2.session_id = t.session_id ' +
														    'AND sp2.request_id = t.request_id ' +
														    'AND sp2.status <> ''sleeping'' ' +
												    ') AS sp20 ' +
												    'LEFT OUTER HASH JOIN ' +
												    '( ' +
													    'SELECT TOP(@i) ' +
														    '( ' +
															    'SELECT TOP(@i) ' +
																    'ms_ticks ' +
															    'FROM sys.dm_os_sys_info ' +
														    ') - ' +
															    'w0.wait_resumed_ms_ticks AS runnable_time, ' +
														    'w0.worker_address, ' +
														    'w0.thread_address, ' +
														    'w0.task_bound_ms_ticks ' +
													    'FROM sys.dm_os_workers AS w0 ' +
													    'WHERE ' +
														    'w0.state = ''RUNNABLE'' ' +
														    'OR @first_collection_ms_ticks >= w0.task_bound_ms_ticks ' +
												    ') AS w ON ' +
													    'w.worker_address = t.worker_address ' +
												    CASE
													    WHEN @output_column_list LIKE '%|[CPU_delta|]%' ESCAPE '|'
														    THEN
															    'LEFT OUTER HASH JOIN sys.dm_os_threads AS tr ON ' +
																    'tr.thread_address = w.thread_address ' +
																    'AND @first_collection_ms_ticks >= w.task_bound_ms_ticks ' 
													    ELSE
														    ''
												    END +
											    ') AS task_info ' +
											    'LEFT OUTER HASH JOIN ' +
											    '( ' +
												    'SELECT TOP(@i) ' +
													    'wt1.wait_type, ' +
													    'wt1.waiting_task_address, ' +
													    'MAX(wt1.wait_duration_ms) AS wait_duration_ms, ' +
													    'MAX(wt1.block_info) AS block_info ' +
												    'FROM ' +
												    '( ' +
													    'SELECT DISTINCT TOP(@i) ' +
														    'wt.wait_type + ' +
															    --TODO: What else can be pulled from the resource_description?
															    'CASE ' +
																    'WHEN wt.wait_type LIKE N''PAGE%LATCH_%'' THEN ' +
																	    ''':'' + ' +
																	    --database name
																	    'COALESCE(DB_NAME(CONVERT(INT, LEFT(wt.resource_description, CHARINDEX(N'':'', wt.resource_description) - 1))), N''(null)'') + ' +
																	    'N'':'' + ' +
																	    --file id
																	    'SUBSTRING(wt.resource_description, CHARINDEX(N'':'', wt.resource_description) + 1, LEN(wt.resource_description) - CHARINDEX(N'':'', REVERSE(wt.resource_description)) - CHARINDEX(N'':'', wt.resource_description)) + ' +
																	    --page # for special pages
																	    'N''('' + ' +
																		    'CASE ' +
																			    'WHEN ' +
																				    'CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) = 1 OR ' +
																				    'CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) % 8088 = 0 THEN N''PFS'' ' +
																			    'WHEN ' +
																				    'CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) = 2 OR ' +
																				    'CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) % 511232 = 0 THEN N''GAM'' ' +
																			    'WHEN ' +
																				    'CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) = 3 OR ' +
																				    'CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) % 511233 = 0 THEN N''SGAM'' ' +
																			    'WHEN ' +
																				    'CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) = 6 OR ' +
																				    'CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) % 511238 = 0 THEN N''DCM'' ' +
																			    'WHEN ' +
																				    'CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) = 7 OR ' +
																				    'CONVERT(INT, RIGHT(wt.resource_description, CHARINDEX(N'':'', REVERSE(wt.resource_description)) - 1)) % 511239 = 0 THEN N''BCM'' ' +
																			    'ELSE N''*'' ' +
																		    'END + ' +
																	    'N'')'' ' +
																    'WHEN wt.wait_type = N''CXPACKET'' THEN ' +
																	    'N'':'' + SUBSTRING(wt.resource_description, CHARINDEX(N''nodeId'', wt.resource_description) + 7, 4) ' +
																    'WHEN wt.wait_type LIKE N''LATCH[_]%'' THEN ' +
																	    'N'' ['' + LEFT(wt.resource_description, COALESCE(NULLIF(CHARINDEX(N'' '', wt.resource_description), 0), LEN(wt.resource_description) + 1) - 1) + N'']'' ' +
																    'ELSE N'''' ' +
															    'END COLLATE Latin1_General_Bin2 AS wait_type, ' +
														    'CASE ' +
															    'WHEN ' +
															    '( ' +
																    'wt.blocking_session_id IS NOT NULL ' +
																    'AND wt.wait_type LIKE N''LCK[_]%'' ' +
															    ') THEN ' +
																    '( ' +
																	    'SELECT TOP(@i) ' +
																		    'x.lock_type, ' +
																		    'REPLACE ' +
																		    '( ' +
																			    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
																			    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
																			    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
																				    'DB_NAME ' +
																				    '( ' +
																					    'CONVERT ' +
																					    '( ' +
																						    'INT, ' +
																						    'SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N''dbid='', wt.resource_description), 0) + 5, COALESCE(NULLIF(CHARINDEX(N'' '', wt.resource_description, CHARINDEX(N''dbid='', wt.resource_description) + 5), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N''dbid='', wt.resource_description) - 5) ' +
																					    ') ' +
																				    '), ' +
																				    'NCHAR(31),N''?''),NCHAR(30),N''?''),NCHAR(29),N''?''),NCHAR(28),N''?''),NCHAR(27),N''?''),NCHAR(26),N''?''),NCHAR(25),N''?''),NCHAR(24),N''?''),NCHAR(23),N''?''),NCHAR(22),N''?''), ' +
																				    'NCHAR(21),N''?''),NCHAR(20),N''?''),NCHAR(19),N''?''),NCHAR(18),N''?''),NCHAR(17),N''?''),NCHAR(16),N''?''),NCHAR(15),N''?''),NCHAR(14),N''?''),NCHAR(12),N''?''), ' +
																				    'NCHAR(11),N''?''),NCHAR(8),N''?''),NCHAR(7),N''?''),NCHAR(6),N''?''),NCHAR(5),N''?''),NCHAR(4),N''?''),NCHAR(3),N''?''),NCHAR(2),N''?''),NCHAR(1),N''?''), ' +
																			    'NCHAR(0), ' +
																			    'N'''' ' +
																		    ') AS database_name, ' +
																		    'CASE x.lock_type ' +
																			    'WHEN N''objectlock'' THEN SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N''objid='', wt.resource_description), 0) + 6, COALESCE(NULLIF(CHARINDEX(N'' '', wt.resource_description, CHARINDEX(N''objid='', wt.resource_description) + 6), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N''objid='', wt.resource_description) - 6) ' +
																			    'ELSE NULL ' +
																		    'END AS object_id, ' +
																		    'CASE x.lock_type ' +
																			    'WHEN N''filelock'' THEN ' +
																				    'SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N''fileid='', wt.resource_description), 0) + 7, COALESCE(NULLIF(CHARINDEX(N'' '', wt.resource_description, CHARINDEX(N''fileid='', wt.resource_description) + 7), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N''fileid='', wt.resource_description) - 7) ' +
																			    'ELSE NULL ' +
																		    'END AS file_id, ' +
																		    'CASE ' +
																			    'WHEN x.lock_type in (N''pagelock'', N''extentlock'', N''ridlock'') THEN ' +
																				    'SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N''associatedObjectId='', wt.resource_description), 0) + 19, COALESCE(NULLIF(CHARINDEX(N'' '', wt.resource_description, CHARINDEX(N''associatedObjectId='', wt.resource_description) + 19), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N''associatedObjectId='', wt.resource_description) - 19) ' +
																			    'WHEN x.lock_type in (N''keylock'', N''hobtlock'', N''allocunitlock'') THEN ' +
																				    'SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N''hobtid='', wt.resource_description), 0) + 7, COALESCE(NULLIF(CHARINDEX(N'' '', wt.resource_description, CHARINDEX(N''hobtid='', wt.resource_description) + 7), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N''hobtid='', wt.resource_description) - 7) ' +
																			    'ELSE NULL ' +
																		    'END AS hobt_id, ' +
																		    'CASE x.lock_type ' +
																			    'WHEN N''applicationlock'' THEN ' +
																				    'SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N''hash='', wt.resource_description), 0) + 5, COALESCE(NULLIF(CHARINDEX(N'' '', wt.resource_description, CHARINDEX(N''hash='', wt.resource_description) + 5), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N''hash='', wt.resource_description) - 5) ' +
																			    'ELSE NULL ' +
																		    'END AS applock_hash, ' +
																		    'CASE x.lock_type ' +
																			    'WHEN N''metadatalock'' THEN ' +
																				    'SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N''subresource='', wt.resource_description), 0) + 12, COALESCE(NULLIF(CHARINDEX(N'' '', wt.resource_description, CHARINDEX(N''subresource='', wt.resource_description) + 12), 0), LEN(wt.resource_description) + 1) - CHARINDEX(N''subresource='', wt.resource_description) - 12) ' +
																			    'ELSE NULL ' +
																		    'END AS metadata_resource, ' +
																		    'CASE x.lock_type ' +
																			    'WHEN N''metadatalock'' THEN ' +
																				    'SUBSTRING(wt.resource_description, NULLIF(CHARINDEX(N''classid='', wt.resource_description), 0) + 8, COALESCE(NULLIF(CHARINDEX(N'' dbid='', wt.resource_description) - CHARINDEX(N''classid='', wt.resource_description), 0), LEN(wt.resource_description) + 1) - 8) ' +
																			    'ELSE NULL ' +
																		    'END AS metadata_class_id ' +
																	    'FROM ' +
																	    '( ' +
																		    'SELECT TOP(1) ' +
																			    'LEFT(wt.resource_description, CHARINDEX(N'' '', wt.resource_description) - 1) COLLATE Latin1_General_Bin2 AS lock_type ' +
																	    ') AS x ' +
																	    'FOR XML ' +
																		    'PATH('''') ' +
																    ') ' +
															    'ELSE NULL ' +
														    'END AS block_info, ' +
														    'wt.wait_duration_ms, ' +
														    'wt.waiting_task_address ' +
													    'FROM ' +
													    '( ' +
														    'SELECT TOP(@i) ' +
															    'wt0.wait_type COLLATE Latin1_General_Bin2 AS wait_type, ' +
															    'wt0.resource_description COLLATE Latin1_General_Bin2 AS resource_description, ' +
															    'wt0.wait_duration_ms, ' +
															    'wt0.waiting_task_address, ' +
															    'CASE ' +
																    'WHEN wt0.blocking_session_id = p.blocked THEN wt0.blocking_session_id ' +
																    'ELSE NULL ' +
															    'END AS blocking_session_id ' +
														    'FROM sys.dm_os_waiting_tasks AS wt0 ' +
														    'CROSS APPLY ' +
														    '( ' +
															    'SELECT TOP(1)' +
																    's0.blocked ' +
															    'FROM @sessions AS s0 ' +
															    'WHERE ' +
																    's0.session_id = wt0.session_id ' +
																    'AND s0.wait_type <> N''OLEDB'' ' +
																    'AND wt0.wait_type <> N''OLEDB'' ' +
														    ') AS p ' +
													    ') AS wt ' +
												    ') AS wt1 ' +
												    'GROUP BY ' +
													    'wt1.wait_type, ' +
													    'wt1.waiting_task_address ' +
											    ') AS wt2 ON ' +
												    'wt2.waiting_task_address = task_info.task_address ' +
												    'AND wt2.wait_duration_ms > 0 ' +
												    'AND task_info.runnable_time IS NULL ' +
											    'GROUP BY ' +
												    'task_info.session_id, ' +
												    'task_info.request_id, ' +
												    'task_info.physical_io, ' +
												    'task_info.context_switches, ' +
												    'task_info.thread_CPU_snapshot, ' +
												    'task_info.num_tasks, ' +
												    'CASE ' +
													    'WHEN task_info.runnable_time IS NOT NULL THEN ' +
														    '''RUNNABLE'' ' +
													    'ELSE ' +
														    'wt2.wait_type ' +
												    'END ' +
										    ') AS w1 ' +
									    ') AS waits ' +
									    'ORDER BY ' +
										    'waits.session_id, ' +
										    'waits.request_id, ' +
										    'waits.r ' +
									    'FOR XML ' +
										    'PATH(N''tasks''), ' +
										    'TYPE ' +
								    ') AS tasks_raw (task_xml_raw) ' +
							    ') AS tasks_final ' +
							    'CROSS APPLY tasks_final.task_xml.nodes(N''/tasks'') AS task_nodes (task_node) ' +
							    'WHERE ' +
								    'task_nodes.task_node.exist(N''session_id'') = 1 ' +
						    ') AS tasks ON ' +
							    'tasks.session_id = y.session_id ' +
							    'AND tasks.request_id = y.request_id '
					    ELSE ''
				    END +
				    'LEFT OUTER HASH JOIN ' +
				    '( ' +
					    'SELECT TOP(@i) ' +
						    't_info.session_id, ' +
						    'COALESCE(t_info.request_id, -1) AS request_id, ' +
						    'SUM(t_info.tempdb_allocations) AS tempdb_allocations, ' +
						    'SUM(t_info.tempdb_current) AS tempdb_current ' +
					    'FROM ' +
					    '( ' +
						    'SELECT TOP(@i) ' +
							    'tsu.session_id, ' +
							    'tsu.request_id, ' +
							    'tsu.user_objects_alloc_page_count + ' +
								    'tsu.internal_objects_alloc_page_count AS tempdb_allocations,' +
							    'tsu.user_objects_alloc_page_count + ' +
								    'tsu.internal_objects_alloc_page_count - ' +
								    'tsu.user_objects_dealloc_page_count - ' +
								    'tsu.internal_objects_dealloc_page_count AS tempdb_current ' +
						    'FROM sys.dm_db_task_space_usage AS tsu ' +
						    'CROSS APPLY ' +
						    '( ' +
							    'SELECT TOP(1) ' +
								    's0.session_id ' +
							    'FROM @sessions AS s0 ' +
							    'WHERE ' +
								    's0.session_id = tsu.session_id ' +
						    ') AS p ' +
						    '' +
						    'UNION ALL ' +
						    '' +
						    'SELECT TOP(@i) ' +
							    'ssu.session_id, ' +
							    'NULL AS request_id, ' +
							    'ssu.user_objects_alloc_page_count + ' +
								    'ssu.internal_objects_alloc_page_count AS tempdb_allocations, ' +
							    'ssu.user_objects_alloc_page_count + ' +
								    'ssu.internal_objects_alloc_page_count - ' +
								    'ssu.user_objects_dealloc_page_count - ' +
								    'ssu.internal_objects_dealloc_page_count AS tempdb_current ' +
						    'FROM sys.dm_db_session_space_usage AS ssu ' +
						    'CROSS APPLY ' +
						    '( ' +
							    'SELECT TOP(1) ' +
								    's0.session_id ' +
							    'FROM @sessions AS s0 ' +
							    'WHERE ' +
								    's0.session_id = ssu.session_id ' +
						    ') AS p ' +
					    ') AS t_info ' +
					    'GROUP BY ' +
						    't_info.session_id, ' +
						    'COALESCE(t_info.request_id, -1) ' +
				    ') AS tempdb_info ON ' +
					    'tempdb_info.session_id = y.session_id ' +
					    'AND tempdb_info.request_id = ' +
						    'CASE ' +
							    'WHEN y.status = N''sleeping'' THEN ' +
								    '-1 ' +
							    'ELSE ' +
								    'y.request_id ' +
						    'END ' +
				    CASE 
					    WHEN 
						    NOT 
						    (
							    @get_avg_time = 1 
							    AND @recursion = 1
						    ) THEN 
							    ''
					    ELSE
						    'LEFT OUTER HASH JOIN ' +
						    '( ' +
							    'SELECT TOP(@i) ' +
								    '* ' +
							    'FROM sys.dm_exec_query_stats ' +
						    ') AS qs ON ' +
							    'qs.sql_handle = y.sql_handle ' + 
							    'AND qs.plan_handle = y.plan_handle ' + 
							    'AND qs.statement_start_offset = y.statement_start_offset ' +
							    'AND qs.statement_end_offset = y.statement_end_offset '
					    END + 
			    ') AS x ' +
			    'OPTION (KEEPFIXED PLAN, OPTIMIZE FOR (@i = 1)); ';

		    SET @sql_n = CONVERT(NVARCHAR(MAX), @sql);

		    SET @last_collection_start = GETDATE();
		
		    IF @recursion = -1
		    BEGIN;
			    SELECT
				    @first_collection_ms_ticks = ms_ticks
			    FROM sys.dm_os_sys_info;
		    END;

		    INSERT #sessions
		    (
			    recursion,
			    session_id,
			    request_id,
			    session_number,
			    elapsed_time,
			    avg_elapsed_time,
			    physical_io,
			    reads,
			    physical_reads,
			    writes,
			    tempdb_allocations,
			    tempdb_current,
			    CPU,
			    thread_CPU_snapshot,
			    context_switches,
			    used_memory,
			    tasks,
			    status,
			    wait_info,
			    transaction_id,
			    open_tran_count,
			    sql_handle,
			    statement_start_offset,
			    statement_end_offset,		
			    sql_text,
			    plan_handle,
			    blocking_session_id,
			    percent_complete,
			    host_name,
			    login_name,
			    database_name,
			    program_name,
			    additional_info,
			    start_time,
			    login_time,
			    last_request_start_time
		    )
		    EXEC sp_executesql 
			    @sql_n,
			    N'@recursion SMALLINT, @filter sysname, @not_filter sysname, @first_collection_ms_ticks BIGINT',
			    @recursion, @filter, @not_filter, @first_collection_ms_ticks;

		    --Collect transaction information?
		    IF
			    @recursion = 1
			    AND
			    (
				    @output_column_list LIKE '%|[tran_start_time|]%' ESCAPE '|'
				    OR @output_column_list LIKE '%|[tran_log_writes|]%' ESCAPE '|' 
			    )
		    BEGIN;	
			    DECLARE @i INT;
			    SET @i = 2147483647;

			    UPDATE s
			    SET
				    tran_start_time =
					    CONVERT
					    (
						    DATETIME,
						    LEFT
						    (
							    x.trans_info,
							    NULLIF(CHARINDEX(NCHAR(254), x.trans_info) - 1, -1)
						    ),
						    121
					    ),
				    tran_log_writes =
					    RIGHT
					    (
						    x.trans_info,
						    LEN(x.trans_info) - CHARINDEX(NCHAR(254), x.trans_info)
					    )
			    FROM
			    (
				    SELECT TOP(@i)
					    trans_nodes.trans_node.value('(session_id/text())[1]', 'SMALLINT') AS session_id,
					    COALESCE(trans_nodes.trans_node.value('(request_id/text())[1]', 'INT'), 0) AS request_id,
					    trans_nodes.trans_node.value('(trans_info/text())[1]', 'NVARCHAR(4000)') AS trans_info				
				    FROM
				    (
					    SELECT TOP(@i)
						    CONVERT
						    (
							    XML,
							    REPLACE
							    (
								    CONVERT(NVARCHAR(MAX), trans_raw.trans_xml_raw) COLLATE Latin1_General_Bin2, 
								    N'</trans_info></trans><trans><trans_info>', N''
							    )
						    )
					    FROM
					    (
						    SELECT TOP(@i)
							    CASE u_trans.r
								    WHEN 1 THEN u_trans.session_id
								    ELSE NULL
							    END AS [session_id],
							    CASE u_trans.r
								    WHEN 1 THEN u_trans.request_id
								    ELSE NULL
							    END AS [request_id],
							    CONVERT
							    (
								    NVARCHAR(MAX),
								    CASE
									    WHEN u_trans.database_id IS NOT NULL THEN
										    CASE u_trans.r
											    WHEN 1 THEN COALESCE(CONVERT(NVARCHAR, u_trans.transaction_start_time, 121) + NCHAR(254), N'')
											    ELSE N''
										    END + 
											    REPLACE
											    (
												    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
												    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
												    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
													    CONVERT(VARCHAR(128), COALESCE(DB_NAME(u_trans.database_id), N'(null)')),
													    NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
													    NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
													    NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
												    NCHAR(0),
												    N'?'
											    ) +
											    N': ' +
										    CONVERT(NVARCHAR, u_trans.log_record_count) + N' (' + CONVERT(NVARCHAR, u_trans.log_kb_used) + N' kB)' +
										    N','
									    ELSE
										    N'N/A,'
								    END COLLATE Latin1_General_Bin2
							    ) AS [trans_info]
						    FROM
						    (
							    SELECT TOP(@i)
								    trans.*,
								    ROW_NUMBER() OVER
								    (
									    PARTITION BY
										    trans.session_id,
										    trans.request_id
									    ORDER BY
										    trans.transaction_start_time DESC
								    ) AS r
							    FROM
							    (
								    SELECT TOP(@i)
									    session_tran_map.session_id,
									    session_tran_map.request_id,
									    s_tran.database_id,
									    COALESCE(SUM(s_tran.database_transaction_log_record_count), 0) AS log_record_count,
									    COALESCE(SUM(s_tran.database_transaction_log_bytes_used), 0) / 1024 AS log_kb_used,
									    MIN(s_tran.database_transaction_begin_time) AS transaction_start_time
								    FROM
								    (
									    SELECT TOP(@i)
										    *
									    FROM sys.dm_tran_active_transactions
									    WHERE
										    transaction_begin_time <= @last_collection_start
								    ) AS a_tran
								    INNER HASH JOIN
								    (
									    SELECT TOP(@i)
										    *
									    FROM sys.dm_tran_database_transactions
									    WHERE
										    database_id < 32767
								    ) AS s_tran ON
									    s_tran.transaction_id = a_tran.transaction_id
								    LEFT OUTER HASH JOIN
								    (
									    SELECT TOP(@i)
										    *
									    FROM sys.dm_tran_session_transactions
								    ) AS tst ON
									    s_tran.transaction_id = tst.transaction_id
								    CROSS APPLY
								    (
									    SELECT TOP(1)
										    s3.session_id,
										    s3.request_id
									    FROM
									    (
										    SELECT TOP(1)
											    s1.session_id,
											    s1.request_id
										    FROM #sessions AS s1
										    WHERE
											    s1.transaction_id = s_tran.transaction_id
											    AND s1.recursion = 1
											
										    UNION ALL
									
										    SELECT TOP(1)
											    s2.session_id,
											    s2.request_id
										    FROM #sessions AS s2
										    WHERE
											    s2.session_id = tst.session_id
											    AND s2.recursion = 1
									    ) AS s3
									    ORDER BY
										    s3.request_id
								    ) AS session_tran_map
								    GROUP BY
									    session_tran_map.session_id,
									    session_tran_map.request_id,
									    s_tran.database_id
							    ) AS trans
						    ) AS u_trans
						    FOR XML
							    PATH('trans'),
							    TYPE
					    ) AS trans_raw (trans_xml_raw)
				    ) AS trans_final (trans_xml)
				    CROSS APPLY trans_final.trans_xml.nodes('/trans') AS trans_nodes (trans_node)
			    ) AS x
			    INNER HASH JOIN #sessions AS s ON
				    s.session_id = x.session_id
				    AND s.request_id = x.request_id
			    OPTION (OPTIMIZE FOR (@i = 1));
		    END;

		    --Variables for text and plan collection
		    DECLARE	
			    @session_id SMALLINT,
			    @request_id INT,
			    @sql_handle VARBINARY(64),
			    @plan_handle VARBINARY(64),
			    @statement_start_offset INT,
			    @statement_end_offset INT,
			    @start_time DATETIME,
			    @database_name sysname;

		    IF 
			    @recursion = 1
			    AND @output_column_list LIKE '%|[sql_text|]%' ESCAPE '|'
		    BEGIN;
			    DECLARE sql_cursor
			    CURSOR LOCAL FAST_FORWARD
			    FOR 
				    SELECT 
					    session_id,
					    request_id,
					    sql_handle,
					    statement_start_offset,
					    statement_end_offset
				    FROM #sessions
				    WHERE
					    recursion = 1
					    AND sql_handle IS NOT NULL
			    OPTION (KEEPFIXED PLAN);

			    OPEN sql_cursor;

			    FETCH NEXT FROM sql_cursor
			    INTO 
				    @session_id,
				    @request_id,
				    @sql_handle,
				    @statement_start_offset,
				    @statement_end_offset;

			    --Wait up to 5 ms for the SQL text, then give up
			    SET LOCK_TIMEOUT 5;

			    WHILE @@FETCH_STATUS = 0
			    BEGIN;
				    BEGIN TRY;
					    UPDATE s
					    SET
						    s.sql_text =
						    (
							    SELECT
								    REPLACE
								    (
									    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
										    N'--' + NCHAR(13) + NCHAR(10) +
										    CASE 
											    WHEN @get_full_inner_text = 1 THEN est.text
											    WHEN LEN(est.text) < (@statement_end_offset / 2) + 1 THEN est.text
											    WHEN SUBSTRING(est.text, (@statement_start_offset/2), 2) LIKE N'[a-zA-Z0-9][a-zA-Z0-9]' THEN est.text
											    ELSE
												    CASE
													    WHEN @statement_start_offset > 0 THEN
														    SUBSTRING
														    (
															    est.text,
															    ((@statement_start_offset/2) + 1),
															    (
																    CASE
																	    WHEN @statement_end_offset = -1 THEN 2147483647
																	    ELSE ((@statement_end_offset - @statement_start_offset)/2) + 1
																    END
															    )
														    )
													    ELSE RTRIM(LTRIM(est.text))
												    END
										    END +
										    NCHAR(13) + NCHAR(10) + N'--' COLLATE Latin1_General_Bin2,
										    NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
										    NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
										    NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
									    NCHAR(0),
									    N''
								    ) AS [processing-instruction(query)]
							    FOR XML
								    PATH(''),
								    TYPE
						    ),
						    s.statement_start_offset = 
							    CASE 
								    WHEN LEN(est.text) < (@statement_end_offset / 2) + 1 THEN 0
								    WHEN SUBSTRING(CONVERT(VARCHAR(MAX), est.text), (@statement_start_offset/2), 2) LIKE '[a-zA-Z0-9][a-zA-Z0-9]' THEN 0
								    ELSE @statement_start_offset
							    END,
						    s.statement_end_offset = 
							    CASE 
								    WHEN LEN(est.text) < (@statement_end_offset / 2) + 1 THEN -1
								    WHEN SUBSTRING(CONVERT(VARCHAR(MAX), est.text), (@statement_start_offset/2), 2) LIKE '[a-zA-Z0-9][a-zA-Z0-9]' THEN -1
								    ELSE @statement_end_offset
							    END
					    FROM 
						    #sessions AS s,
						    (
							    SELECT TOP(1)
								    text
							    FROM
							    (
								    SELECT 
									    text, 
									    0 AS row_num
								    FROM sys.dm_exec_sql_text(@sql_handle)
								
								    UNION ALL
								
								    SELECT 
									    NULL,
									    1 AS row_num
							    ) AS est0
							    ORDER BY
								    row_num
						    ) AS est
					    WHERE 
						    s.session_id = @session_id
						    AND s.request_id = @request_id
						    AND s.recursion = 1
					    OPTION (KEEPFIXED PLAN);
				    END TRY
				    BEGIN CATCH;
					    UPDATE s
					    SET
						    s.sql_text = 
							    CASE ERROR_NUMBER() 
								    WHEN 1222 THEN '<timeout_exceeded />'
								    ELSE '<error message="' + ERROR_MESSAGE() + '" />'
							    END
					    FROM #sessions AS s
					    WHERE 
						    s.session_id = @session_id
						    AND s.request_id = @request_id
						    AND s.recursion = 1
					    OPTION (KEEPFIXED PLAN);
				    END CATCH;

				    FETCH NEXT FROM sql_cursor
				    INTO
					    @session_id,
					    @request_id,
					    @sql_handle,
					    @statement_start_offset,
					    @statement_end_offset;
			    END;

			    --Return this to the default
			    SET LOCK_TIMEOUT -1;

			    CLOSE sql_cursor;
			    DEALLOCATE sql_cursor;
		    END;

		    IF 
			    @get_outer_command = 1 
			    AND @recursion = 1
			    AND @output_column_list LIKE '%|[sql_command|]%' ESCAPE '|'
		    BEGIN;
			    DECLARE @buffer_results TABLE
			    (
				    EventType VARCHAR(30),
				    Parameters INT,
				    EventInfo NVARCHAR(4000),
				    start_time DATETIME,
				    session_number INT IDENTITY(1,1) NOT NULL PRIMARY KEY
			    );

			    DECLARE buffer_cursor
			    CURSOR LOCAL FAST_FORWARD
			    FOR 
				    SELECT 
					    session_id,
					    MAX(start_time) AS start_time
				    FROM #sessions
				    WHERE
					    recursion = 1
				    GROUP BY
					    session_id
				    ORDER BY
					    session_id
				    OPTION (KEEPFIXED PLAN);

			    OPEN buffer_cursor;

			    FETCH NEXT FROM buffer_cursor
			    INTO 
				    @session_id,
				    @start_time;

			    WHILE @@FETCH_STATUS = 0
			    BEGIN;
				    BEGIN TRY;
					    --In SQL Server 2008, DBCC INPUTBUFFER will throw 
					    --an exception if the session no longer exists
					    INSERT @buffer_results
					    (
						    EventType,
						    Parameters,
						    EventInfo
					    )
					    EXEC sp_executesql
						    N'DBCC INPUTBUFFER(@session_id) WITH NO_INFOMSGS;',
						    N'@session_id SMALLINT',
						    @session_id;

					    UPDATE br
					    SET
						    br.start_time = @start_time
					    FROM @buffer_results AS br
					    WHERE
						    br.session_number = 
						    (
							    SELECT MAX(br2.session_number)
							    FROM @buffer_results br2
						    );
				    END TRY
				    BEGIN CATCH
				    END CATCH;

				    FETCH NEXT FROM buffer_cursor
				    INTO 
					    @session_id,
					    @start_time;
			    END;

			    UPDATE s
			    SET
				    sql_command = 
				    (
					    SELECT 
						    REPLACE
						    (
							    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
							    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
							    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
								    CONVERT
								    (
									    NVARCHAR(MAX),
									    N'--' + NCHAR(13) + NCHAR(10) + br.EventInfo + NCHAR(13) + NCHAR(10) + N'--' COLLATE Latin1_General_Bin2
								    ),
								    NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
								    NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
								    NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
							    NCHAR(0),
							    N''
						    ) AS [processing-instruction(query)]
					    FROM @buffer_results AS br
					    WHERE 
						    br.session_number = s.session_number
						    AND br.start_time = s.start_time
						    AND 
						    (
							    (
								    s.start_time = s.last_request_start_time
								    AND EXISTS
								    (
									    SELECT *
									    FROM sys.dm_exec_requests r2
									    WHERE
										    r2.session_id = s.session_id
										    AND r2.request_id = s.request_id
										    AND r2.start_time = s.start_time
								    )
							    )
							    OR 
							    (
								    s.request_id = 0
								    AND EXISTS
								    (
									    SELECT *
									    FROM sys.dm_exec_sessions s2
									    WHERE
										    s2.session_id = s.session_id
										    AND s2.last_request_start_time = s.last_request_start_time
								    )
							    )
						    )
					    FOR XML
						    PATH(''),
						    TYPE
				    )
			    FROM #sessions AS s
			    WHERE
				    recursion = 1
			    OPTION (KEEPFIXED PLAN);

			    CLOSE buffer_cursor;
			    DEALLOCATE buffer_cursor;
		    END;

		    IF 
			    @get_plans >= 1 
			    AND @recursion = 1
			    AND @output_column_list LIKE '%|[query_plan|]%' ESCAPE '|'
		    BEGIN;
			    DECLARE plan_cursor
			    CURSOR LOCAL FAST_FORWARD
			    FOR 
				    SELECT
					    session_id,
					    request_id,
					    plan_handle,
					    statement_start_offset,
					    statement_end_offset
				    FROM #sessions
				    WHERE
					    recursion = 1
					    AND plan_handle IS NOT NULL
			    OPTION (KEEPFIXED PLAN);

			    OPEN plan_cursor;

			    FETCH NEXT FROM plan_cursor
			    INTO 
				    @session_id,
				    @request_id,
				    @plan_handle,
				    @statement_start_offset,
				    @statement_end_offset;

			    --Wait up to 5 ms for a query plan, then give up
			    SET LOCK_TIMEOUT 5;

			    WHILE @@FETCH_STATUS = 0
			    BEGIN;
				    BEGIN TRY;
					    UPDATE s
					    SET
						    s.query_plan =
						    (
							    SELECT
								    CONVERT(xml, query_plan)
							    FROM sys.dm_exec_text_query_plan
							    (
								    @plan_handle, 
								    CASE @get_plans
									    WHEN 1 THEN
										    @statement_start_offset
									    ELSE
										    0
								    END, 
								    CASE @get_plans
									    WHEN 1 THEN
										    @statement_end_offset
									    ELSE
										    -1
								    END
							    )
						    )
					    FROM #sessions AS s
					    WHERE 
						    s.session_id = @session_id
						    AND s.request_id = @request_id
						    AND s.recursion = 1
					    OPTION (KEEPFIXED PLAN);
				    END TRY
				    BEGIN CATCH;
					    IF ERROR_NUMBER() = 6335
					    BEGIN;
						    UPDATE s
						    SET
							    s.query_plan =
							    (
								    SELECT
									    N'--' + NCHAR(13) + NCHAR(10) + 
									    N'-- Could not render showplan due to XML data type limitations. ' + NCHAR(13) + NCHAR(10) + 
									    N'-- To see the graphical plan save the XML below as a .SQLPLAN file and re-open in SSMS.' + NCHAR(13) + NCHAR(10) +
									    N'--' + NCHAR(13) + NCHAR(10) +
										    REPLACE(qp.query_plan, N'<RelOp', NCHAR(13)+NCHAR(10)+N'<RelOp') + 
										    NCHAR(13) + NCHAR(10) + N'--' COLLATE Latin1_General_Bin2 AS [processing-instruction(query_plan)]
								    FROM sys.dm_exec_text_query_plan
								    (
									    @plan_handle, 
									    CASE @get_plans
										    WHEN 1 THEN
											    @statement_start_offset
										    ELSE
											    0
									    END, 
									    CASE @get_plans
										    WHEN 1 THEN
											    @statement_end_offset
										    ELSE
											    -1
									    END
								    ) AS qp
								    FOR XML
									    PATH(''),
									    TYPE
							    )
						    FROM #sessions AS s
						    WHERE 
							    s.session_id = @session_id
							    AND s.request_id = @request_id
							    AND s.recursion = 1
						    OPTION (KEEPFIXED PLAN);
					    END;
					    ELSE
					    BEGIN;
						    UPDATE s
						    SET
							    s.query_plan = 
								    CASE ERROR_NUMBER() 
									    WHEN 1222 THEN '<timeout_exceeded />'
									    ELSE '<error message="' + ERROR_MESSAGE() + '" />'
								    END
						    FROM #sessions AS s
						    WHERE 
							    s.session_id = @session_id
							    AND s.request_id = @request_id
							    AND s.recursion = 1
						    OPTION (KEEPFIXED PLAN);
					    END;
				    END CATCH;

				    FETCH NEXT FROM plan_cursor
				    INTO
					    @session_id,
					    @request_id,
					    @plan_handle,
					    @statement_start_offset,
					    @statement_end_offset;
			    END;

			    --Return this to the default
			    SET LOCK_TIMEOUT -1;

			    CLOSE plan_cursor;
			    DEALLOCATE plan_cursor;
		    END;

		    IF 
			    @get_locks = 1 
			    AND @recursion = 1
			    AND @output_column_list LIKE '%|[locks|]%' ESCAPE '|'
		    BEGIN;
			    DECLARE locks_cursor
			    CURSOR LOCAL FAST_FORWARD
			    FOR 
				    SELECT DISTINCT
					    database_name
				    FROM #locks
				    WHERE
					    EXISTS
					    (
						    SELECT *
						    FROM #sessions AS s
						    WHERE
							    s.session_id = #locks.session_id
							    AND recursion = 1
					    )
					    AND database_name <> '(null)'
				    OPTION (KEEPFIXED PLAN);

			    OPEN locks_cursor;

			    FETCH NEXT FROM locks_cursor
			    INTO 
				    @database_name;

			    WHILE @@FETCH_STATUS = 0
			    BEGIN;
				    BEGIN TRY;
					    SET @sql_n = CONVERT(NVARCHAR(MAX), '') +
						    'UPDATE l ' +
						    'SET ' +
							    'object_name = ' +
								    'REPLACE ' +
								    '( ' +
									    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
									    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
									    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
										    'o.name COLLATE Latin1_General_Bin2, ' +
										    'NCHAR(31),N''?''),NCHAR(30),N''?''),NCHAR(29),N''?''),NCHAR(28),N''?''),NCHAR(27),N''?''),NCHAR(26),N''?''),NCHAR(25),N''?''),NCHAR(24),N''?''),NCHAR(23),N''?''),NCHAR(22),N''?''), ' +
										    'NCHAR(21),N''?''),NCHAR(20),N''?''),NCHAR(19),N''?''),NCHAR(18),N''?''),NCHAR(17),N''?''),NCHAR(16),N''?''),NCHAR(15),N''?''),NCHAR(14),N''?''),NCHAR(12),N''?''), ' +
										    'NCHAR(11),N''?''),NCHAR(8),N''?''),NCHAR(7),N''?''),NCHAR(6),N''?''),NCHAR(5),N''?''),NCHAR(4),N''?''),NCHAR(3),N''?''),NCHAR(2),N''?''),NCHAR(1),N''?''), ' +
									    'NCHAR(0), ' +
									    N''''' ' +
								    '), ' +
							    'index_name = ' +
								    'REPLACE ' +
								    '( ' +
									    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
									    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
									    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
										    'i.name COLLATE Latin1_General_Bin2, ' +
										    'NCHAR(31),N''?''),NCHAR(30),N''?''),NCHAR(29),N''?''),NCHAR(28),N''?''),NCHAR(27),N''?''),NCHAR(26),N''?''),NCHAR(25),N''?''),NCHAR(24),N''?''),NCHAR(23),N''?''),NCHAR(22),N''?''), ' +
										    'NCHAR(21),N''?''),NCHAR(20),N''?''),NCHAR(19),N''?''),NCHAR(18),N''?''),NCHAR(17),N''?''),NCHAR(16),N''?''),NCHAR(15),N''?''),NCHAR(14),N''?''),NCHAR(12),N''?''), ' +
										    'NCHAR(11),N''?''),NCHAR(8),N''?''),NCHAR(7),N''?''),NCHAR(6),N''?''),NCHAR(5),N''?''),NCHAR(4),N''?''),NCHAR(3),N''?''),NCHAR(2),N''?''),NCHAR(1),N''?''), ' +
									    'NCHAR(0), ' +
									    N''''' ' +
								    '), ' +
							    'schema_name = ' +
								    'REPLACE ' +
								    '( ' +
									    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
									    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
									    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
										    's.name COLLATE Latin1_General_Bin2, ' +
										    'NCHAR(31),N''?''),NCHAR(30),N''?''),NCHAR(29),N''?''),NCHAR(28),N''?''),NCHAR(27),N''?''),NCHAR(26),N''?''),NCHAR(25),N''?''),NCHAR(24),N''?''),NCHAR(23),N''?''),NCHAR(22),N''?''), ' +
										    'NCHAR(21),N''?''),NCHAR(20),N''?''),NCHAR(19),N''?''),NCHAR(18),N''?''),NCHAR(17),N''?''),NCHAR(16),N''?''),NCHAR(15),N''?''),NCHAR(14),N''?''),NCHAR(12),N''?''), ' +
										    'NCHAR(11),N''?''),NCHAR(8),N''?''),NCHAR(7),N''?''),NCHAR(6),N''?''),NCHAR(5),N''?''),NCHAR(4),N''?''),NCHAR(3),N''?''),NCHAR(2),N''?''),NCHAR(1),N''?''), ' +
									    'NCHAR(0), ' +
									    N''''' ' +
								    '), ' +
							    'principal_name = ' + 
								    'REPLACE ' +
								    '( ' +
									    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
									    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
									    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
										    'dp.name COLLATE Latin1_General_Bin2, ' +
										    'NCHAR(31),N''?''),NCHAR(30),N''?''),NCHAR(29),N''?''),NCHAR(28),N''?''),NCHAR(27),N''?''),NCHAR(26),N''?''),NCHAR(25),N''?''),NCHAR(24),N''?''),NCHAR(23),N''?''),NCHAR(22),N''?''), ' +
										    'NCHAR(21),N''?''),NCHAR(20),N''?''),NCHAR(19),N''?''),NCHAR(18),N''?''),NCHAR(17),N''?''),NCHAR(16),N''?''),NCHAR(15),N''?''),NCHAR(14),N''?''),NCHAR(12),N''?''), ' +
										    'NCHAR(11),N''?''),NCHAR(8),N''?''),NCHAR(7),N''?''),NCHAR(6),N''?''),NCHAR(5),N''?''),NCHAR(4),N''?''),NCHAR(3),N''?''),NCHAR(2),N''?''),NCHAR(1),N''?''), ' +
									    'NCHAR(0), ' +
									    N''''' ' +
								    ') ' +
						    'FROM #locks AS l ' +
						    'LEFT OUTER JOIN ' + QUOTENAME(@database_name) + '.sys.allocation_units AS au ON ' +
							    'au.allocation_unit_id = l.allocation_unit_id ' +
						    'LEFT OUTER JOIN ' + QUOTENAME(@database_name) + '.sys.partitions AS p ON ' +
							    'p.hobt_id = ' +
								    'COALESCE ' +
								    '( ' +
									    'l.hobt_id, ' +
									    'CASE ' +
										    'WHEN au.type IN (1, 3) THEN au.container_id ' +
										    'ELSE NULL ' +
									    'END ' +
								    ') ' +
						    'LEFT OUTER JOIN ' + QUOTENAME(@database_name) + '.sys.partitions AS p1 ON ' +
							    'l.hobt_id IS NULL ' +
							    'AND au.type = 2 ' +
							    'AND p1.partition_id = au.container_id ' +
						    'LEFT OUTER JOIN ' + QUOTENAME(@database_name) + '.sys.objects AS o ON ' +
							    'o.object_id = COALESCE(l.object_id, p.object_id, p1.object_id) ' +
						    'LEFT OUTER JOIN ' + QUOTENAME(@database_name) + '.sys.indexes AS i ON ' +
							    'i.object_id = COALESCE(l.object_id, p.object_id, p1.object_id) ' +
							    'AND i.index_id = COALESCE(l.index_id, p.index_id, p1.index_id) ' +
						    'LEFT OUTER JOIN ' + QUOTENAME(@database_name) + '.sys.schemas AS s ON ' +
							    's.schema_id = COALESCE(l.schema_id, o.schema_id) ' +
						    'LEFT OUTER JOIN ' + QUOTENAME(@database_name) + '.sys.database_principals AS dp ON ' +
							    'dp.principal_id = l.principal_id ' +
						    'WHERE ' +
							    'l.database_name = @database_name ' +
						    'OPTION (KEEPFIXED PLAN); ';
					
					    EXEC sp_executesql
						    @sql_n,
						    N'@database_name sysname',
						    @database_name;
				    END TRY
				    BEGIN CATCH;
					    UPDATE #locks
					    SET
						    query_error = 
							    REPLACE
							    (
								    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
								    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
								    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									    CONVERT
									    (
										    NVARCHAR(MAX), 
										    ERROR_MESSAGE() COLLATE Latin1_General_Bin2
									    ),
									    NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
									    NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
									    NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
								    NCHAR(0),
								    N''
							    )
					    WHERE 
						    database_name = @database_name
					    OPTION (KEEPFIXED PLAN);
				    END CATCH;

				    FETCH NEXT FROM locks_cursor
				    INTO
					    @database_name;
			    END;

			    CLOSE locks_cursor;
			    DEALLOCATE locks_cursor;

			    CREATE CLUSTERED INDEX IX_SRD ON #locks (session_id, request_id, database_name);

			    UPDATE s
			    SET 
				    s.locks =
				    (
					    SELECT 
						    REPLACE
						    (
							    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
							    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
							    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
								    CONVERT
								    (
									    NVARCHAR(MAX), 
									    l1.database_name COLLATE Latin1_General_Bin2
								    ),
								    NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
								    NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
								    NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
							    NCHAR(0),
							    N''
						    ) AS [Database/@name],
						    MIN(l1.query_error) AS [Database/@query_error],
						    (
							    SELECT 
								    l2.request_mode AS [Lock/@request_mode],
								    l2.request_status AS [Lock/@request_status],
								    COUNT(*) AS [Lock/@request_count]
							    FROM #locks AS l2
							    WHERE 
								    l1.session_id = l2.session_id
								    AND l1.request_id = l2.request_id
								    AND l2.database_name = l1.database_name
								    AND l2.resource_type = 'DATABASE'
							    GROUP BY
								    l2.request_mode,
								    l2.request_status
							    FOR XML
								    PATH(''),
								    TYPE
						    ) AS [Database/Locks],
						    (
							    SELECT
								    COALESCE(l3.object_name, '(null)') AS [Object/@name],
								    l3.schema_name AS [Object/@schema_name],
								    (
									    SELECT
										    l4.resource_type AS [Lock/@resource_type],
										    l4.page_type AS [Lock/@page_type],
										    l4.index_name AS [Lock/@index_name],
										    CASE 
											    WHEN l4.object_name IS NULL THEN l4.schema_name
											    ELSE NULL
										    END AS [Lock/@schema_name],
										    l4.principal_name AS [Lock/@principal_name],
										    l4.resource_description AS [Lock/@resource_description],
										    l4.request_mode AS [Lock/@request_mode],
										    l4.request_status AS [Lock/@request_status],
										    SUM(l4.request_count) AS [Lock/@request_count]
									    FROM #locks AS l4
									    WHERE 
										    l4.session_id = l3.session_id
										    AND l4.request_id = l3.request_id
										    AND l3.database_name = l4.database_name
										    AND COALESCE(l3.object_name, '(null)') = COALESCE(l4.object_name, '(null)')
										    AND COALESCE(l3.schema_name, '') = COALESCE(l4.schema_name, '')
										    AND l4.resource_type <> 'DATABASE'
									    GROUP BY
										    l4.resource_type,
										    l4.page_type,
										    l4.index_name,
										    CASE 
											    WHEN l4.object_name IS NULL THEN l4.schema_name
											    ELSE NULL
										    END,
										    l4.principal_name,
										    l4.resource_description,
										    l4.request_mode,
										    l4.request_status
									    FOR XML
										    PATH(''),
										    TYPE
								    ) AS [Object/Locks]
							    FROM #locks AS l3
							    WHERE 
								    l3.session_id = l1.session_id
								    AND l3.request_id = l1.request_id
								    AND l3.database_name = l1.database_name
								    AND l3.resource_type <> 'DATABASE'
							    GROUP BY 
								    l3.session_id,
								    l3.request_id,
								    l3.database_name,
								    COALESCE(l3.object_name, '(null)'),
								    l3.schema_name
							    FOR XML
								    PATH(''),
								    TYPE
						    ) AS [Database/Objects]
					    FROM #locks AS l1
					    WHERE
						    l1.session_id = s.session_id
						    AND l1.request_id = s.request_id
						    AND l1.start_time IN (s.start_time, s.last_request_start_time)
						    AND s.recursion = 1
					    GROUP BY 
						    l1.session_id,
						    l1.request_id,
						    l1.database_name
					    FOR XML
						    PATH(''),
						    TYPE
				    )
			    FROM #sessions s
			    OPTION (KEEPFIXED PLAN);
		    END;

		    IF 
			    @find_block_leaders = 1
			    AND @recursion = 1
			    AND @output_column_list LIKE '%|[blocked_session_count|]%' ESCAPE '|'
		    BEGIN;
			    WITH
			    blockers AS
			    (
				    SELECT
					    session_id,
					    session_id AS top_level_session_id
				    FROM #sessions
				    WHERE
					    recursion = 1

				    UNION ALL

				    SELECT
					    s.session_id,
					    b.top_level_session_id
				    FROM blockers AS b
				    JOIN #sessions AS s ON
					    s.blocking_session_id = b.session_id
					    AND s.recursion = 1
			    )
			    UPDATE s
			    SET
				    s.blocked_session_count = x.blocked_session_count
			    FROM #sessions AS s
			    JOIN
			    (
				    SELECT
					    b.top_level_session_id AS session_id,
					    COUNT(*) - 1 AS blocked_session_count
				    FROM blockers AS b
				    GROUP BY
					    b.top_level_session_id
			    ) x ON
				    s.session_id = x.session_id
			    WHERE
				    s.recursion = 1;
		    END;

		    IF
			    @get_task_info = 2
			    AND @output_column_list LIKE '%|[additional_info|]%' ESCAPE '|'
			    AND @recursion = 1
		    BEGIN;
			    CREATE TABLE #blocked_requests
			    (
				    session_id SMALLINT NOT NULL,
				    request_id INT NOT NULL,
				    database_name sysname NOT NULL,
				    object_id INT,
				    hobt_id BIGINT,
				    schema_id INT,
				    schema_name sysname NULL,
				    object_name sysname NULL,
				    query_error NVARCHAR(2048),
				    PRIMARY KEY (database_name, session_id, request_id)
			    );

			    CREATE STATISTICS s_database_name ON #blocked_requests (database_name)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_schema_name ON #blocked_requests (schema_name)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_object_name ON #blocked_requests (object_name)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
			    CREATE STATISTICS s_query_error ON #blocked_requests (query_error)
			    WITH SAMPLE 0 ROWS, NORECOMPUTE;
		
			    INSERT #blocked_requests
			    (
				    session_id,
				    request_id,
				    database_name,
				    object_id,
				    hobt_id,
				    schema_id
			    )
			    SELECT
				    session_id,
				    request_id,
				    database_name,
				    object_id,
				    hobt_id,
				    CONVERT(INT, SUBSTRING(schema_node, CHARINDEX(' = ', schema_node) + 3, LEN(schema_node))) AS schema_id
			    FROM
			    (
				    SELECT
					    session_id,
					    request_id,
					    agent_nodes.agent_node.value('(database_name/text())[1]', 'sysname') AS database_name,
					    agent_nodes.agent_node.value('(object_id/text())[1]', 'int') AS object_id,
					    agent_nodes.agent_node.value('(hobt_id/text())[1]', 'bigint') AS hobt_id,
					    agent_nodes.agent_node.value('(metadata_resource/text()[.="SCHEMA"]/../../metadata_class_id/text())[1]', 'varchar(100)') AS schema_node
				    FROM #sessions AS s
				    CROSS APPLY s.additional_info.nodes('//block_info') AS agent_nodes (agent_node)
				    WHERE
					    s.recursion = 1
			    ) AS t
			    WHERE
				    t.object_id IS NOT NULL
				    OR t.hobt_id IS NOT NULL
				    OR t.schema_node IS NOT NULL;
			
			    DECLARE blocks_cursor
			    CURSOR LOCAL FAST_FORWARD
			    FOR
				    SELECT DISTINCT
					    database_name
				    FROM #blocked_requests;
				
			    OPEN blocks_cursor;
			
			    FETCH NEXT FROM blocks_cursor
			    INTO 
				    @database_name;
			
			    WHILE @@FETCH_STATUS = 0
			    BEGIN;
				    BEGIN TRY;
					    SET @sql_n = 
						    CONVERT(NVARCHAR(MAX), '') +
						    'UPDATE b ' +
						    'SET ' +
							    'b.schema_name = ' +
								    'REPLACE ' +
								    '( ' +
									    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
									    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
									    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
										    's.name COLLATE Latin1_General_Bin2, ' +
										    'NCHAR(31),N''?''),NCHAR(30),N''?''),NCHAR(29),N''?''),NCHAR(28),N''?''),NCHAR(27),N''?''),NCHAR(26),N''?''),NCHAR(25),N''?''),NCHAR(24),N''?''),NCHAR(23),N''?''),NCHAR(22),N''?''), ' +
										    'NCHAR(21),N''?''),NCHAR(20),N''?''),NCHAR(19),N''?''),NCHAR(18),N''?''),NCHAR(17),N''?''),NCHAR(16),N''?''),NCHAR(15),N''?''),NCHAR(14),N''?''),NCHAR(12),N''?''), ' +
										    'NCHAR(11),N''?''),NCHAR(8),N''?''),NCHAR(7),N''?''),NCHAR(6),N''?''),NCHAR(5),N''?''),NCHAR(4),N''?''),NCHAR(3),N''?''),NCHAR(2),N''?''),NCHAR(1),N''?''), ' +
									    'NCHAR(0), ' +
									    N''''' ' +
								    '), ' +
							    'b.object_name = ' +
								    'REPLACE ' +
								    '( ' +
									    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
									    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
									    'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE( ' +
										    'o.name COLLATE Latin1_General_Bin2, ' +
										    'NCHAR(31),N''?''),NCHAR(30),N''?''),NCHAR(29),N''?''),NCHAR(28),N''?''),NCHAR(27),N''?''),NCHAR(26),N''?''),NCHAR(25),N''?''),NCHAR(24),N''?''),NCHAR(23),N''?''),NCHAR(22),N''?''), ' +
										    'NCHAR(21),N''?''),NCHAR(20),N''?''),NCHAR(19),N''?''),NCHAR(18),N''?''),NCHAR(17),N''?''),NCHAR(16),N''?''),NCHAR(15),N''?''),NCHAR(14),N''?''),NCHAR(12),N''?''), ' +
										    'NCHAR(11),N''?''),NCHAR(8),N''?''),NCHAR(7),N''?''),NCHAR(6),N''?''),NCHAR(5),N''?''),NCHAR(4),N''?''),NCHAR(3),N''?''),NCHAR(2),N''?''),NCHAR(1),N''?''), ' +
									    'NCHAR(0), ' +
									    N''''' ' +
								    ') ' +
						    'FROM #blocked_requests AS b ' +
						    'LEFT OUTER JOIN ' + QUOTENAME(@database_name) + '.sys.partitions AS p ON ' +
							    'p.hobt_id = b.hobt_id ' +
						    'LEFT OUTER JOIN ' + QUOTENAME(@database_name) + '.sys.objects AS o ON ' +
							    'o.object_id = COALESCE(p.object_id, b.object_id) ' +
						    'LEFT OUTER JOIN ' + QUOTENAME(@database_name) + '.sys.schemas AS s ON ' +
							    's.schema_id = COALESCE(o.schema_id, b.schema_id) ' +
						    'WHERE ' +
							    'b.database_name = @database_name; ';
					
					    EXEC sp_executesql
						    @sql_n,
						    N'@database_name sysname',
						    @database_name;
				    END TRY
				    BEGIN CATCH;
					    UPDATE #blocked_requests
					    SET
						    query_error = 
							    REPLACE
							    (
								    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
								    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
								    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									    CONVERT
									    (
										    NVARCHAR(MAX), 
										    ERROR_MESSAGE() COLLATE Latin1_General_Bin2
									    ),
									    NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
									    NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
									    NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
								    NCHAR(0),
								    N''
							    )
					    WHERE
						    database_name = @database_name;
				    END CATCH;

				    FETCH NEXT FROM blocks_cursor
				    INTO
					    @database_name;
			    END;
			
			    CLOSE blocks_cursor;
			    DEALLOCATE blocks_cursor;
			
			    UPDATE s
			    SET
				    additional_info.modify
				    ('
					    insert <schema_name>{sql:column("b.schema_name")}</schema_name>
					    as last
					    into (/additional_info/block_info)[1]
				    ')
			    FROM #sessions AS s
			    INNER JOIN #blocked_requests AS b ON
				    b.session_id = s.session_id
				    AND b.request_id = s.request_id
				    AND s.recursion = 1
			    WHERE
				    b.schema_name IS NOT NULL;

			    UPDATE s
			    SET
				    additional_info.modify
				    ('
					    insert <object_name>{sql:column("b.object_name")}</object_name>
					    as last
					    into (/additional_info/block_info)[1]
				    ')
			    FROM #sessions AS s
			    INNER JOIN #blocked_requests AS b ON
				    b.session_id = s.session_id
				    AND b.request_id = s.request_id
				    AND s.recursion = 1
			    WHERE
				    b.object_name IS NOT NULL;

			    UPDATE s
			    SET
				    additional_info.modify
				    ('
					    insert <query_error>{sql:column("b.query_error")}</query_error>
					    as last
					    into (/additional_info/block_info)[1]
				    ')
			    FROM #sessions AS s
			    INNER JOIN #blocked_requests AS b ON
				    b.session_id = s.session_id
				    AND b.request_id = s.request_id
				    AND s.recursion = 1
			    WHERE
				    b.query_error IS NOT NULL;
		    END;

		    IF
			    @output_column_list LIKE '%|[program_name|]%' ESCAPE '|'
			    AND @output_column_list LIKE '%|[additional_info|]%' ESCAPE '|'
			    AND @recursion = 1
		    BEGIN;
			    DECLARE @job_id UNIQUEIDENTIFIER;
			    DECLARE @step_id INT;

			    DECLARE agent_cursor
			    CURSOR LOCAL FAST_FORWARD
			    FOR 
				    SELECT
					    s.session_id,
					    agent_nodes.agent_node.value('(job_id/text())[1]', 'uniqueidentifier') AS job_id,
					    agent_nodes.agent_node.value('(step_id/text())[1]', 'int') AS step_id
				    FROM #sessions AS s
				    CROSS APPLY s.additional_info.nodes('//agent_job_info') AS agent_nodes (agent_node)
				    WHERE
					    s.recursion = 1
			    OPTION (KEEPFIXED PLAN);
			
			    OPEN agent_cursor;

			    FETCH NEXT FROM agent_cursor
			    INTO 
				    @session_id,
				    @job_id,
				    @step_id;

			    WHILE @@FETCH_STATUS = 0
			    BEGIN;
				    BEGIN TRY;
					    DECLARE @job_name sysname;
					    SET @job_name = NULL;
					    DECLARE @step_name sysname;
					    SET @step_name = NULL;
					
					    SELECT
						    @job_name = 
							    REPLACE
							    (
								    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
								    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
								    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									    j.name,
									    NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
									    NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
									    NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
								    NCHAR(0),
								    N'?'
							    ),
						    @step_name = 
							    REPLACE
							    (
								    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
								    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
								    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									    s.step_name,
									    NCHAR(31),N'?'),NCHAR(30),N'?'),NCHAR(29),N'?'),NCHAR(28),N'?'),NCHAR(27),N'?'),NCHAR(26),N'?'),NCHAR(25),N'?'),NCHAR(24),N'?'),NCHAR(23),N'?'),NCHAR(22),N'?'),
									    NCHAR(21),N'?'),NCHAR(20),N'?'),NCHAR(19),N'?'),NCHAR(18),N'?'),NCHAR(17),N'?'),NCHAR(16),N'?'),NCHAR(15),N'?'),NCHAR(14),N'?'),NCHAR(12),N'?'),
									    NCHAR(11),N'?'),NCHAR(8),N'?'),NCHAR(7),N'?'),NCHAR(6),N'?'),NCHAR(5),N'?'),NCHAR(4),N'?'),NCHAR(3),N'?'),NCHAR(2),N'?'),NCHAR(1),N'?'),
								    NCHAR(0),
								    N'?'
							    )
					    FROM msdb.dbo.sysjobs AS j
					    INNER JOIN msdb..sysjobsteps AS s ON
						    j.job_id = s.job_id
					    WHERE
						    j.job_id = @job_id
						    AND s.step_id = @step_id;

					    IF @job_name IS NOT NULL
					    BEGIN;
						    UPDATE s
						    SET
							    additional_info.modify
							    ('
								    insert text{sql:variable("@job_name")}
								    into (/additional_info/agent_job_info/job_name)[1]
							    ')
						    FROM #sessions AS s
						    WHERE 
							    s.session_id = @session_id
						    OPTION (KEEPFIXED PLAN);
						
						    UPDATE s
						    SET
							    additional_info.modify
							    ('
								    insert text{sql:variable("@step_name")}
								    into (/additional_info/agent_job_info/step_name)[1]
							    ')
						    FROM #sessions AS s
						    WHERE 
							    s.session_id = @session_id
						    OPTION (KEEPFIXED PLAN);
					    END;
				    END TRY
				    BEGIN CATCH;
					    DECLARE @msdb_error_message NVARCHAR(256);
					    SET @msdb_error_message = ERROR_MESSAGE();
				
					    UPDATE s
					    SET
						    additional_info.modify
						    ('
							    insert <msdb_query_error>{sql:variable("@msdb_error_message")}</msdb_query_error>
							    as last
							    into (/additional_info/agent_job_info)[1]
						    ')
					    FROM #sessions AS s
					    WHERE 
						    s.session_id = @session_id
						    AND s.recursion = 1
					    OPTION (KEEPFIXED PLAN);
				    END CATCH;

				    FETCH NEXT FROM agent_cursor
				    INTO 
					    @session_id,
					    @job_id,
					    @step_id;
			    END;

			    CLOSE agent_cursor;
			    DEALLOCATE agent_cursor;
		    END; 
		
		    IF 
			    @delta_interval > 0 
			    AND @recursion <> 1
		    BEGIN;
			    SET @recursion = 1;

			    DECLARE @delay_time CHAR(12);
			    SET @delay_time = CONVERT(VARCHAR, DATEADD(second, @delta_interval, 0), 114);
			    WAITFOR DELAY @delay_time;

			    GOTO REDO;
		    END;
	    END;

	    SET @sql = 
		    --Outer column list
		    CONVERT
		    (
			    VARCHAR(MAX),
			    CASE
				    WHEN 
					    @destination_table <> '' 
					    AND @return_schema = 0 
						    THEN 'INSERT ' + @destination_table + ' '
				    ELSE ''
			    END +
			    'SELECT ' +
				    @output_column_list + ' ' +
			    CASE @return_schema
				    WHEN 1 THEN 'INTO #session_schema '
				    ELSE ''
			    END
		    --End outer column list
		    ) + 
		    --Inner column list
		    CONVERT
		    (
			    VARCHAR(MAX),
			    'FROM ' +
			    '( ' +
				    'SELECT ' +
					    'session_id, ' +
					    --[dd hh:mm:ss.mss]
					    CASE @format_output
						    WHEN 1 THEN
							    'CASE ' +
								    'WHEN elapsed_time < 0 THEN ' +
									    'RIGHT ' +
									    '( ' +
										    'REPLICATE(''0'', max_elapsed_length) + CONVERT(VARCHAR, (-1 * elapsed_time) / 86400), ' +
										    'max_elapsed_length ' +
									    ') + ' +
										    'RIGHT ' +
										    '( ' +
											    'CONVERT(VARCHAR, DATEADD(second, (-1 * elapsed_time), 0), 120), ' +
											    '9 ' +
										    ') + ' +
										    '''.000'' ' +
								    'ELSE ' +
									    'RIGHT ' +
									    '( ' +
										    'REPLICATE(''0'', max_elapsed_length) + CONVERT(VARCHAR, elapsed_time / 86400000), ' +
										    'max_elapsed_length ' +
									    ') + ' +
										    'RIGHT ' +
										    '( ' +
											    'CONVERT(VARCHAR, DATEADD(second, elapsed_time / 1000, 0), 120), ' +
											    '9 ' +
										    ') + ' +
										    '''.'' + ' + 
										    'RIGHT(''000'' + CONVERT(VARCHAR, elapsed_time % 1000), 3) ' +
							    'END AS [dd hh:mm:ss.mss], '
						    ELSE
							    ''
					    END +
					    --[dd hh:mm:ss.mss (avg)] / avg_elapsed_time
					    CASE @format_output
						    WHEN 1 THEN 
							    'RIGHT ' +
							    '( ' +
								    '''00'' + CONVERT(VARCHAR, avg_elapsed_time / 86400000), ' +
								    '2 ' +
							    ') + ' +
								    'RIGHT ' +
								    '( ' +
									    'CONVERT(VARCHAR, DATEADD(second, avg_elapsed_time / 1000, 0), 120), ' +
									    '9 ' +
								    ') + ' +
								    '''.'' + ' +
								    'RIGHT(''000'' + CONVERT(VARCHAR, avg_elapsed_time % 1000), 3) AS [dd hh:mm:ss.mss (avg)], '
						    ELSE
							    'avg_elapsed_time, '
					    END +
					    --physical_io
					    CASE @format_output
						    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, physical_io))) OVER() - LEN(CONVERT(VARCHAR, physical_io))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, physical_io), 1), 19)) AS '
						    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, physical_io), 1), 19)) AS '
						    ELSE ''
					    END + 'physical_io, ' +
					    --reads
					    CASE @format_output
						    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, reads))) OVER() - LEN(CONVERT(VARCHAR, reads))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, reads), 1), 19)) AS '
						    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, reads), 1), 19)) AS '
						    ELSE ''
					    END + 'reads, ' +
					    --physical_reads
					    CASE @format_output
						    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, physical_reads))) OVER() - LEN(CONVERT(VARCHAR, physical_reads))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, physical_reads), 1), 19)) AS '
						    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, physical_reads), 1), 19)) AS '
						    ELSE ''
					    END + 'physical_reads, ' +
					    --writes
					    CASE @format_output
						    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, writes))) OVER() - LEN(CONVERT(VARCHAR, writes))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, writes), 1), 19)) AS '
						    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, writes), 1), 19)) AS '
						    ELSE ''
					    END + 'writes, ' +
					    --tempdb_allocations
					    CASE @format_output
						    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, tempdb_allocations))) OVER() - LEN(CONVERT(VARCHAR, tempdb_allocations))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tempdb_allocations), 1), 19)) AS '
						    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tempdb_allocations), 1), 19)) AS '
						    ELSE ''
					    END + 'tempdb_allocations, ' +
					    --tempdb_current
					    CASE @format_output
						    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, tempdb_current))) OVER() - LEN(CONVERT(VARCHAR, tempdb_current))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tempdb_current), 1), 19)) AS '
						    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tempdb_current), 1), 19)) AS '
						    ELSE ''
					    END + 'tempdb_current, ' +
					    --CPU
					    CASE @format_output
						    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, CPU))) OVER() - LEN(CONVERT(VARCHAR, CPU))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, CPU), 1), 19)) AS '
						    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, CPU), 1), 19)) AS '
						    ELSE ''
					    END + 'CPU, ' +
					    --context_switches
					    CASE @format_output
						    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, context_switches))) OVER() - LEN(CONVERT(VARCHAR, context_switches))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, context_switches), 1), 19)) AS '
						    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, context_switches), 1), 19)) AS '
						    ELSE ''
					    END + 'context_switches, ' +
					    --used_memory
					    CASE @format_output
						    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, used_memory))) OVER() - LEN(CONVERT(VARCHAR, used_memory))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, used_memory), 1), 19)) AS '
						    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, used_memory), 1), 19)) AS '
						    ELSE ''
					    END + 'used_memory, ' +
					    --physical_io_delta			
					    'CASE ' +
						    'WHEN ' +
							    'first_request_start_time = last_request_start_time ' + 
							    'AND num_events = 2 ' +
							    'AND physical_io_delta >= 0 ' +
								    'THEN ' +
								    CASE @format_output
									    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, physical_io_delta))) OVER() - LEN(CONVERT(VARCHAR, physical_io_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, physical_io_delta), 1), 19)) ' 
									    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, physical_io_delta), 1), 19)) '
									    ELSE 'physical_io_delta '
								    END +
						    'ELSE NULL ' +
					    'END AS physical_io_delta, ' +
					    --reads_delta
					    'CASE ' +
						    'WHEN ' +
							    'first_request_start_time = last_request_start_time ' + 
							    'AND num_events = 2 ' +
							    'AND reads_delta >= 0 ' +
								    'THEN ' +
								    CASE @format_output
									    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, reads_delta))) OVER() - LEN(CONVERT(VARCHAR, reads_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, reads_delta), 1), 19)) '
									    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, reads_delta), 1), 19)) '
									    ELSE 'reads_delta '
								    END +
						    'ELSE NULL ' +
					    'END AS reads_delta, ' +
					    --physical_reads_delta
					    'CASE ' +
						    'WHEN ' +
							    'first_request_start_time = last_request_start_time ' + 
							    'AND num_events = 2 ' +
							    'AND physical_reads_delta >= 0 ' +
								    'THEN ' +
								    CASE @format_output
									    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, physical_reads_delta))) OVER() - LEN(CONVERT(VARCHAR, physical_reads_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, physical_reads_delta), 1), 19)) '
									    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, physical_reads_delta), 1), 19)) '
									    ELSE 'physical_reads_delta '
								    END + 
						    'ELSE NULL ' +
					    'END AS physical_reads_delta, ' +
					    --writes_delta
					    'CASE ' +
						    'WHEN ' +
							    'first_request_start_time = last_request_start_time ' + 
							    'AND num_events = 2 ' +
							    'AND writes_delta >= 0 ' +
								    'THEN ' +
								    CASE @format_output
									    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, writes_delta))) OVER() - LEN(CONVERT(VARCHAR, writes_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, writes_delta), 1), 19)) '
									    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, writes_delta), 1), 19)) '
									    ELSE 'writes_delta '
								    END + 
						    'ELSE NULL ' +
					    'END AS writes_delta, ' +
					    --tempdb_allocations_delta
					    'CASE ' +
						    'WHEN ' +
							    'first_request_start_time = last_request_start_time ' + 
							    'AND num_events = 2 ' +
							    'AND tempdb_allocations_delta >= 0 ' +
								    'THEN ' +
								    CASE @format_output
									    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, tempdb_allocations_delta))) OVER() - LEN(CONVERT(VARCHAR, tempdb_allocations_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tempdb_allocations_delta), 1), 19)) '
									    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tempdb_allocations_delta), 1), 19)) '
									    ELSE 'tempdb_allocations_delta '
								    END + 
						    'ELSE NULL ' +
					    'END AS tempdb_allocations_delta, ' +
					    --tempdb_current_delta
					    --this is the only one that can (legitimately) go negative 
					    'CASE ' +
						    'WHEN ' +
							    'first_request_start_time = last_request_start_time ' + 
							    'AND num_events = 2 ' +
								    'THEN ' +
								    CASE @format_output
									    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, tempdb_current_delta))) OVER() - LEN(CONVERT(VARCHAR, tempdb_current_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tempdb_current_delta), 1), 19)) '
									    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tempdb_current_delta), 1), 19)) '
									    ELSE 'tempdb_current_delta '
								    END + 
						    'ELSE NULL ' +
					    'END AS tempdb_current_delta, ' +
					    --CPU_delta
					    'CASE ' +
						    'WHEN ' +
							    'first_request_start_time = last_request_start_time ' + 
							    'AND num_events = 2 ' +
								    'THEN ' +
									    'CASE ' +
										    'WHEN ' +
											    'thread_CPU_delta > CPU_delta ' +
											    'AND thread_CPU_delta > 0 ' +
												    'THEN ' +
													    CASE @format_output
														    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, thread_CPU_delta + CPU_delta))) OVER() - LEN(CONVERT(VARCHAR, thread_CPU_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, thread_CPU_delta), 1), 19)) '
														    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, thread_CPU_delta), 1), 19)) '
														    ELSE 'thread_CPU_delta '
													    END + 
										    'WHEN CPU_delta >= 0 THEN ' +
											    CASE @format_output
												    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, thread_CPU_delta + CPU_delta))) OVER() - LEN(CONVERT(VARCHAR, CPU_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, CPU_delta), 1), 19)) '
												    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, CPU_delta), 1), 19)) '
												    ELSE 'CPU_delta '
											    END + 
										    'ELSE NULL ' +
									    'END ' +
						    'ELSE ' +
							    'NULL ' +
					    'END AS CPU_delta, ' +
					    --context_switches_delta
					    'CASE ' +
						    'WHEN ' +
							    'first_request_start_time = last_request_start_time ' + 
							    'AND num_events = 2 ' +
							    'AND context_switches_delta >= 0 ' +
								    'THEN ' +
								    CASE @format_output
									    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, context_switches_delta))) OVER() - LEN(CONVERT(VARCHAR, context_switches_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, context_switches_delta), 1), 19)) '
									    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, context_switches_delta), 1), 19)) '
									    ELSE 'context_switches_delta '
								    END + 
						    'ELSE NULL ' +
					    'END AS context_switches_delta, ' +
					    --used_memory_delta
					    'CASE ' +
						    'WHEN ' +
							    'first_request_start_time = last_request_start_time ' + 
							    'AND num_events = 2 ' +
							    'AND used_memory_delta >= 0 ' +
								    'THEN ' +
								    CASE @format_output
									    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, used_memory_delta))) OVER() - LEN(CONVERT(VARCHAR, used_memory_delta))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, used_memory_delta), 1), 19)) '
									    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, used_memory_delta), 1), 19)) '
									    ELSE 'used_memory_delta '
								    END + 
						    'ELSE NULL ' +
					    'END AS used_memory_delta, ' +
					    --tasks
					    CASE @format_output
						    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, tasks))) OVER() - LEN(CONVERT(VARCHAR, tasks))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tasks), 1), 19)) AS '
						    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, tasks), 1), 19)) '
						    ELSE ''
					    END + 'tasks, ' +
					    'status, ' +
					    'wait_info, ' +
					    'locks, ' +
					    'tran_start_time, ' +
					    'LEFT(tran_log_writes, LEN(tran_log_writes) - 1) AS tran_log_writes, ' +
					    --open_tran_count
					    CASE @format_output
						    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, open_tran_count))) OVER() - LEN(CONVERT(VARCHAR, open_tran_count))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, open_tran_count), 1), 19)) AS '
						    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, open_tran_count), 1), 19)) AS '
						    ELSE ''
					    END + 'open_tran_count, ' +
					    --sql_command
					    CASE @format_output 
						    WHEN 0 THEN 'REPLACE(REPLACE(CONVERT(NVARCHAR(MAX), sql_command), ''<?query --''+CHAR(13)+CHAR(10), ''''), CHAR(13)+CHAR(10)+''--?>'', '''') AS '
						    ELSE ''
					    END + 'sql_command, ' +
					    --sql_text
					    CASE @format_output 
						    WHEN 0 THEN 'REPLACE(REPLACE(CONVERT(NVARCHAR(MAX), sql_text), ''<?query --''+CHAR(13)+CHAR(10), ''''), CHAR(13)+CHAR(10)+''--?>'', '''') AS '
						    ELSE ''
					    END + 'sql_text, ' +
					    'query_plan, ' +
					    'blocking_session_id, ' +
					    --blocked_session_count
					    CASE @format_output
						    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, blocked_session_count))) OVER() - LEN(CONVERT(VARCHAR, blocked_session_count))) + LEFT(CONVERT(CHAR(22), CONVERT(MONEY, blocked_session_count), 1), 19)) AS '
						    WHEN 2 THEN 'CONVERT(VARCHAR, LEFT(CONVERT(CHAR(22), CONVERT(MONEY, blocked_session_count), 1), 19)) AS '
						    ELSE ''
					    END + 'blocked_session_count, ' +
					    --percent_complete
					    CASE @format_output
						    WHEN 1 THEN 'CONVERT(VARCHAR, SPACE(MAX(LEN(CONVERT(VARCHAR, CONVERT(MONEY, percent_complete), 2))) OVER() - LEN(CONVERT(VARCHAR, CONVERT(MONEY, percent_complete), 2))) + CONVERT(CHAR(22), CONVERT(MONEY, percent_complete), 2)) AS '
						    WHEN 2 THEN 'CONVERT(VARCHAR, CONVERT(CHAR(22), CONVERT(MONEY, blocked_session_count), 1)) AS '
						    ELSE ''
					    END + 'percent_complete, ' +
					    'host_name, ' +
					    'login_name, ' +
					    'database_name, ' +
					    'program_name, ' +
					    'additional_info, ' +
					    'start_time, ' +
					    'login_time, ' +
					    'CASE ' +
						    'WHEN status = N''sleeping'' THEN NULL ' +
						    'ELSE request_id ' +
					    'END AS request_id, ' +
					    'GETDATE() AS collection_time '
		    --End inner column list
		    ) +
		    --Derived table and INSERT specification
		    CONVERT
		    (
			    VARCHAR(MAX),
				    'FROM ' +
				    '( ' +
					    'SELECT TOP(2147483647) ' +
						    '*, ' +
						    'CASE ' +
							    'MAX ' +
							    '( ' +
								    'LEN ' +
								    '( ' +
									    'CONVERT ' +
									    '( ' +
										    'VARCHAR, ' +
										    'CASE ' +
											    'WHEN elapsed_time < 0 THEN ' +
												    '(-1 * elapsed_time) / 86400 ' +
											    'ELSE ' +
												    'elapsed_time / 86400000 ' +
										    'END ' +
									    ') ' +
								    ') ' +
							    ') OVER () ' +
								    'WHEN 1 THEN 2 ' +
								    'ELSE ' +
									    'MAX ' +
									    '( ' +
										    'LEN ' +
										    '( ' +
											    'CONVERT ' +
											    '( ' +
												    'VARCHAR, ' +
												    'CASE ' +
													    'WHEN elapsed_time < 0 THEN ' +
														    '(-1 * elapsed_time) / 86400 ' +
													    'ELSE ' +
														    'elapsed_time / 86400000 ' +
												    'END ' +
											    ') ' +
										    ') ' +
									    ') OVER () ' +
						    'END AS max_elapsed_length, ' +
						    'MAX(physical_io * recursion) OVER (PARTITION BY session_id, request_id) + ' +
							    'MIN(physical_io * recursion) OVER (PARTITION BY session_id, request_id) AS physical_io_delta, ' +
						    'MAX(reads * recursion) OVER (PARTITION BY session_id, request_id) + ' +
							    'MIN(reads * recursion) OVER (PARTITION BY session_id, request_id) AS reads_delta, ' +
						    'MAX(physical_reads * recursion) OVER (PARTITION BY session_id, request_id) + ' +
							    'MIN(physical_reads * recursion) OVER (PARTITION BY session_id, request_id) AS physical_reads_delta, ' +
						    'MAX(writes * recursion) OVER (PARTITION BY session_id, request_id) + ' +
							    'MIN(writes * recursion) OVER (PARTITION BY session_id, request_id) AS writes_delta, ' +
						    'MAX(tempdb_allocations * recursion) OVER (PARTITION BY session_id, request_id) + ' +
							    'MIN(tempdb_allocations * recursion) OVER (PARTITION BY session_id, request_id) AS tempdb_allocations_delta, ' +
						    'MAX(tempdb_current * recursion) OVER (PARTITION BY session_id, request_id) + ' +
							    'MIN(tempdb_current * recursion) OVER (PARTITION BY session_id, request_id) AS tempdb_current_delta, ' +
						    'MAX(CPU * recursion) OVER (PARTITION BY session_id, request_id) + ' +
							    'MIN(CPU * recursion) OVER (PARTITION BY session_id, request_id) AS CPU_delta, ' +
						    'MAX(thread_CPU_snapshot * recursion) OVER (PARTITION BY session_id, request_id) + ' +
							    'MIN(thread_CPU_snapshot * recursion) OVER (PARTITION BY session_id, request_id) AS thread_CPU_delta, ' +
						    'MAX(context_switches * recursion) OVER (PARTITION BY session_id, request_id) + ' +
							    'MIN(context_switches * recursion) OVER (PARTITION BY session_id, request_id) AS context_switches_delta, ' +
						    'MAX(used_memory * recursion) OVER (PARTITION BY session_id, request_id) + ' +
							    'MIN(used_memory * recursion) OVER (PARTITION BY session_id, request_id) AS used_memory_delta, ' +
						    'MIN(last_request_start_time) OVER (PARTITION BY session_id, request_id) AS first_request_start_time, ' +
						    'COUNT(*) OVER (PARTITION BY session_id, request_id) AS num_events ' +
					    'FROM #sessions AS s1 ' +
					    CASE 
						    WHEN @sort_order = '' THEN ''
						    ELSE
							    'ORDER BY ' +
								    @sort_order
					    END +
				    ') AS s ' +
				    'WHERE ' +
					    's.recursion = 1 ' +
			    ') x ' +
			    'OPTION (KEEPFIXED PLAN); ' +
			    '' +
			    CASE @return_schema
				    WHEN 1 THEN
					    'SET @schema = ' +
						    '''CREATE TABLE <table_name> ( '' + ' +
							    'STUFF ' +
							    '( ' +
								    '( ' +
									    'SELECT ' +
										    ''','' + ' +
										    'QUOTENAME(COLUMN_NAME) + '' '' + ' +
										    'DATA_TYPE + ' + 
										    'CASE ' +
											    'WHEN DATA_TYPE LIKE ''%char'' THEN ''('' + COALESCE(NULLIF(CONVERT(VARCHAR, CHARACTER_MAXIMUM_LENGTH), ''-1''), ''max'') + '') '' ' +
											    'ELSE '' '' ' +
										    'END + ' +
										    'CASE IS_NULLABLE ' +
											    'WHEN ''NO'' THEN ''NOT '' ' +
											    'ELSE '''' ' +
										    'END + ''NULL'' AS [text()] ' +
									    'FROM tempdb.INFORMATION_SCHEMA.COLUMNS ' +
									    'WHERE ' +
										    'TABLE_NAME = (SELECT name FROM tempdb.sys.objects WHERE object_id = OBJECT_ID(''tempdb..#session_schema'')) ' +
										    'ORDER BY ' +
											    'ORDINAL_POSITION ' +
									    'FOR XML ' +
										    'PATH('''') ' +
								    '), + ' +
								    '1, ' +
								    '1, ' +
								    ''''' ' +
							    ') + ' +
						    ''')''; ' 
				    ELSE ''
			    END
		    --End derived table and INSERT specification
		    );

	    SET @sql_n = CONVERT(NVARCHAR(MAX), @sql);

	    EXEC sp_executesql
		    @sql_n,
		    N'@schema VARCHAR(MAX) OUTPUT',
		    @schema OUTPUT;
        END;
"@

    }

    process {
       
        Invoke-Sqlcmd2 -ServerInstance $SqlCredHash.ServerInstance -Database Master -Credential $SqlCredHash.Credential  -Query $query

    }
      
}


function Test-WhoIsActivePresent
{
    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
  
    )

    process {

        $Query = "SELECT 1  FROM sys.procedures WHERE Name = 'sp_WhoIsActive' "   
       
        $result = Invoke-Sqlcmd2 -ServerInstance $SqlCredHash.ServerInstance -Database Master -Credential $SqlCredHash.Credential  -Query $query

        return $result

    }
      
}


function Get-WhoIsActiveLog 
{

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

        if($desc){ $query = "select * from WHOISACTIVE order by record_number desc,collection_time desc" }

        $result = Invoke-Sqlcmd2 @SqlCredHash -Query $query 

        return $result                
    }

}


function Remove-AppFailureTable 
{

    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
  
    )
    process {
   
        $query = "DROP TABLE APP_FAILURE"
        
        Invoke-Sqlcmd2 @SqlCredHash -Query $query

    }
    
} 


function Remove-WhoisActiveTable 
{

    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
  
    )
    process {
   
        $query = "DROP TABLE WHOISACTIVE"
        
        Invoke-Sqlcmd2 @SqlCredHash -Query $query

    }
    
} 


function Remove-WhoisActiveAppLockTable 
{

    [CmdletBinding()]
    Param
    (       
        [Parameter(Mandatory=$true)]
        [hashtable] 
        $SqlCredHash
  
    )
    process {
   
        $query = "DROP TABLE WHOISACTIVE_AppLock"
        
        Invoke-Sqlcmd2 @SqlCredHash -Query $query

    }
    
} 


function Remove-AllWhoIsActiveTables
 {

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


