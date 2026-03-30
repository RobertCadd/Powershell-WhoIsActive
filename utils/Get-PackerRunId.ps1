Function Get-PackerRunId {
[CmdletBinding()]
    Param
    (
        [Parameter(Mandatory=$true)]
        [string]
        $PackerRuntimeLogLocation
    )
    process {
        if ($PackerRuntimeLogLocation -match 'packer_runtime-([^.]+)\.log') {
            return $Matches[1]
        }
        throw "Could not parse packer run ID from: $PackerRuntimeLogLocation"
    }
}
