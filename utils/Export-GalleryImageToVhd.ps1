Function Export-GalleryImageToVhd {
[CmdletBinding()]
    Param
    (
        [Parameter(Mandatory=$true)]
        [string]
        $GalleryResourceGroup,

        [Parameter(Mandatory=$true)]
        [string]
        $GalleryName,

        [Parameter(Mandatory=$true)]
        [string]
        $ImageDefinition,

        [Parameter(Mandatory=$true)]
        [string]
        $ImageVersion,

        [Parameter(Mandatory=$true)]
        [string]
        $StorageAccountName,

        [Parameter(Mandatory=$true)]
        [string]
        $StorageAccountResourceGroup,

        [Parameter(Mandatory=$true)]
        [string]
        $VhdBlobName,

        [Parameter()]
        [string]
        $ContainerName = "system",

        [Parameter()]
        [string]
        $TenantId,

        [Parameter()]
        [string]
        $ApplicationId,

        [Parameter()]
        [string]
        $ClientSecret,

        [Parameter()]
        [string]
        $SubscriptionId,

        [Parameter()]
        [string]
        $TempDiskResourceGroup,

        [Parameter()]
        [int]
        $SasExpiryDurationSeconds = 3600
    )

    process {

        if (-not (Get-Command azcopy -ErrorAction SilentlyContinue)) {
            throw "azcopy was not found in PATH. Install azcopy and ensure it is accessible before running this function."
        }

        if ($TenantId -and $ApplicationId -and $ClientSecret) {
            Write-Verbose "Connecting to Azure as service principal $ApplicationId"
            $spnCredential = New-Object PSCredential($ApplicationId, (ConvertTo-SecureString $ClientSecret -AsPlainText -Force))
            Connect-AzAccount -ServicePrincipal -Credential $spnCredential -TenantId $TenantId | Out-Null
        }

        if ($SubscriptionId) {
            Write-Verbose "Setting subscription context to $SubscriptionId"
            Set-AzContext -SubscriptionId $SubscriptionId | Out-Null
        }

        if (-not $TempDiskResourceGroup) {
            $TempDiskResourceGroup = $GalleryResourceGroup
        }

        $disk         = $null
        $tempDiskName = "tmp-vhd-export-$([System.Guid]::NewGuid().ToString('N').Substring(0,8))"

        try {

            Write-Verbose "Retrieving image version $ImageVersion from gallery $GalleryName"
            $imageVersionObj = Get-AzGalleryImageVersion `
                -ResourceGroupName          $GalleryResourceGroup `
                -GalleryName                $GalleryName `
                -GalleryImageDefinitionName $ImageDefinition `
                -Name                       $ImageVersion

            Write-Verbose "Building managed disk configuration"
            $diskConfig = New-AzDiskConfig `
                -Location              $imageVersionObj.Location `
                -CreateOption          FromImage `
                -GalleryImageReference @{ Id = $imageVersionObj.Id }

            Write-Verbose "Creating temporary managed disk: $tempDiskName"
            $disk = New-AzDisk `
                -ResourceGroupName $TempDiskResourceGroup `
                -DiskName          $tempDiskName `
                -Disk              $diskConfig

            Write-Verbose "Granting SAS read access (expires in $SasExpiryDurationSeconds seconds)"
            $sas = Grant-AzDiskAccess `
                -ResourceGroupName $TempDiskResourceGroup `
                -DiskName          $tempDiskName `
                -Access            Read `
                -DurationInSecond  $SasExpiryDurationSeconds

            $blobPath = "Microsoft.Compute/Images/images/$VhdBlobName"

            Write-Verbose "Building destination SAS URL for $StorageAccountName/$ContainerName/$blobPath"
            $storageAccount = Get-AzStorageAccount `
                -ResourceGroupName $StorageAccountResourceGroup `
                -Name              $StorageAccountName

            $storageCtx = $storageAccount.Context

            $destSasToken = New-AzStorageBlobSASToken `
                -Container  $ContainerName `
                -Blob       $blobPath `
                -Permission rw `
                -ExpiryTime (Get-Date).AddSeconds($SasExpiryDurationSeconds) `
                -Context    $storageCtx `
                -FullUri

            Write-Verbose "Starting azcopy from disk SAS to blob destination"
            azcopy copy $sas.AccessSAS $destSasToken --blob-type PageBlob

            if ($LASTEXITCODE -ne 0) {
                throw "azcopy exited with code $LASTEXITCODE. See azcopy output above for details."
            }

            Write-Verbose "azcopy completed successfully"

            $blobUri = "$($storageAccount.PrimaryEndpoints.Blob)$ContainerName/$blobPath"

            return [PSCustomObject]@{
                BlobUri        = $blobUri
                StorageAccount = $StorageAccountName
                Container      = $ContainerName
                BlobName       = $blobPath
                ImageVersion   = $ImageVersion
            }
        }
        catch {
            Write-Error "Export-GalleryImageToVhd failed: $_"
            throw
        }
        finally {
            if ($disk) {
                Write-Verbose "Revoking SAS access on $tempDiskName"
                Revoke-AzDiskAccess `
                    -ResourceGroupName $TempDiskResourceGroup `
                    -DiskName          $tempDiskName `
                    -ErrorAction SilentlyContinue

                Write-Verbose "Removing temporary managed disk $tempDiskName"
                Remove-AzDisk `
                    -ResourceGroupName $TempDiskResourceGroup `
                    -DiskName          $tempDiskName `
                    -Force `
                    -ErrorAction SilentlyContinue
            }
        }
    }
}
