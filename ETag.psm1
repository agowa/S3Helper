<#
    MIT License

    Copyright (c) 2020 Klaus Frank

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
#>

# Calculate S3 ETag value for local files to check if a local and a remote file match

function Calculate-BlockSize($filePath, $ETag) {
    $fileSize = (Get-Item -LiteralPath $filePath).Length
    if (-not $ETag.Contains('-')) {
        # Because the ETag does not contain a part count we know that it was not uploaded as multipart
        # Therefore set the startblock size to filesize + 1, as it is at least one byte bigger.
        return ($fileSize + 1)
    }

    $blockCount = [int]::Parse($ETag.Split('-')[-1])
    # Start with 1 MB e.g. 2^20 bytes
    $blockSizePow = 20
    $blockSize = [bigint]::Pow(2, $blockSizePow)
    # Last block is less than the block Size, all others are equal in Size
    while( [bigint]::Divide($fileSize, $blockSize) -gt $blockCount ) {
        $blockSizePow += 1
        $blockSize = [bigint]::Pow(2, $blockSizePow)
    }
    return $blockSize
}

function Get-MD5HashList($filePath, $blockSize, [switch]$UseSequentialAccess) {
    [byte[]] $binHash = @()
    [int]$chunks = 0

    try {
        [System.IO.FileStream]$reader = [System.IO.File]::OpenRead($filePath)
        [long]$fileSize = $reader.Length
        [bigint]$blockCount = [System.Math]::Ceiling([double]$filesize / [double]$blockSize)
        $maxThreads = [System.Environment]::ProcessorCount - 1
        if($UseSequentialAccess -or -not $reader.CanSeek) {
            # Sequential read / single threading
            $maxThreads = 1
        }

        if ($blockCount -lt $maxThreads) {
            $maxThreads = $blockCount
        }
        $chunkSize = [bigint]([System.Math]::Ceiling([double]$filesize / [double]$maxThreads))

        # Thread
        $threadScriptBlock = {
            param(
                [String]$filePath,
                [bigint]$chunkSize,
                [bigint]$blockSize,
                [int]$threadNr
            )
            [int]$threadChunks = 0
            [byte[]] $threadBinHash = @()
            try {
                $md5 = [System.Security.Cryptography.MD5CryptoServiceProvider]::new()
                $threadReader = [System.IO.File]::OpenRead($filePath)
                $threadBuf = [byte[]]::new($chunkSize)

                $threadReaderStartPosition = [bigint]::Multiply($threadNr, $chunkSize)
                $threadReaderEndPosition = [bigint]::Subtract([bigint]::Multiply($threadNr + 1, $chunkSize), 1)
                $null = $threadReader.Seek($threadReaderStartPosition, [System.IO.SeekOrigin]::Begin)

                while ($threadReader.Position -le $threadReaderEndPosition) {
                    $read_len = $threadReader.Read($threadBuf, 0, $threadBuf.Length)
                    $threadChunks += 1
                    $threadBinHash += $md5.ComputeHash($threadBuf, 0, $read_len)
                }
            } finally {
                $threadReader.Close()
                $threadReader.Dispose()
            }
            return $threadChunks, $threadBinHash
        }

        $runspacePool = [RunspaceFactory]::CreateRunspacePool(1, $maxThreads)
        $runspacePool.Open()
        [Object[]]$jobs = @()
        0..($maxThreads-1) | ForEach-Object {
            $powerShell = [powershell]::Create()
	        $powerShell.RunspacePool = $runspacePool
	        $null = $powerShell.AddScript($threadScriptBlock).AddArgument($filePath).AddArgument($chunkSize).AddArgument($blockSize).AddArgument($_)
	        $jobs += New-Object -TypeName psobject -Property @{
                threadNr = $_
                PowerShell = $powerShell
                asyncResult = $powerShell.BeginInvoke()
            }
        }
        while ($jobs.asyncresult.IsCompleted -contains $false) {
	        Start-Sleep -Milliseconds 100
        }
        foreach($job in $jobs) {
            [long]$threadChunks, [byte[]]$threadBinHash = $job.PowerShell.EndInvoke($job.asyncResult)
            $chunks += $threadChunks
            #[System.Array]::Copy($threadBinHash, 0, $binHash, $binHash.Length, $threadBinHash.Length)
            $binHash += $threadBinHash
            $job.PowerShell.Dispose()
        }
    } finally {
        $reader.Close()
        $reader.Dispose()
    }
    return $chunks, $binHash
}


function Get-ETag($filePath, $blockSize = [bigint]::Pow(2, 24), [Switch]$UseSequentialAccess) {
    # ToDo: Speedup multithreaded performance

    # blockSize is in bytes (e.g. 2^24 bytes == 16 MiB)
    [long]$chunks, [byte[]]$binHash = Get-MD5HashList -filePath $filePath -blockSize $blockSize -UseSequentialAccess:$UseSequentialAccess

    if ($chunks -eq 1) {
        return [System.BitConverter]::ToString($binHash).Replace('-','').ToLower()
    } else {
        #$Global:DbgBinHash = $binHash
        #[System.BitConverter]::ToString( $DbgBinHash[0..15] ).Replace('-','').ToLower()
        
        # Hash the hash list.
        $md5 = [System.Security.Cryptography.MD5CryptoServiceProvider]::new()
        $binHash = $md5.ComputeHash( $binHash )
        return [String]::Concat([System.BitConverter]::ToString($binHash).Replace('-','').ToLower(), '-', $chunks)
    }
}

function Test-ETag($filePath, $ETag) {
    $blockSize = Calculate-BlockSize -filePath $filePath -ETag $ETag
    $fileETag = Get-ETagPerBlock -filePath $filePath -blockSize $blockSize
    return $fileETag.Equals($ETag)
}


@(
    'Test-ETag',
    'Get-ETag'
) | Export-ModuleMember
