# Calculate S3 ETag value for local files to check if a local and a remote file match

function Get-ETagPerBlock($filePath, $blockSize = [bigint]::Pow(2, 24)) {
    [int]$chunks = 0
    [byte[]] $binHash = @()

    $md5 = [System.Security.Cryptography.MD5CryptoServiceProvider]::new()

    try {
        $reader = [System.IO.File]::OpenRead($filePath)
        $buf = [byte[]]::new($blockSize)
        while ( $read_len = $reader.Read($buf,0,$buf.length) ){
            $chunks += 1
            $binHash += $md5.ComputeHash($buf,0,$read_len)
        }
    } finally {
        $reader.Close()
        $reader.Dispose()
    }

    if ($chunks -eq 1) {
        return [System.BitConverter]::ToString( $binHash ).Replace("-","").ToLower()
    }

    #$Global:DbgBinHash = $binHash
    #[System.BitConverter]::ToString( $DbgBinHash[0..15] ).Replace("-","").ToLower()

    # Hash the hash list.
    $binHash = $md5.ComputeHash( $binHash )
    
    return [String]::Concat( [System.BitConverter]::ToString( $binHash ).Replace("-","").ToLower(), "-", $chunks )
}

function Calculate-BlockSize($filePath, $ETag) {
    $fileSize = (Get-Item -LiteralPath $filePath).Length
    if (-not $ETag.Contains("-")) {
        # Because the ETag does not contain a part count we know that it was not uploaded as multipart
        # Therefore set the startblock size to filesize + 1, as it is at least one byte bigger.
        return ($fileSize + 1)
    }

    $blockCount = [int]::Parse($ETag.Split("-")[-1])
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



function Test-ETag($fileName, $ETag) {
    $blockSize = Calculate-BlockSize -filePath $fileName -ETag $ETag
    $fileETag = Get-ETagPerBlock -filePath $fileName -blockSize $blockSize
    return ($fileETag.Equals($ETag))
}

function Get-ETag($fileName, $blockSize) {
    return (Get-ETagPerBlock -filePath $fileName -blockSize $blockSize)
}

@(
    "Test-ETag",
    "Get-ETag"
) | Export-ModuleMember