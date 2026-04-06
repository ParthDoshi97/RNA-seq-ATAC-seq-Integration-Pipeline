# CONFIG
$S3_BUCKET  = "s3://multiomic-project-data/Geo_data"
$LOCAL_TEMP = "C:\Users\user\Downloads\geo_downloads"
$SRA_BIN    = "C:\Users\user\Downloads\sratoolkit\sratoolkit.3.4.1-win64\bin"
$THREADS    = (Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors

$DATASETS = @{
    "GSE147314" = "primary"
    "GSE183565" = "validation_1"
    "GSE181223" = "validation_2"
}

function Show-DiskSpace {
    $drive = Split-Path -Qualifier $LOCAL_TEMP
    $disk  = Get-PSDrive ($drive.TrimEnd(':'))
    $free  = [math]::Round($disk.Free / 1GB, 2)
    Write-Host "  [DISK] Free on ${drive}: ${free} GB" -ForegroundColor DarkGray
}

function Compress-Fastq {
    param($FilePath)
    $gzipGit = "C:\Program Files\Git\usr\bin\gzip.exe"
    $gzip7z  = "C:\Program Files\7-Zip\7z.exe"

    if (Test-Path $gzipGit) {
        & $gzipGit $FilePath
    }
    elseif (Test-Path $gzip7z) {
        & $gzip7z a -tgzip "$FilePath.gz" $FilePath | Out-Null
        Remove-Item $FilePath -Force
    }
    else {
        $dest       = "$FilePath.gz"
        $srcStream  = [System.IO.File]::OpenRead($FilePath)
        $destStream = [System.IO.File]::Create($dest)
        $gzStream   = [System.IO.Compression.GZipStream]::new($destStream, [System.IO.Compression.CompressionMode]::Compress)
        $srcStream.CopyTo($gzStream)
        $gzStream.Close()
        $destStream.Close()
        $srcStream.Close()
        Remove-Item $FilePath -Force
    }
}

# Correct 3-step approach: esearch(gds) -> elink(gds->sra) -> efetch(runinfo)
function Get-SRRList {
    param($Accession, $OutDir)

    $baseUrl = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

    # STEP A: esearch in GEO DataSets (gds) database
    Write-Host "  [A] esearch GDS for $Accession..." -ForegroundColor Yellow
    $searchResp = Invoke-RestMethod `
        -Uri "${baseUrl}/esearch.fcgi?db=gds&term=${Accession}&retmode=json&retmax=10" `
        -Method Get

    $gdsIds = $searchResp.esearchresult.idlist
    if (-not $gdsIds -or $gdsIds.Count -eq 0) {
        Write-Warning "  No GDS records found for $Accession"
        return @()
    }
    Write-Host "  [A] Found GDS IDs: $($gdsIds -join ', ')" -ForegroundColor Green

    # STEP B: elink from gds -> sra to get SRA UIDs
    Write-Host "  [B] elink gds->sra..." -ForegroundColor Yellow
    $idStr    = $gdsIds -join ","
    $linkResp = Invoke-RestMethod `
        -Uri "${baseUrl}/elink.fcgi?dbfrom=gds&db=sra&id=${idStr}&retmode=json" `
        -Method Get

    $sraUids = @()
    foreach ($linkSet in $linkResp.linksets) {
        foreach ($linkSetDb in $linkSet.linksetdbs) {
            if ($linkSetDb.dbto -eq "sra") {
                $sraUids += $linkSetDb.links
            }
        }
    }

    if ($sraUids.Count -eq 0) {
        Write-Warning "  No SRA UIDs linked from GDS for $Accession"
        return @()
    }
    Write-Host "  [B] Found $($sraUids.Count) SRA UID(s)" -ForegroundColor Green

    # STEP C: efetch runinfo CSV using SRA UIDs
    Write-Host "  [C] efetch runinfo CSV..." -ForegroundColor Yellow
    $sraIdStr   = $sraUids -join ","
    $efetchUrl  = "${baseUrl}/efetch.fcgi?db=sra&id=${sraIdStr}&rettype=runinfo&retmode=text"
    Invoke-WebRequest -Uri $efetchUrl -OutFile "$OutDir\runinfo.csv" -UseBasicParsing

    # Parse SRR run IDs from the CSV
    $srrList = Import-Csv "$OutDir\runinfo.csv" |
               Select-Object -ExpandProperty Run |
               Where-Object { $_ -match "^SRR" }

    return $srrList
}

function Download-GEO {
    param($Accession, $Label)

    $outDir = "$LOCAL_TEMP\$Accession"
    $s3Base = "$S3_BUCKET/$Label/$Accession"

    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "  Dataset : $Accession  =>  $Label"      -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan

    New-Item -ItemType Directory -Force -Path "$outDir\sra","$outDir\fastq","$outDir\suppl" | Out-Null

    # STEP 1: Fetch SRR run list
    Write-Host ""
    Write-Host "[1/4] Fetching SRR run list..." -ForegroundColor Yellow
    try {
        $srrList = Get-SRRList -Accession $Accession -OutDir $outDir
    }
    catch {
        Write-Warning "Could not fetch run list: $_"
        return
    }

    if (-not $srrList -or @($srrList).Count -eq 0) {
        Write-Warning "No SRR runs found for $Accession. Skipping."
        return
    }

    $total = @($srrList).Count
    Write-Host "  Found $total run(s): $($srrList -join ', ')" -ForegroundColor Green

    # STEP 2: Per-SRR: prefetch -> dump -> compress -> S3 -> delete
    Write-Host ""
    Write-Host "[2/4] Processing runs one by one..." -ForegroundColor Yellow

    $i = 0
    foreach ($srr in $srrList) {
        $i++
        Write-Host ""
        Write-Host "  --- Run $i of $total : $srr ---" -ForegroundColor Magenta
        Show-DiskSpace

        $sraDir  = "$outDir\sra\$srr"
        $sraFile = "$sraDir\$srr.sra"

        # prefetch
        Write-Host "  -> prefetch $srr..."
        & "$SRA_BIN\prefetch.exe" --max-size 100GB --output-directory "$outDir\sra" $srr

        if (-Not (Test-Path $sraFile)) {
            $sraFile = "$outDir\sra\$srr.sra"
        }
        if (-Not (Test-Path $sraFile)) {
            Write-Warning "  SRA file not found for $srr - skipping"
            continue
        }
        Write-Host "  [OK] prefetch done" -ForegroundColor Green
        Show-DiskSpace

        # fasterq-dump
        Write-Host "  -> fasterq-dump $srr (threads: $THREADS)..."
        & "$SRA_BIN\fasterq-dump.exe" `
            --outdir  "$outDir\fastq" `
            --temp    "$outDir\fastq" `
            --threads $THREADS `
            --split-3 `
            $sraFile

        # Delete SRA immediately to free space
        Write-Host "  -> Deleting SRA for $srr..."
        Remove-Item $sraDir  -Recurse -Force -ErrorAction SilentlyContinue
        Remove-Item $sraFile -Force          -ErrorAction SilentlyContinue
        Show-DiskSpace

        # Compress each FASTQ
        $fastqs = Get-ChildItem "$outDir\fastq\$srr*.fastq" -ErrorAction SilentlyContinue
        foreach ($fq in $fastqs) {
            Write-Host "  -> Compressing $($fq.Name)..."
            Compress-Fastq -FilePath $fq.FullName
        }
        Write-Host "  [OK] gzip done" -ForegroundColor Green
        Show-DiskSpace

        # Sync this SRR to S3 then delete local copy
        Write-Host "  -> Syncing $srr to S3..."
        aws s3 sync "$outDir\fastq" "$s3Base/fastq" `
            --exclude "*" `
            --include "$srr*.fastq.gz" `
            --storage-class STANDARD_IA `
            --no-progress

        Write-Host "  -> Deleting local FASTQs for $srr..."
        Get-ChildItem "$outDir\fastq\$srr*.fastq.gz" | Remove-Item -Force
        Write-Host "  [OK] $srr complete - local copy deleted" -ForegroundColor Green
        Show-DiskSpace
    }

    # STEP 3: Supplementary files
    Write-Host ""
    Write-Host "[3/4] Downloading supplementary files..." -ForegroundColor Yellow
    $ftpBase = "https://ftp.ncbi.nlm.nih.gov/geo/series/$($Accession.Substring(0,6))nnn/$Accession/suppl/"
    try {
        $page  = Invoke-WebRequest -Uri $ftpBase -UseBasicParsing
        $links = $page.Links | Where-Object { $_.href -match "\.(gz|tar|h5|h5ad|csv|tsv|bed|bw|txt|narrowPeak)$" }
        foreach ($link in $links) {
            $fileName = [System.IO.Path]::GetFileName($link.href)
            Write-Host "  -> $fileName"
            Invoke-WebRequest -Uri "$ftpBase$fileName" `
                              -OutFile "$outDir\suppl\$fileName" `
                              -UseBasicParsing
            aws s3 cp "$outDir\suppl\$fileName" "$s3Base/suppl/$fileName" `
                --storage-class STANDARD_IA --no-progress
            Remove-Item "$outDir\suppl\$fileName" -Force
        }
        Write-Host "  [OK] Supplementary files done" -ForegroundColor Green
    }
    catch {
        Write-Warning "Supplementary download failed: $_"
    }

    # STEP 4: Cleanup
    Write-Host ""
    Write-Host "[4/4] Cleaning up empty directories..." -ForegroundColor Yellow
    Remove-Item $outDir -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "  [OK] $Accession fully complete" -ForegroundColor Green
    Show-DiskSpace
}

# ENTRY POINT
New-Item -ItemType Directory -Force -Path $LOCAL_TEMP | Out-Null

Write-Host "GEO Download Pipeline" -ForegroundColor White
Write-Host "S3 destination : $S3_BUCKET" -ForegroundColor White
Write-Host "Local staging  : $LOCAL_TEMP" -ForegroundColor White
Write-Host "CPU threads    : $THREADS"    -ForegroundColor White
Show-DiskSpace

foreach ($entry in $DATASETS.GetEnumerator()) {
    Download-GEO -Accession $entry.Key -Label $entry.Value
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  ALL DATASETS COMPLETE" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
aws s3 ls $S3_BUCKET --recursive --human-readable | Select-Object -Last 20