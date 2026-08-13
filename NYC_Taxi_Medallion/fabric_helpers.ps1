# Fabric REST helper functions using MSAL cache-based token acquisition.
# Dot-source this file: . .\fabric_helpers.ps1

$Script:PYTHON_EXE = if ($env:FABRIC_PYTHON_EXE) {
    $env:FABRIC_PYTHON_EXE
} else {
    "python"
}
$Script:TOKEN_SCRIPT = "$PSScriptRoot\fabric_token.py"
$Script:SKILL_HEADER_VALUE = "e2e-medallion-architecture"
$Script:FabricTokenCache = @{}

function Get-FabricAccessToken {
    param([Parameter(Mandatory=$true)][string]$Scope)
    if ($Script:FabricTokenCache.ContainsKey($Scope)) {
        $cached = $Script:FabricTokenCache[$Scope]
        if ($cached.expiry -gt (Get-Date).AddMinutes(2)) {
            return $cached.token
        }
    }
    $errorFile = [System.IO.Path]::GetTempFileName()
    try {
        $out = & $Script:PYTHON_EXE $Script:TOKEN_SCRIPT $Scope 2>$errorFile
        if ($LASTEXITCODE -ne 0) {
            $errtxt = Get-Content $errorFile -Raw
            throw "Token acquisition failed for scope $Scope. Exit=$LASTEXITCODE Err=$errtxt"
        }
    } finally {
        Remove-Item $errorFile -Force -ErrorAction SilentlyContinue
    }
    $token = ($out | Select-Object -Last 1).ToString().Trim()
    $Script:FabricTokenCache[$Scope] = @{ token = $token; expiry = (Get-Date).AddMinutes(50) }
    return $token
}

function Invoke-FabricApi {
    param(
        [Parameter(Mandatory=$true)][string]$Method,
        [Parameter(Mandatory=$true)][string]$Url,
        [string]$BodyFile,
        [hashtable]$ExtraHeaders
    )
    $token = Get-FabricAccessToken -Scope "https://api.fabric.microsoft.com/.default"
    $headers = @{
        "x-ms-fabric-skill" = $Script:SKILL_HEADER_VALUE
    }
    if ($ExtraHeaders) { foreach ($k in $ExtraHeaders.Keys) { $headers[$k] = $ExtraHeaders[$k] } }
    $params = @{
        Method = $Method
        Uri = $Url
        Headers = $headers
        Authentication = "Bearer"
        Token = (ConvertTo-SecureString $token -AsPlainText -Force)
        ContentType = "application/json"
        UseBasicParsing = $true
        SkipHttpErrorCheck = $true
    }
    if ($BodyFile) { $params["InFile"] = $BodyFile }
    $resp = Invoke-WebRequest @params
    return @{ StatusCode = [int]$resp.StatusCode; Headers = $resp.Headers; Body = $resp.Content }
}

function Wait-FabricLro {
    param(
        [Parameter(Mandatory=$true)]$InitialResponse,
        [int]$MaxWaitSeconds = 600
    )
    if ($InitialResponse.StatusCode -ne 202) {
        return $InitialResponse
    }
    $loc = $InitialResponse.Headers["Location"]
    if ($loc -is [array]) { $loc = $loc[0] }
    if (-not $loc) { return $InitialResponse }
    $retryAfter = $InitialResponse.Headers["Retry-After"]
    if ($retryAfter -is [array]) { $retryAfter = $retryAfter[0] }
    $wait = 5
    if ($retryAfter) { $wait = [int]$retryAfter }
    $elapsed = 0
    while ($elapsed -lt $MaxWaitSeconds) {
        Start-Sleep -Seconds $wait
        $elapsed += $wait
        $poll = Invoke-FabricApi -Method GET -Url $loc
        $j = $poll.Body | ConvertFrom-Json
        if ($j.status -eq "Succeeded") { return $poll }
        if ($j.status -eq "Failed") { return $poll }
        $wait = 5
    }
    return $poll
}

function Get-OneLakeToken {
    return Get-FabricAccessToken -Scope "https://storage.azure.com/.default"
}
