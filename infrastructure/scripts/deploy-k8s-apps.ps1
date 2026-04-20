[CmdletBinding()]
param(
    [string]$Namespace = "mlops",
    [string]$PostgresUser = "recsys",
    [string]$PostgresPassword = "",
    [string]$S3AccessKey = "",
    [string]$S3SecretKey = "",
    [string]$AdminPassword = "admin123",
    [string]$Timeout = "300s",
    [switch]$SkipSecretSetup
)

$ErrorActionPreference = "Stop"

function Require-Command {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found in PATH."
    }
}

function Ensure-Secret {
    param([string]$Name)
    kubectl get secret $Name -n $Namespace | Out-Null
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$appsDir = Join-Path $repoRoot "k8s\apps"
$manifests = @(
    (Join-Path $repoRoot "k8s\11-data-pipeline-config.yaml"),
    (Join-Path $repoRoot "k8s\12-online-service-config.yaml"),
    (Join-Path $repoRoot "k8s\13-api.yaml"),
    (Join-Path $repoRoot "k8s\14-online-service-api.yaml"),
    (Join-Path $repoRoot "k8s\15-training-manager.yaml")
)

Require-Command -Name "kubectl"

if (-not $SkipSecretSetup) {
    if ([string]::IsNullOrWhiteSpace($PostgresPassword)) {
        throw "PostgresPassword is required unless -SkipSecretSetup is supplied."
    }
    if ([string]::IsNullOrWhiteSpace($S3AccessKey) -or [string]::IsNullOrWhiteSpace($S3SecretKey)) {
        throw "S3AccessKey and S3SecretKey are required unless -SkipSecretSetup is supplied."
    }

    kubectl create namespace $Namespace --dry-run=client -o yaml | kubectl apply -f -
    kubectl create secret generic data-pipeline-secrets `
        --namespace $Namespace `
        --from-literal=POSTGRES_USER=$PostgresUser `
        --from-literal=POSTGRES_PASSWORD=$PostgresPassword `
        --from-literal=S3_ACCESS_KEY=$S3AccessKey `
        --from-literal=S3_SECRET_KEY=$S3SecretKey `
        --from-literal=ADMIN_PASSWORD=$AdminPassword `
        --dry-run=client -o yaml | kubectl apply -f -
}
else {
    kubectl create namespace $Namespace --dry-run=client -o yaml | kubectl apply -f -
    Ensure-Secret -Name "data-pipeline-secrets"
}

kubectl kustomize $appsDir *> $null
if ($LASTEXITCODE -eq 0) {
    kubectl apply -k $appsDir
}
else {
    foreach ($manifest in $manifests) {
        kubectl apply -f $manifest
    }
}

kubectl rollout status deployment/api -n $Namespace --timeout=$Timeout
kubectl rollout status deployment/online-service-api -n $Namespace --timeout=$Timeout
kubectl rollout status deployment/training-manager -n $Namespace --timeout=$Timeout

kubectl get pods -n $Namespace
kubectl get svc -n $Namespace
