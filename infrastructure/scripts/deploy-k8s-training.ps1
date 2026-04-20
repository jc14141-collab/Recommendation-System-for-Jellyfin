[CmdletBinding()]
param(
    [string]$Namespace = "mlops",
    [string]$Timeout = "300s"
)

$ErrorActionPreference = "Stop"

function Require-Command {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found in PATH."
    }
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$trainingDir = Join-Path $repoRoot "k8s\training"
$manifests = @(
    (Join-Path $repoRoot "k8s\14-training-config.yaml"),
    (Join-Path $repoRoot "k8s\15-training-manager.yaml"),
    (Join-Path $repoRoot "k8s\16-training-retrain-cronjob.yaml")
)

Require-Command -Name "kubectl"

if ((Get-Command sudo -ErrorAction SilentlyContinue) -and (Get-Command crictl -ErrorAction SilentlyContinue)) {
    $runtimeImages = sudo crictl images | Out-String
    if ($runtimeImages -notmatch "jellyfin-training") {
        throw "Training image docker.io/library/jellyfin-training:latest was not found in the node runtime. Run ./scripts/import-training-image.sh on the node first."
    }
}

kubectl get namespace $Namespace | Out-Null
kubectl get secret minio-secret -n $Namespace | Out-Null

kubectl kustomize $trainingDir *> $null
if ($LASTEXITCODE -eq 0) {
    kubectl apply -k $trainingDir
}
else {
    foreach ($manifest in $manifests) {
        kubectl apply -f $manifest
    }
}

kubectl rollout status deployment/training-manager -n $Namespace --timeout=$Timeout
kubectl get pods -n $Namespace
kubectl get svc -n $Namespace
kubectl get cronjob -n $Namespace
