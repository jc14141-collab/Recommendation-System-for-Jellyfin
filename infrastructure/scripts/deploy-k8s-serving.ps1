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
$servingDir = Join-Path $repoRoot "k8s\serving"
$manifests = @(
    (Join-Path $repoRoot "k8s\17-serving-config.yaml"),
    (Join-Path $repoRoot "k8s\18-serving-multiworker.yaml")
)

Require-Command -Name "kubectl"

if ((Get-Command sudo -ErrorAction SilentlyContinue) -and (Get-Command crictl -ErrorAction SilentlyContinue)) {
    $runtimeImages = sudo crictl images | Out-String
    if ($runtimeImages -notmatch "project25-serving-multiworker") {
        throw "Serving image songchenxue/project25-serving-multiworker:latest was not found in the node runtime. Run ./scripts/import-serving-image.sh on the node first."
    }
}

kubectl get namespace $Namespace | Out-Null
kubectl get secret minio-secret -n $Namespace | Out-Null

kubectl kustomize $servingDir *> $null
if ($LASTEXITCODE -eq 0) {
    kubectl apply -k $servingDir
}
else {
    foreach ($manifest in $manifests) {
        kubectl apply -f $manifest
    }
}

kubectl rollout status deployment/recommender-serving -n $Namespace --timeout=$Timeout
kubectl get pods -n $Namespace -l app=recommender-serving
kubectl get svc -n $Namespace recommender-serving
