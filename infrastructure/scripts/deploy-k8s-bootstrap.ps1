[CmdletBinding()]
param(
    [string]$Namespace = "mlops",
    [string]$PostgresDb = "recsys",
    [string]$PostgresUser = "recsys",
    [string]$PostgresPassword = "",
    [string]$MinioRootUser = "",
    [string]$MinioRootPassword = "",
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

function Apply-ManifestPhase {
    param(
        [string]$Phase,
        [string[]]$Files
    )

    Write-Host "========================================"
    Write-Host " $Phase"
    Write-Host "========================================"
    foreach ($manifest in $Files) {
        kubectl apply -f $manifest
    }
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir

$infraManifests = @(
    (Join-Path $repoRoot "k8s\00-namespace.yaml"),
    (Join-Path $repoRoot "k8s\01-postgres-initdb.yaml"),
    (Join-Path $repoRoot "k8s\01-postgres.yaml"),
    (Join-Path $repoRoot "k8s\02-mlflow.yaml"),
    (Join-Path $repoRoot "k8s\04-minio.yaml"),
    (Join-Path $repoRoot "k8s\05-minio-init.yaml"),
    (Join-Path $repoRoot "k8s\06-adminer.yaml")
)

$configManifests = @(
    (Join-Path $repoRoot "k8s\postgres-initdb.yaml"),
    (Join-Path $repoRoot "k8s\data-configmap.yaml"),
    (Join-Path $repoRoot "k8s\online-service-configmap.yaml"),
    (Join-Path $repoRoot "k8s\simulator-configmap.yaml")
)

$appManifests = @(
    (Join-Path $repoRoot "k8s\11-online-service.yaml"),
    (Join-Path $repoRoot "k8s\12-simulator.yaml"),
    (Join-Path $repoRoot "k8s\13-data-api.yaml")
)

Require-Command -Name "kubectl"

if (-not $SkipSecretSetup) {
    if ([string]::IsNullOrWhiteSpace($PostgresPassword)) {
        throw "PostgresPassword is required unless -SkipSecretSetup is supplied."
    }
    if ([string]::IsNullOrWhiteSpace($MinioRootUser) -or [string]::IsNullOrWhiteSpace($MinioRootPassword)) {
        throw "MinioRootUser and MinioRootPassword are required unless -SkipSecretSetup is supplied."
    }

    kubectl create namespace $Namespace --dry-run=client -o yaml | kubectl apply -f -
    kubectl create secret generic postgres-secret `
        --namespace $Namespace `
        --from-literal=POSTGRES_DB=$PostgresDb `
        --from-literal=POSTGRES_USER=$PostgresUser `
        --from-literal=POSTGRES_PASSWORD=$PostgresPassword `
        --dry-run=client -o yaml | kubectl apply -f -
    kubectl create secret generic minio-secret `
        --namespace $Namespace `
        --from-literal=MINIO_ROOT_USER=$MinioRootUser `
        --from-literal=MINIO_ROOT_PASSWORD=$MinioRootPassword `
        --dry-run=client -o yaml | kubectl apply -f -
}
else {
    kubectl create namespace $Namespace --dry-run=client -o yaml | kubectl apply -f -
    Ensure-Secret -Name "postgres-secret"
    Ensure-Secret -Name "minio-secret"
}

kubectl delete job minio-init -n $Namespace --ignore-not-found | Out-Null

Apply-ManifestPhase -Phase "Phase 1: Infrastructure" -Files $infraManifests

kubectl rollout status statefulset/postgres -n $Namespace --timeout=$Timeout
kubectl rollout status deployment/mlflow -n $Namespace --timeout=$Timeout
kubectl rollout status deployment/minio -n $Namespace --timeout=$Timeout
kubectl rollout status deployment/adminer -n $Namespace --timeout=$Timeout
kubectl wait --for=condition=complete job/minio-init -n $Namespace --timeout=$Timeout

Apply-ManifestPhase -Phase "Phase 2: Config" -Files $configManifests
Apply-ManifestPhase -Phase "Phase 3: Applications" -Files $appManifests

kubectl rollout status deployment/api -n $Namespace --timeout=$Timeout
kubectl rollout status deployment/online-service-api -n $Namespace --timeout=$Timeout
kubectl rollout status deployment/online-service-worker -n $Namespace --timeout=$Timeout
kubectl rollout status deployment/simulator -n $Namespace --timeout=$Timeout

kubectl get pods -n $Namespace
kubectl get svc -n $Namespace
kubectl get pvc -n $Namespace
