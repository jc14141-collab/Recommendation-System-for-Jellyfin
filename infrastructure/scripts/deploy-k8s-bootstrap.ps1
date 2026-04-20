[CmdletBinding()]
param(
    [string]$Namespace = "mlops",
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

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$bootstrapDir = Join-Path $repoRoot "k8s\bootstrap"
$manifests = @(
    (Join-Path $repoRoot "k8s\00-namespace.yaml"),
    (Join-Path $repoRoot "k8s\01-postgres-initdb.yaml"),
    (Join-Path $repoRoot "k8s\01-postgres.yaml"),
    (Join-Path $repoRoot "k8s\02-mlflow.yaml"),
    (Join-Path $repoRoot "k8s\03-jellyfin.yaml"),
    (Join-Path $repoRoot "k8s\04-minio.yaml"),
    (Join-Path $repoRoot "k8s\05-minio-init.yaml"),
    (Join-Path $repoRoot "k8s\06-adminer.yaml"),
    (Join-Path $repoRoot "k8s\07-compose-apps-template.yaml"),
    (Join-Path $repoRoot "k8s\07-data-role-components.yaml"),
    (Join-Path $repoRoot "k8s\08-training-role-components.yaml"),
    (Join-Path $repoRoot "k8s\09-serving-role-components.yaml"),
    (Join-Path $repoRoot "k8s\10-devops-platform-components.yaml")
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
kubectl kustomize $bootstrapDir *> $null
if ($LASTEXITCODE -eq 0) {
    kubectl apply -k $bootstrapDir
}
else {
    foreach ($manifest in $manifests) {
        kubectl apply -f $manifest
    }
}

kubectl rollout status statefulset/postgres -n $Namespace --timeout=$Timeout
kubectl rollout status deployment/mlflow -n $Namespace --timeout=$Timeout
kubectl rollout status deployment/jellyfin -n $Namespace --timeout=$Timeout
kubectl rollout status deployment/minio -n $Namespace --timeout=$Timeout
kubectl rollout status deployment/adminer -n $Namespace --timeout=$Timeout
kubectl wait --for=condition=complete job/minio-init -n $Namespace --timeout=$Timeout

kubectl get pods -n $Namespace
kubectl get svc -n $Namespace
kubectl get pvc -n $Namespace
