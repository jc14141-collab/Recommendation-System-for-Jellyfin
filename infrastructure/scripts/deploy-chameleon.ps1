[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$PostgresPassword,

    [string]$MinioRootUser = "minioadmin",
    [string]$MinioRootPassword = "",

    [string]$FloatingIp = "129.114.25.219",
    [string]$SshUser = "cc",
    [string]$SshKeyPath = "$HOME\.ssh\id_rsa_chameleon",
    [string]$Namespace = "mlops",
    [string]$RemoteDir = "~/mlops-infra",
    [switch]$SkipK3sInstall
)

$ErrorActionPreference = "Stop"

function Require-Command {
    param([string]$Name)

    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found in PATH."
    }
}

Require-Command -Name "ssh"
Require-Command -Name "scp"

if (-not (Test-Path -LiteralPath $SshKeyPath)) {
    throw "SSH key not found: $SshKeyPath"
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$remoteTarget = "$SshUser@$FloatingIp"
$passwordBytes = [System.Text.Encoding]::UTF8.GetBytes($PostgresPassword)
$passwordB64 = [Convert]::ToBase64String($passwordBytes)
$minioPasswordValue = $MinioRootPassword
if ([string]::IsNullOrWhiteSpace($minioPasswordValue)) {
    $minioPasswordValue = -join ((48..57) + (65..90) + (97..122) | Get-Random -Count 20 | ForEach-Object { [char]$_ })
}
$minioPasswordBytes = [System.Text.Encoding]::UTF8.GetBytes($minioPasswordValue)
$minioPasswordB64 = [Convert]::ToBase64String($minioPasswordBytes)
$remoteCommandLines = @(
    "set -euo pipefail",
    "mkdir -p $RemoteDir"
)

if (-not $SkipK3sInstall) {
    $remoteCommandLines += "cd $RemoteDir"
    $remoteCommandLines += "chmod +x scripts/*.sh"
    $remoteCommandLines += "sudo ./scripts/install-k3s-server.sh"
}

$remoteCommandLines += @(
    "cd $RemoteDir",
    "chmod +x scripts/*.sh",
    "export NAMESPACE='$Namespace'",
    "export POSTGRES_PASSWORD=`"$(printf '%s' '$passwordB64' | base64 -d)`"",
    "export MINIO_ROOT_USER='$MinioRootUser'",
    "export MINIO_ROOT_PASSWORD=`"$(printf '%s' '$minioPasswordB64' | base64 -d)`"",
    "./scripts/create-postgres-secret.sh",
    "./scripts/create-minio-secret.sh",
    "sudo kubectl apply -f k8s/00-namespace.yaml",
    "sudo kubectl apply -f k8s/01-postgres.yaml",
    "sudo kubectl apply -f k8s/02-mlflow.yaml",
    "sudo kubectl apply -f k8s/04-minio.yaml",
    "sudo kubectl delete job minio-init -n $Namespace --ignore-not-found",
    "sudo kubectl apply -f k8s/05-minio-init.yaml",
    "sudo kubectl apply -f k8s/06-adminer.yaml",
    "sudo kubectl rollout status statefulset/postgres -n $Namespace --timeout=180s",
    "sudo kubectl rollout status deployment/mlflow -n $Namespace --timeout=180s",
    "sudo kubectl rollout status deployment/minio -n $Namespace --timeout=180s",
    "sudo kubectl rollout status deployment/adminer -n $Namespace --timeout=180s",
    "sudo kubectl wait --for=condition=complete job/minio-init -n $Namespace --timeout=180s",
    "sudo kubectl get pods -n $Namespace -o wide",
    "sudo kubectl get svc -n $Namespace",
    "sudo kubectl get pvc -n $Namespace"
)

$remoteCommand = ($remoteCommandLines -join "; ")

Write-Host "Syncing repository to $remoteTarget ..."
scp -i $SshKeyPath -o StrictHostKeyChecking=no -r `
    "$repoRoot\README.md" `
    "$repoRoot\docs" `
    "$repoRoot\k8s" `
    "$repoRoot\scripts" `
    "${remoteTarget}:$RemoteDir"

Write-Host "Running remote deployment on $FloatingIp ..."
ssh -i $SshKeyPath -o StrictHostKeyChecking=no $remoteTarget "bash -lc `"$remoteCommand`""
Write-Host "MinIO root user: $MinioRootUser"
Write-Host "MinIO root password: $minioPasswordValue"
