#!/usr/bin/env python3

import argparse
import os
from pathlib import Path

import boto3
import torch
import torch.nn as nn
import yaml
from botocore.client import Config



class RecommenderMLP(nn.Module):
    def __init__(self, embedding_dim=384, hidden_dims=None, dropout=0.3):
        super().__init__()
        if hidden_dims is None:
            hidden_dims = [512, 256, 128]

        input_dim = embedding_dim * 2 + 3
        layers = []
        prev_dim = input_dim
        for h in hidden_dims:
            layers.extend([
                nn.Linear(prev_dim, h),
                nn.BatchNorm1d(h),
                nn.ReLU(),
                nn.Dropout(dropout),
            ])
            prev_dim = h
        layers.append(nn.Linear(prev_dim, 1))
        self.net = nn.Sequential(*layers)

    def forward(self, user_emb, movie_emb):
        cosine_sim = torch.sum(user_emb * movie_emb, dim=1, keepdim=True) / (
                torch.norm(user_emb, dim=1, keepdim=True) *
                torch.norm(movie_emb, dim=1, keepdim=True) + 1e-8
        )
        dot_product = torch.sum(user_emb * movie_emb, dim=1, keepdim=True)
        l2_dist = torch.norm(user_emb - movie_emb, dim=1, keepdim=True)
        x = torch.cat([user_emb, movie_emb, cosine_sim, dot_product, l2_dist], dim=1)
        return self.net(x).squeeze(1)


FEATURE_DIM = 384 * 2 + 3  # 771
EMBEDDING_DIM = 384



def build_s3_client(endpoint, access_key, secret_key, region="us-east-1"):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4"),
    )


def download_pt(s3, bucket, key, local_path: Path):
    print(f"[download] s3://{bucket}/{key} -> {local_path}")
    s3.download_file(bucket, key, str(local_path))


def upload_onnx(s3, local_path: Path, bucket, key):
    print(f"[upload]   {local_path} -> s3://{bucket}/{key}")
    s3.upload_file(str(local_path), bucket, key)



def export_onnx(pt_path: Path, onnx_path: Path,
                hidden_dims, dropout, embedding_dim=EMBEDDING_DIM):
    """Load .pt weights -> export .onnx with dynamic batch axis."""
    model = RecommenderMLP(
        embedding_dim=embedding_dim,
        hidden_dims=hidden_dims,
        dropout=dropout,
    )
    state = torch.load(pt_path, map_location="cpu", weights_only=True)

    remapped = {}
    for k, v in state.items():
        remapped[k if k.startswith("net.") else f"net.{k}"] = v
    model.load_state_dict(remapped)
    model.eval()

    dummy_user = torch.randn(4, embedding_dim, dtype=torch.float32)
    dummy_movie = torch.randn(4, embedding_dim, dtype=torch.float32)

    onnx_path.parent.mkdir(parents=True, exist_ok=True)
    torch.onnx.export(
        model,
        (dummy_user, dummy_movie),
        str(onnx_path),
        input_names=["user_embedding", "movie_embedding"],
        output_names=["scores"],
        dynamic_axes={
            "user_embedding": {0: "batch"},
            "movie_embedding": {0: "batch"},
            "scores": {0: "batch"},
        },
        opset_version=17,
    )
    print(f"[export]   ONNX saved to {onnx_path}")
    return onnx_path



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configs/config.yaml")
    # 可以直接传路径覆盖 config（方便 pipeline 脚本调用）
    parser.add_argument("--pt-path", default=None, help="Local .pt file; skip S3 download")
    parser.add_argument("--onnx-path", default="/tmp/model_mlp_best.onnx")
    parser.add_argument("--version-key", default=None, help="Override onnx_output.version_key")
    parser.add_argument("--latest-key", default=None, help="Override onnx_output.latest_key")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    s3_cfg = cfg["s3"]
    s3 = build_s3_client(
        endpoint=os.environ.get("S3_ENDPOINT", s3_cfg["endpoint"]),
        access_key=os.environ.get("S3_ACCESS_KEY", s3_cfg["access_key_id"]),
        secret_key=os.environ.get("S3_SECRET_KEY", s3_cfg["secret_access_key"]),
        region=s3_cfg.get("region", "us-east-1"),
    )

    model_params = cfg["models"]["mlp"]["params"]
    hidden_dims = model_params["hidden_dims"]
    dropout = model_params["dropout"]
    embedding_dim = cfg["data"].get("embedding_dim", 384)

    onnx_out = cfg.get("onnx_output", {})
    bucket = onnx_out.get("s3_bucket", cfg["model_output"]["s3_bucket"])
    version_key = args.version_key or onnx_out.get("version_key")
    latest_key = args.latest_key or onnx_out.get("latest_key")

    pt_path = Path(args.pt_path) if args.pt_path else Path("/tmp/model_mlp_best.pt")
    if not pt_path.exists():
        src_key = cfg.get("model_output", {}).get("latest_key", "models/mlp/latest/model_mlp_best.pt")
        src_bucket = cfg.get("model_output", {}).get("s3_bucket", bucket)
        download_pt(s3, src_bucket, src_key, pt_path)

    onnx_path = Path(args.onnx_path)
    export_onnx(pt_path, onnx_path, hidden_dims, dropout, embedding_dim)

    if version_key:
        upload_onnx(s3, onnx_path, bucket, version_key)
    if latest_key:
        upload_onnx(s3, onnx_path, bucket, latest_key)

    print("\n[done] .onnx uploaded to MinIO.")


if __name__ == "__main__":
    main()