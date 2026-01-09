import pandas as pd
from utils.folders_paths import aidev_path
import os

os.makedirs(aidev_path, exist_ok=True)

# === Read datasets from Parquet ===
repo_df = pd.read_parquet("hf://datasets/hao-li/AIDev/repository.parquet")
pr_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pull_request.parquet")
pr_commits = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commits.parquet")
pr_humans = pd.read_parquet("hf://datasets/hao-li/AIDev/human_pull_request.parquet")

# === Save as CSV ===
repo_df.to_csv(os.path.join(aidev_path, "repository.csv"), index=False)
pr_df.to_csv(os.path.join(aidev_path, "pull_request.csv"), index=False)
pr_commits.to_csv(os.path.join(aidev_path, "pr_commits.csv"), index=False)
pr_humans.to_csv(os.path.join(aidev_path, "human_pull_request.csv"), index=False)


print("CSV files have been saved successfully!")