import pandas as pd
from utils.folders_paths import aidev_path
import os

os.makedirs(aidev_path, exist_ok=True)

# === Read datasets from Parquet ===
# repo_df = pd.read_parquet("hf://datasets/hao-li/AIDev/repository.parquet")
# pr_df = pd.read_parquet("hf://datasets/hao-li/AIDev/pull_request.parquet")
# pr_commits = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commits.parquet")
# pr_commit_details = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commit_details.parquet")
all_user_df = pd.read_parquet("hf://datasets/hao-li/AIDev/all_user.parquet")
human_pull_request_df = pd.read_parquet("hf://datasets/hao-li/AIDev/human_pull_request.parquet")
human_pr_task_type_df = pd.read_parquet("hf://datasets/hao-li/AIDev/human_pr_task_type.parquet")


# === Save as CSV ===
# repo_df.to_csv(os.path.join(aidev_path, "repository.csv"), index=False)
# pr_df.to_csv(os.path.join(aidev_path, "pull_request.csv"), index=False)
# pr_commits.to_csv(os.path.join(aidev_path, "pr_commits.csv"), index=False)
# pr_commit_details.to_csv(os.path.join(aidev_path, "pr_commit_details.csv"), index=False)
all_user_df.to_csv(os.path.join(aidev_path, "all_user.csv"), index=False)
human_pull_request_df.to_csv(os.path.join(aidev_path, "human_pull_request.csv"), index=False)
human_pr_task_type_df.to_csv(os.path.join(aidev_path, "human_pr_task_type.csv"), index=False)

print("CSV files have been saved successfully!")

# Save sample of 10 rows for inspection
human_pull_request_sample = human_pull_request_df.head(10)
human_pr_task_type_sample = human_pr_task_type_df.head(10)

# Save samples to CSV
human_pull_request_sample.to_csv(os.path.join(aidev_path, "sample_human_pull_request.csv"), index=False)
human_pr_task_type_sample.to_csv(os.path.join(aidev_path, "sample_human_pr_task_type.csv"), index=False)

print("\nSample CSV files (10 rows each) have been saved!")
print(f"- {os.path.join(aidev_path, 'sample_human_pull_request.csv')}")
print(f"- {os.path.join(aidev_path, 'sample_human_pr_task_type.csv')}")

print("\n=== Sample: human_pull_request (first 10 rows) ===")
print(human_pull_request_sample)

print("\n=== Sample: human_pr_task_type (first 10 rows) ===")
print(human_pr_task_type_sample)

# Extract repository names from repo_url
print("\n" + "="*80)
print("REPOSITORY AND PR STATISTICS")
print("="*80)

# Split repo_url by "repos" and get the last part
human_pull_request_df['repo_name'] = human_pull_request_df['repo_url'].apply(
    lambda x: x.split('repos/')[-1] if pd.notna(x) and 'repos/' in x else None
)

# Get unique repositories
unique_repos = human_pull_request_df['repo_name'].dropna().unique()
total_unique_repos = len(unique_repos)

# Get total number of Human-PRs
total_human_prs = len(human_pull_request_df)

print(f"\nTotal unique repositories: {total_unique_repos}")
print(f"Total Human-PRs: {total_human_prs}")

print("\n=== Sample of repository names ===")
print(unique_repos[:20])  # Show first 20 repo names