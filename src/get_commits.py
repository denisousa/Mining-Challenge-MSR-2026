import os
import pandas as pd
from utils.languages import LANGUAGES
from utils.folders_paths import aidev_path, results_01_path
from dotenv import load_dotenv

load_dotenv()
token = os.getenv("GITHUB_TOKEN")

os.makedirs(results_01_path, exist_ok=True)

# === Load datasets ===
print("Loading datasets...")
repo_df = pd.read_csv(os.path.join(aidev_path, "repository.csv"))
pr_df = pd.read_csv(os.path.join(aidev_path, "pull_request.csv"))
pr_commits_df = pd.read_csv(os.path.join(aidev_path, "pr_commits.csv"))
balanced_prs_df = pd.read_csv(os.path.join(results_01_path, "balanced_repos_35_65.csv"))
human_prs_df = pd.read_csv(os.path.join(aidev_path, "human_pull_request.csv"))

# Create tuple of (full_name, last_merged_pr_date) for all repositories
repos = list(balanced_prs_df[["full_name", "last_merged_pr_date"]].itertuples(index=False, name=None))

print(f"\nTotal repositories: {len(repos)}")
print("\n=== Sample of repos with last merged PR date ===")
for i, (name, date) in enumerate(repos[:5]):
    print(f"  {name}: {date}")
print("  ...")
