import os
import pandas as pd
from utils.folders_paths import aidev_path, main_results
from dotenv import load_dotenv

load_dotenv()
token = os.getenv("GITHUB_TOKEN")
os.makedirs(main_results, exist_ok=True)

# === Load datasets ===
print("Loading datasets...")
repo_df = pd.read_csv(os.path.join(aidev_path, "repository.csv"))
pr_df = pd.read_csv(os.path.join(aidev_path, "pull_request.csv"))

pr_df_merged = pr_df[pr_df["merged_at"].notna()].copy()
pr_df_merged['pr_type'] = 'agent'

merged_prs = pd.merge(
    pr_df_merged,
    repo_df,
    left_on="repo_id",
    right_on="id",
    how="inner",
)

# Rename PR identifier column for clarity
if "id_x" in merged_prs.columns:
    merged_prs = merged_prs.rename(columns={"id_x": "id"})

output_csv = os.path.join(main_results, "new_agent_pull_request.csv")
merged_prs.to_csv(output_csv, index=False)