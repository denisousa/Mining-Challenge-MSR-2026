import os
import pandas as pd
from utils.languages import LANGUAGES
from utils.folders_paths import aidev_path

# Load dataset files
repo_df = pd.read_csv(os.path.join(aidev_path, "repository.csv"))
pr_df = pd.read_csv(os.path.join(aidev_path, "pull_request.csv"))
sample_projects = pd.read_csv("rq1/sample_full_projects_merged_prs_rq1.csv")

# Remove duplicates
repo_df = repo_df.drop_duplicates(subset="url", keep="first")

# Keep repos in allowed languages
repo_df = repo_df[repo_df["language"].isin(LANGUAGES.keys())]

# Only keep the repositories that are present in the sample list
allowed_projects = set(sample_projects["full_name"].unique())
repo_df = repo_df[repo_df["full_name"].isin(allowed_projects)]

merged_prs = pr_df[pr_df["merged_at"].notna()].copy()

# Attach repo metadata to PRs
merged_prs = merged_prs.merge(
    repo_df[["url", "full_name", "language"]],
    left_on="repo_url",
    right_on="url",
    how="inner"
)

# Keep only PRs from allowed projects
merged_prs = merged_prs[merged_prs["full_name"].isin(allowed_projects)]

last_pr_per_project = (
    merged_prs.sort_values("merged_at")
    .groupby("full_name")
    .tail(1)              # last merged PR
    .reset_index(drop=True)
)

final_result = last_pr_per_project[[
    "full_name",
    "language",
    "id",        # PR ID
    "number",    # PR number
    "merged_at",
]]


print("\n=== LAST MERGED PR per PROJECT ===")
print(final_result)

output_path = "rq1/last_merged_pr_per_project_rq1.csv"
final_result.to_csv(output_path, index=False)

print(f"\nCSV saved as: {output_path}")
