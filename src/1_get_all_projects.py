import os
import pandas as pd
from utils.languages import LANGUAGES
from utils.folders_paths import aidev_path, results_01_path
from utils.boxplot import (
    export_q3plus_projects_csv,
)

os.makedirs(results_01_path, exist_ok=True)

# === Load datasets ===
repo_df = pd.read_csv(os.path.join(aidev_path, "repository.csv"))
pr_df = pd.read_csv(os.path.join(aidev_path, "pull_request.csv"))

# === Remove duplicate repositories and filter by language ===
repo_df = repo_df.drop_duplicates(subset="url", keep="first")
repo_df = repo_df[repo_df["language"].isin(LANGUAGES.keys())]

# === Keep only merged pull requests ===
pr_df_merged = pr_df[pr_df["merged_at"].notna()].copy()

# === Join PRs with repositories to retrieve project names and languages ===
merged_prs = pd.merge(
    pr_df_merged,              # Left DataFrame (contains 'repo_id')
    repo_df[["id", "url", "full_name", "language"]],            # Right DataFrame (contains 'id')
    how='inner',        # 'inner' keeps only rows that match in both tables
    left_on='repo_id',  # Column name in pr_df
    right_on='id'       # Column name in repo_df
)

unique_repos_count = merged_prs["url"].nunique()

print(
    f"Unique repositories by language {list(LANGUAGES.keys())} in AIDev: {unique_repos_count}"
)

# Check if there is at least one missing value in 'agent'
has_empty = merged_prs['agent'].isnull().any()

if has_empty:
    print("Yes, there are missing values.")
else:
    print("No, the column is full.")

# === Count merged PRs per project (grouped by language) ===
merged_prs_per_language = (
    merged_prs.groupby(["full_name", "language"])
    .size()
    .reset_index(name="num_prs")
    .sort_values(by=["language", "num_prs"], ascending=[True, False])
)

print("\n=== Number of MERGED PRs per project (grouped by language) ===")
print(merged_prs_per_language.head())

latest_pr_per_repo = (
    merged_prs.dropna(subset=["full_name", "language", "merged_at", "number"])
    .sort_values("merged_at")
    .groupby(["full_name", "language"], as_index=False)
    .tail(1)[["full_name", "language", "number", "merged_at"]]
    .rename(columns={"merged_at": "latest_merged_at"})
)

# Q3+
q3plus_projects = export_q3plus_projects_csv(merged_prs_per_language)
q3plus_projects = q3plus_projects.merge(
    latest_pr_per_repo, on=["full_name", "language"], how="left"
)
q3plus_outpath = os.path.join(results_01_path, "q3plus_projects_by_language.csv")
q3plus_projects.to_csv(q3plus_outpath, index=False)

