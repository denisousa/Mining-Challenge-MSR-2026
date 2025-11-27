import os
import pandas as pd
from utils.compute_sample import compute_sample
from utils.languages import LANGUAGES
from utils.folders_paths import aidev_path, rq1_path

os.makedirs(rq1_path, exist_ok=True)

# === Load datasets ===
repo_df = pd.read_csv(os.path.join(aidev_path, "repository.csv"))
pr_df = pd.read_csv(os.path.join(aidev_path, "pull_request.csv"))

# === Remove duplicate repositories and filter by language ===
repo_df = repo_df.drop_duplicates(subset="url", keep="first")
repo_df = repo_df[repo_df["language"].isin(LANGUAGES.keys())]

# === Keep only merged pull requests ===
merged_prs = pr_df[pr_df["merged_at"].notna()].copy()

# === Join PRs with repositories to retrieve project names and languages ===
merged_prs = merged_prs.merge(
    repo_df[["url", "full_name", "language"]],
    left_on="repo_url",
    right_on="url",
    how="left"
)

# === Count merged PRs per project (grouped by language) ===
merged_prs_per_project = (
    merged_prs.groupby(["full_name", "language"])
    .size()
    .reset_index(name="num_prs")
    .sort_values(by=["language", "num_prs"], ascending=[True, False])
)

print("\n=== Number of MERGED PRs per project (grouped by language) ===")
print(merged_prs_per_project.head())

# === Aggregate: number of projects and total merged PRs per language ===
language_summary = (
    merged_prs_per_project
    .groupby("language")
    .agg(
        num_projects=("full_name", "nunique"),
        total_prs=("num_prs", "sum")
    )
    .reset_index()
    .sort_values("language")
)

print("\n=== Summary: number of projects and total MERGED PRs per language ===")
print(language_summary)

# === Save CSV (full dataset) ===
output_path = "rq1/projects_merged_prs_rq1.csv"
language_summary.to_csv(output_path, index=False)
print(f"\nCSV saved as: {output_path}")

# ============================================================
# === SAMPLE COMPUTATION =====================================
# ============================================================

sample_merged_prs_per_project = compute_sample(merged_prs_per_project)

# === Save the full sample content (all projects + PR counts) ===
sample_full_output_path = "rq1/sample_full_projects_merged_prs_rq1.csv"
sample_merged_prs_per_project.to_csv(sample_full_output_path, index=False)
print(f"\nFull SAMPLE dataset saved as: {sample_full_output_path}")

# === Aggregate sample dataset ===
sample_language_summary = (
    sample_merged_prs_per_project
    .groupby("language")
    .agg(
        num_projects=("full_name", "nunique"),
        total_prs=("num_prs", "sum")
    )
    .reset_index()
    .sort_values("language")
)

print("\n=== SAMPLE | Summary: number of projects and total MERGED PRs per language ===")
print(sample_language_summary)

# Save CSV (sample summary)
output_path = "rq1/sample_projects_merged_prs_rq1.csv"
sample_language_summary.to_csv(output_path, index=False)
print(f"\nCSV saved as: {output_path}")
