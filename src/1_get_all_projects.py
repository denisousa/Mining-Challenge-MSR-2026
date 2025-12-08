import os
import pandas as pd
from utils.languages import LANGUAGES
from utils.folders_paths import aidev_path, results_01_path
from utils.boxplot import (
    export_q3plus_projects_csv,
    enrich_projects_with_github_counts_until_date,
)

os.makedirs(results_01_path, exist_ok=True)

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
    how="left",
)

unique_repos_count = merged_prs["url"].nunique()

print(
    f"Unique repositories by language {list(LANGUAGES.keys())} in AIDev: {unique_repos_count}"
)


# === Count merged PRs per project (grouped by language) ===
merged_prs_per_language = (
    merged_prs.groupby(["full_name", "language"])
    .size()
    .reset_index(name="num_prs")
    .sort_values(by=["language", "num_prs"], ascending=[True, False])
)

# === Count merged PRs per project (grouped by language and agent) ===
merged_prs_per_language_and_agent = (
    merged_prs.groupby(["full_name", "language", "agent"])
    .size()
    .reset_index(name="num_prs")
    .sort_values(by=["language", "num_prs"], ascending=[True, False])
)

# Count unique agents per project
agent_counts = merged_prs.groupby("full_name")["agent"].nunique()

# Filter only those with > 1 agent
mixed_projects = agent_counts[agent_counts > 1]

print("Projects with multiple agents:")
print(mixed_projects)


latest_pr_per_repo = (
    merged_prs.dropna(subset=["full_name", "language", "merged_at", "number"])
    .sort_values("merged_at")
    .groupby(["full_name", "language"], as_index=False)
    .tail(1)[["full_name", "language", "number", "merged_at"]]
    .rename(columns={"merged_at": "latest_merged_at"})
)

all_agents = merged_prs['agent'].unique().tolist()

for agent_aidev in all_agents:
    df_agent = merged_prs_per_language_and_agent[merged_prs_per_language_and_agent["agent"] == agent_aidev]

    # Q3+
    q3plus_projects = export_q3plus_projects_csv(df_agent)
    q3plus_projects = q3plus_projects.merge(
        latest_pr_per_repo, on=["full_name", "language"], how="left"
    )
    q3plus_projects = enrich_projects_with_github_counts_until_date(
        q3plus_projects, date_col="latest_merged_at"
    )
    q3plus_outpath = os.path.join(results_01_path, f"q3plus_projects_by_{agent_aidev}_.csv")
    q3plus_projects.to_csv(q3plus_outpath, index=False)

    print("\n=== Number of MERGED PRs per project (grouped by language) ===")
    print(merged_prs_per_language.head())


# Q3+
q3plus_projects = export_q3plus_projects_csv(merged_prs_per_language)
q3plus_projects = q3plus_projects.merge(
    latest_pr_per_repo, on=["full_name", "language"], how="left"
)
q3plus_projects = enrich_projects_with_github_counts_until_date(
    q3plus_projects, date_col="latest_merged_at"
)
q3plus_outpath = os.path.join(results_01_path, "q3plus_projects_by_language.csv")
q3plus_projects.to_csv(q3plus_outpath, index=False)

print("\n=== Number of MERGED PRs per project (grouped by language) ===")
print(merged_prs_per_language.head())


# === Aggregate: number of projects and total merged PRs per language ===
language_summary = (
    merged_prs_per_language.groupby("language")
    .agg(num_projects=("full_name", "nunique"), total_prs=("num_prs", "sum"))
    .reset_index()
    .sort_values("language")
)

print("\n=== Summary: number of projects and total MERGED PRs per language ===")
print(language_summary)

# === Save CSV (full dataset) ===
output_path = f"{results_01_path}/projects_merged_prs.csv"
language_summary.to_csv(output_path, index=False)
print(f"\nCSV saved as: {output_path}")
