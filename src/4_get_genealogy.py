import os
import pandas as pd
import subprocess
import logging
import requests
from dotenv import load_dotenv
from utils.folders_paths import repos_path, aidev_path, rq2_path
from utils.compute_time import timed
from utils.languages import LANGUAGES
from clone_genealogy.core import get_clone_genealogy

os.makedirs(rq2_path, exist_ok=True)

load_dotenv()  # Load environment variables from .env file
token = os.getenv("GITHUB_TOKEN")

# Get PR commits and related data
def get_pr_commits(repo, pr_id, token):
    url = f'https://api.github.com/repos/{repo}/pulls/{pr_id}/commits'
    headers = {'Authorization': f'token {token}'}
    response = requests.get(url, headers=headers)
    commits = response.json()
    merged_commit = commits[-1] if commits else None
    first_pr_commit = commits[0] if commits else None
    return merged_commit.get("sha"), first_pr_commit.get("sha")

# Main function to process the data
@timed()
def main():
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

    # === Load clone density ===
    csv_path = "rq1/clone_density_results_rq1.csv"
    df_clone_density = pd.read_csv(csv_path)

    # === Filter clone_density > 0 ===
    df_positive_clone = df_clone_density[df_clone_density["clone_density"] > 0].copy()

    merged_prs = merged_prs[
        merged_prs["full_name"].isin(df_positive_clone["full_name"])
    ].copy()

    # === Convert merged_prs into a dictionary grouped by full_name. ===
    merged_prs_dict = (
        merged_prs
        .groupby("full_name")
        .apply(lambda g: g.to_dict(orient="records"))
        .to_dict()
    )

    for full_name, pr_data_list in merged_prs_dict.items():
        methodology_commits = []
        for i, pr_data in enumerate(pr_data_list):
            print(f"Execution: {i} | total PRs in {full_name}: {len(merged_prs_dict.values())} | Total PRs {merged_prs.shape[0]}")
            full_name = pr_data["full_name"]
            pr_number = pr_data["number"]
            pr_language = pr_data["language"]

            repo_local_path = os.path.join(repos_path, full_name.replace("/", "_"))
            print(f"\n=== Processing {full_name} (PR #{pr_number}) ===")

            # Get merge commit using GitHub API
            print(f"  Resolving merge commit for PR #{pr_number} ...")
            try:
                merged_commit, first_pr_commit = get_pr_commits(full_name, pr_number, token)
                if not merged_commit:
                    raise ValueError(f"Could not resolve merge commit for PR #{pr_number}")
                print(f"  ✔ Merge commit found: {merged_commit}")
            except Exception as e:
                logging.error(f"({i}) Error resolving merge commit for {full_name} (PR #{pr_number}): {e}")
                continue

            # Run git rev-list to get the commit SHA
            print(f"  Getting commit SHA for {first_pr_commit} ...")
            try:
                # Execute the git rev-list command
                result = subprocess.run(
                    ["git", "rev-list", "--parents", "-n", "1", first_pr_commit],
                    cwd=repo_local_path,
                    check=True,
                    text=True,  # Capture the output as a string
                    stdout=subprocess.PIPE,  # Capture the standard output
                    stderr=subprocess.PIPE   # Capture the standard error
                )
                
                # Extract the commit SHA from the output
                parent_first_commit_pr = result.stdout.strip().split(" ")[-1]  # Remove leading/trailing whitespaces
                print(f"  ✔ Get parent commit from first PR commit SHA: {parent_first_commit_pr}")
                
            except subprocess.CalledProcessError as e:
                logging.error(f"({i}) Error getting commit SHA for {first_pr_commit} in {repo_local_path}: {e}")
                continue

            # Fetch the base commit
            print(f"  Fetch out commit {merged_commit} ...")
            try:
                subprocess.run(["git", "fetch", "origin", merged_commit], cwd=repo_local_path, check=True)
                print(f"  ✔ Checked out to commit {merged_commit}")
            except subprocess.CalledProcessError as e:
                logging.error(f"({i}) Error checking out commit {merged_commit} for {full_name}: {e}")
                continue

            # Checkout the base commit
            print(f"  Checking out commit {merged_commit} ...")
            try:
                subprocess.run(["git", "checkout", merged_commit], cwd=repo_local_path, check=True)
                print(f"  ✔ Checked out to commit {merged_commit}")
            except subprocess.CalledProcessError as e:
                logging.error(f"({i}) Error checking out commit {merged_commit} for {full_name}: {e}")
                continue

            methodology_commits.append({"commits": {"developer": parent_first_commit_pr, "coding_agent": merged_commit},
                                        "language": LANGUAGES[pr_language],
                                        "agent": pr_data["agent"]})


        if len(methodology_commits) <= 3:
            get_clone_genealogy(f"https://github.com/{full_name}", methodology_commits)

        print("\n=== All PRs processed ===")

# Execute main function
if __name__ == "__main__":
    main()
