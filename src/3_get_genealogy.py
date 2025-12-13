import os
import pandas as pd
from dotenv import load_dotenv
from utils.folders_paths import results_02_path, results_03_path
from utils.compute_time import timed
from utils.languages import LANGUAGES
from clone_genealogy.core import get_clone_genealogy

# ONLY_EXECUTE = ["featureform/enrichmcp", "jscarle/LightResults", "wieslawsoltes/Xaml.Behaviors"]
# ONLY_EXECUTE = ["featureform/enrichmcp", "jscarle/LightResults"]
# ONLY_EXECUTE = ["jscarle/LightResults"]

load_dotenv()
token = os.getenv("GITHUB_TOKEN") 

os.makedirs(results_03_path, exist_ok=True)

# Main function to process the data
@timed(results_03_path)
def main():
    # === Load projects_with_pr_sha.csv ===
    csv_path = os.path.join("02_results_copy", "projects_with_pr_sha.csv")
    df_prs = pd.read_csv(csv_path)

    print(f"Loaded {len(df_prs)} PRs from {csv_path}")
    print(f"Columns: {df_prs.columns.tolist()}")

    # === Group by full_name to process each project ===
    projects_grouped = df_prs.groupby("full_name")

    i = 0
    for full_name, project_prs in projects_grouped:
        i += 1

        if i != 1:
            continue
        
        total_prs = len(project_prs)
        
        print(f"\n=== Processing project: {full_name} ({total_prs} PRs) ===")
        
        # Loop through each PR in the project
        context_commits_by_project = []
        pr_idx = 0
        for pr_idx, row in project_prs.iterrows():
            pr_number = row["pr_number"]
            pr_language = LANGUAGES[row["language"]]
            sha = row["sha"]
            author = row.get("author", "")
            real_author = row.get("author_aidev", "")
            
            print(f"\n[{pr_idx}/{total_prs}] Processing {full_name} (PR #{pr_number})...")
            print(f"  SHA: {sha}")
            print(f"  Author: {author} | Agent: {real_author}")
            
            context_commits_by_project.append(
                {
                    "sha": sha,
                    "language": pr_language,
                    "author": real_author,
                    "pr_number": pr_number,
                    "project": full_name,
                }
            )

        # Process clone genealogy if we have commits
        print(f"\n  Processing clone genealogy for {full_name} ({len(context_commits_by_project)} commits)...")
        get_clone_genealogy(f"https://github.com/{full_name}", context_commits_by_project)

        print("\n=== All PRs processed ===")

# Execute main function
if __name__ == "__main__":
    main()
