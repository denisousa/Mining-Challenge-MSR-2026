import os
import logging
import pandas as pd
import requests
from dotenv import load_dotenv
from utils.folders_paths import aidev_path, results_01_path, results_02_path
from utils.languages import LANGUAGES

# Load environment variables
load_dotenv()
token = os.getenv("GITHUB_TOKEN")

# Ensure rq2 directory exists
os.makedirs(results_02_path, exist_ok=True)

# Set up logging
log_file = f'{results_02_path}/errors.log'
if os.path.exists(log_file):
    os.remove(log_file)  # Delete the existing log file when script runs again

logging.basicConfig(filename=log_file, level=logging.INFO)

def get_merged_prs(pr_df, repo_df):
    # === Remove duplicate repositories and filter by language ===
    repo_df = repo_df.drop_duplicates(subset="url", keep="first")
    repo_df = repo_df[repo_df["language"].isin(LANGUAGES.keys())]

    # === Keep only merged pull requests ===
    merged_prs = pr_df[pr_df["merged_at"].notna()].copy()

    # === Join PRs with repositories to retrieve project names and languages ===
    return merged_prs.merge(
        repo_df[["url", "full_name", "language"]],
        left_on="repo_url",
        right_on="url",
        how="left"
    )

def get_pr_merged_sha(repo, pr_number, token):
    """
    Get the merged commit SHA for a pull request.
    Returns the SHA of the last commit in the PR (merged commit).
    """
    url = f'https://api.github.com/repos/{repo}/pulls/{pr_number}/commits'
    headers = {'Authorization': f'token {token}'} if token else {}
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            commits = response.json()
            if commits:
                # Return the SHA of the last commit (merged commit)
                return commits[-1].get("sha")
        else:
            print(f"  Warning: Failed to fetch commits for {repo} PR #{pr_number} (Status: {response.status_code})")
            logging.error(f"Failed to fetch commits for {repo} PR #{pr_number} (Status: {response.status_code})")
            return None
    except Exception as e:
        print(f"  Error fetching commits for {repo} PR #{pr_number}: {e}")
        logging.error(f"Error fetching commits for {repo} PR #{pr_number}: {e}")
        return None

def get_pr_author(repo, pr_number, token):
    """
    Get the author (user) of a pull request via GitHub API.
    Returns the login username of the PR author.
    """
    url = f'https://api.github.com/repos/{repo}/pulls/{pr_number}'
    headers = {'Authorization': f'token {token}'} if token else {}
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            pr_data = response.json()
            user = pr_data.get("user")
            if user:
                return user.get("login")
        else:
            print(f"  Warning: Failed to fetch PR info for {repo} PR #{pr_number} (Status: {response.status_code})")
            logging.error(f"Failed to fetch PR info for {repo} PR #{pr_number} (Status: {response.status_code})")
            return None
    except Exception as e:
        print(f"  Error fetching PR info for {repo} PR #{pr_number}: {e}")
        logging.error(f"Error fetching PR info for {repo} PR #{pr_number}: {e}")
        return None

def get_all_merged_prs_until(df, repo, pr_id, language, token=None):
    """
    Get all merged pull requests for a GitHub repository up to a specific PR ID.
    """
    headers = {'Authorization': f'token {token}'} if token else {}
    prs_list = []
    page = 1
    per_page = 100
    
    print(f"Fetching merged PRs for {repo} up to PR #{pr_id}...")
    
    while True:
        # Fetch merged PRs from GitHub API
        url = f'https://api.github.com/repos/{repo}/pulls'
        params = {
            'state': 'closed',
            'sort': 'number',
            'direction': 'asc',
            'per_page': per_page,
            'page': page
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                prs = response.json()
                
                if not prs:
                    break
                
                for pr in prs:
                    pr_number = pr.get('number')
                    
                    # Stop if we've reached or passed the target PR ID
                    if pr_number > pr_id:
                        return prs_list
                    
                    # Only process merged PRs
                    if pr.get('merged_at') is None:
                        continue
                    
                    # Get PR details
                    pr_url = pr.get('html_url', f'https://github.com/{repo}/pull/{pr_number}')
                    author = pr.get('user', {}).get('login', '')
                    
                    # Get merged commit SHA
                    sha = get_pr_merged_sha(repo, pr_number, token)
                    
                    # Check agent status from the passed dataframe 'df'
                    # Note: Ensure 'df' (merged_prs) actually has an 'agent' column or logic to map it
                    author_aidev_series = df[(df['full_name'] == repo) & (df['number'] == pr_number)]['agent']
                    
                    if not author_aidev_series.empty:
                        author_aidev = author_aidev_series.iloc[0]
                    else:
                        # Default to Developer if not found in our CSV map
                        author_aidev = "Developer"

                    pr_info = {
                        "full_name": repo,
                        "language": language,
                        "pr_url": pr_url,
                        "pr_number": pr_number,
                        "sha": sha if sha else "",
                        "author": author if author else "",
                        "author_aidev": author_aidev
                    }
                    
                    prs_list.append(pr_info)
                    print(f"  ✓ Found merged PR #{pr_number} (SHA: {sha[:8] if sha else 'N/A'}, Author: {author})")
                
                # If we got fewer results than per_page, we've reached the end
                if len(prs) < per_page:
                    break
                
                page += 1
                
            elif response.status_code == 404:
                print(f"  ✗ Repository {repo} not found")
                logging.error(f"Repository {repo} not found (404)")
                break
            else:
                print(f"  ✗ Failed to fetch PRs (Status: {response.status_code})")
                logging.error(f"Failed to fetch PRs for {repo} (Status: {response.status_code})")
                break
                
        except Exception as e:
            print(f"  ✗ Error fetching PRs: {e}")
            logging.error(f"Error fetching PRs for {repo}: {e}")
            break
    
    print(f"  Total merged PRs found: {len(prs_list)}")
    return prs_list

def generate_summary_table(output_df):
    """
    Generates a pretty printed summary table with agent/dev percentages.
    """
    summary_list = []
    
    # Group by Project and Language
    for (project, lang), group in output_df.groupby(['full_name', 'language']):
        total = len(group)
        
        # Count Developer commits
        dev_count = len(group[group['author_aidev'] == 'Developer'])
        
        # Count Agent commits
        agent_count = total - dev_count
        
        # Identify which Agents were involved (excluding 'Developer')
        agents_involved = group[group['author_aidev'] != 'Developer']['author_aidev'].unique()
        agent_str = ", ".join(str(a) for a in agents_involved) if len(agents_involved) > 0 else "None"
        
        # Calculate Percentages
        dev_pct = (dev_count / total) * 100 if total > 0 else 0
        agent_pct = (agent_count / total) * 100 if total > 0 else 0
        
        summary_list.append({
            "Project": project,
            "Language": lang,
            "Agent": agent_str,
            "Total Merged Commits": total,
            "Merged Commits (Developer) %": f"{dev_pct:.2f}%",
            "Merged Commits (Agent) %": f"{agent_pct:.2f}%"
        })
        
    return pd.DataFrame(summary_list)

def main():
    repo_df = pd.read_csv(os.path.join(aidev_path, "repository.csv"))
    pr_df = pd.read_csv(os.path.join(aidev_path, "pull_request.csv"))
    merged_prs = get_merged_prs(pr_df, repo_df)
    
    # Read the input CSV
    input_csv = f"{results_01_path}/q3plus_projects_by_language.csv"
    print(f"Reading {input_csv}...")
    df = pd.read_csv(input_csv)
    
    print(f"Processing {len(df)} projects...")
    
    # Prepare output data
    output_data = []
    START_CUT = 0.35
    END_CUT = 0.65
    MIN_TOTAL_PRS = 50

    for idx, row in df.iterrows():
        current_prop = row.get('prop_num_prs', 0)
        current_total = row.get('number_prs_merged_up_to_date', 0)

        cut_condition = (current_prop >= START_CUT) and (current_prop <= END_CUT)
        min_condition = current_total >= MIN_TOTAL_PRS

        # Skip if conditions are met
        if not cut_condition or not min_condition:
            continue

        repo = row["full_name"]
        language = row["language"]
        max_pr_number = row["number"]
        
        print(f"\n[{idx + 1}/{len(df)}] Processing {repo} (all PRs up to #{max_pr_number})...")
        
        # Get all merged PRs up to max_pr_number using the function
        prs_list = get_all_merged_prs_until(merged_prs, repo, max_pr_number, language, token)
        
        # Add all PRs to output data
        for pr_info in prs_list:
            output_data.append(pr_info)
            logging.info(f"[{idx + 1}/{len(df)}] Added PR #{pr_info['pr_number']} for {repo}")
    
    # Create output DataFrame
    output_df = pd.DataFrame(output_data)
    
    # Save the detailed PR data to CSV
    output_path = os.path.join(results_02_path, "projects_with_pr_sha.csv")
    output_df.to_csv(output_path, index=False)
    
    print(f"\n✓ Main CSV saved to: {output_path}")
    print(f"  Total rows: {len(output_df)}")
    print(f"  Rows with SHA: {output_df['sha'].notna().sum()}")
    print("-" * 60)

    # === GENERATE AND SAVE SUMMARY CSV ===
    if not output_df.empty:
        summary_df = generate_summary_table(output_df)
        
        # Define output path for the summary
        summary_csv_path = os.path.join(results_02_path, "projects_summary_stats.csv")
        
        # Save to CSV
        summary_df.to_csv(summary_csv_path, index=False)
        
        print(f"\n✓ Summary CSV saved to: {summary_csv_path}")
        print("  (Contains percentage breakdown of Developer vs Agent commits per project)")
    else:
        print("\nNo data to summarize.")

if __name__ == "__main__":
    main()