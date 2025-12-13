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

def detect_agent_influence(pr_data):
    """
    Analyzes PR data to detect indications that it was created or influenced by an Agent/Bot.
    Returns the detected Agent name/type or None if it appears to be a human Developer.
    """
    user = pr_data.get('user', {})
    login = user.get('login', '').lower()
    user_type = user.get('type', '')
    labels = [l.get('name', '').lower() for l in pr_data.get('labels', [])]

    # 1. High Confidence: Explicit API Checks
    if user_type == 'Bot':
        return f"Bot (API Type: {user.get('login')})"
    
    if login.endswith('[bot]'):
        return f"Bot (App: {user.get('login')})"

    # 2. Medium Confidence: Known Bot List
    # Common automation bots that might not be "Generative AI" but are definitely agents
    known_bots = ['dependabot', 'renovate', 'snyk-bot', 'imgbot', 'github-actions', 'codecov']
    if any(bot in login for bot in known_bots):
        return f"Bot (Known: {user.get('login')})"

    # 3. Medium Confidence: Labels
    bot_labels = ['bot', 'automated-pr', 'dependencies', 'auto-generated']
    if any(label in labels for label in bot_labels):
        return "Suspected Agent (Label)"

    return None # Likely a Human Developer

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

def check_agent_proportion(df, repo, latest_merged_at, token=None):
    """
    Check if a project should be analyzed based on agent PR proportion.
    Returns (should_analyze: bool, agent_pct: float, total_prs: int)
    Only analyzes if agent PRs are between 35% and 65% of total merged PRs up to latest_merged_at.
    """
    headers = {'Authorization': f'token {token}'} if token else {}
    page = 1
    per_page = 100
    merged_prs_data = []
    
    print(f"  Checking agent proportion for {repo} up to {latest_merged_at}...")
    
    while True:
        url = f'https://api.github.com/repos/{repo}/pulls'
        params = {
            'state': 'closed',
            'sort': 'created',
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
                    # Only process merged PRs
                    merged_at = pr.get('merged_at')
                    if merged_at is None:
                        continue
                    
                    # Stop if we've passed the latest_merged_at date
                    if merged_at > latest_merged_at:
                        break
                    
                    pr_number = pr.get('number')
                    
                    # Check if this PR is from AiDEV dataset
                    author_aidev_series = df[(df['full_name'] == repo) & (df['number'] == pr_number)]['agent']
                    
                    author_aidev = "Developer"  # Default
                    is_aidev_dataset = False
                    
                    if not author_aidev_series.empty:
                        author_aidev = author_aidev_series.iloc[0]
                        is_aidev_dataset = True
                    else:
                        # Apply heuristics to detect agents
                        suspected_agent = detect_agent_influence(pr)
                        if suspected_agent:
                            author_aidev = suspected_agent
                    
                    merged_prs_data.append({
                        'pr_number': pr_number,
                        'merged_at': merged_at,
                        'author_aidev': author_aidev,
                        'is_aidev_dataset': is_aidev_dataset
                    })
                
                # Check if we should stop
                if not prs or len(prs) < per_page:
                    break
                    
                # Also check if the last PR in this page exceeded the date
                if prs and prs[-1].get('merged_at') and prs[-1].get('merged_at') > latest_merged_at:
                    break
                
                page += 1
                
            elif response.status_code == 404:
                print(f"    ✗ Repository {repo} not found")
                logging.error(f"Repository {repo} not found (404)")
                return False, 0.0, 0
            else:
                print(f"    ✗ Failed to fetch PRs (Status: {response.status_code})")
                logging.error(f"Failed to fetch PRs for {repo} (Status: {response.status_code})")
                return False, 0.0, 0
                
        except Exception as e:
            print(f"    ✗ Error fetching PRs: {e}")
            logging.error(f"Error fetching PRs for {repo}: {e}")
            return False, 0.0, 0
    
    if not merged_prs_data:
        print(f"    ✗ No merged PRs found up to {latest_merged_at}")
        return False, 0.0, 0
    
    # Calculate agent proportion
    total_prs = len(merged_prs_data)
    agent_prs = sum(1 for pr in merged_prs_data if not pr['is_aidev_dataset'])
    agent_pct = (agent_prs / total_prs) * 100 if total_prs > 0 else 0
    
    should_analyze = (35 <= agent_pct <= 65) and total_prs >= 100
    
    print(f"    Total merged PRs: {total_prs}, Agent PRs: {agent_prs} ({agent_pct:.2f}%)")
    if should_analyze:
        print(f"    ✓ Agent proportion is within range (35-65%), will analyze this project")
    else:
        print(f"    ✗ Agent proportion is outside range (35-65%), skipping this project")
    
    return should_analyze, agent_pct, total_prs

def get_all_merged_prs_until(df, repo, pr_id, language, token=None):
    """
    Get all merged pull requests for a GitHub repository up to a specific PR ID.
    Includes logic to detect Agents/Bots via AiDEV dataset OR Heuristics.
    """
    headers = {'Authorization': f'token {token}'} if token else {}
    prs_list = []
    page = 1
    per_page = 100
    
    print(f"  Fetching merged PRs for {repo} up to PR #{pr_id}...")
    
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
                    
                    # === AGENT DETECTION LOGIC ===
                    # 1. Check existing AiDEV dataset (Truth)
                    author_aidev_series = df[(df['full_name'] == repo) & (df['number'] == pr_number)]['agent']
                    
                    author_aidev = "Developer" # Default
                    dataset_source = False
                    suspected_agent = False

                    if not author_aidev_series.empty:
                        author_aidev = author_aidev_series.iloc[0]
                        dataset_source = True
                    else:
                        # 2. If not in dataset, apply Heuristics
                        suspected_agent = detect_agent_influence(pr)
                        if suspected_agent:
                            author_aidev = suspected_agent
                        # else: remains "Developer"

                    pr_info = {
                        "full_name": repo,
                        "language": language,
                        "pr_url": pr_url,
                        "pr_number": pr_number,
                        "sha": sha if sha else "",
                        "author": author if author else "",
                        "author_aidev": author_aidev,
                        "is_aidev_dataset": dataset_source,
                        "merged_pr": pr['merge_commit_sha']
                    }
                    
                    prs_list.append(pr_info)
                    
                    # Visual feedback in console
                    status_icon = "🤖" if suspected_agent or dataset_source else "👤"
                    print(f"  ✓ {status_icon} Found merged PR #{pr_number} (SHA: {sha[:8] if sha else 'N/A'}, Type: {author_aidev})")
                
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
    Generates a summary table with agent/dev percentages.
    Only includes: Project, Language, Total Merged Commits, and percentages.
    """
    summary_list = []
    
    # Group by Project and Language
    for (project, lang), group in output_df.groupby(['full_name', 'language']):
        total = len(group)
        
        # Count Developer commits
        dev_count = len(group[group['author_aidev'] == 'Developer'])
        
        # Count Agent commits (Everything that is NOT Developer)
        agent_count = total - dev_count
        
        # Calculate Percentages
        dev_pct = (dev_count / total) * 100 if total > 0 else 0
        agent_pct = (agent_count / total) * 100 if total > 0 else 0
        
        summary_list.append({
            "Project": project,
            "Language": lang,
            "Total Merged Commits": total,
            "Merged Commits (Developer) %": f"{dev_pct:.2f}%",
            "Merged Commits (Agent) %": f"{agent_pct:.2f}%"
        })
        
    return pd.DataFrame(summary_list)

def generate_aidev_dataset_summary(output_df):
    """
    Generates a summary table based on whether PRs are in AiDEV dataset or not.
    Counts: PRs in AiDEV Dataset vs PRs NOT in AiDEV Dataset (detected by heuristics).
    """
    summary_list = []
    
    # Group by Project and Language
    for (project, lang), group in output_df.groupby(['full_name', 'language']):
        total = len(group)
        
        # Count PRs that ARE in AiDEV dataset
        in_aidev_count = len(group[group['is_aidev_dataset'] == True])
        
        # Count PRs that are NOT in AiDEV dataset (detected by heuristics)
        not_in_aidev_count = len(group[group['is_aidev_dataset'] == False])
        
        # Calculate Percentages
        in_aidev_pct = (in_aidev_count / total) * 100 if total > 0 else 0
        not_in_aidev_pct = (not_in_aidev_count / total) * 100 if total > 0 else 0
        
        summary_list.append({
            "Project": project,
            "Language": lang,
            "Total Merged Commits": total,
            "Merged Commits (Developer) %": f"{in_aidev_pct:.2f}%",
            "Merged Commits (Agent) %": f"{not_in_aidev_pct:.2f}%"
        })
        
    return pd.DataFrame(summary_list)

def generate_suspected_agents_report(output_df):
    """
    Generates a report of suspected agents count per project.
    Returns a formatted string for txt output.
    """
    report_lines = []
    report_lines.append("="*80)
    report_lines.append("SUSPECTED AGENTS REPORT")
    report_lines.append("="*80)
    report_lines.append("")
    
    # Group by Project
    for project, group in output_df.groupby('full_name'):
        # Filter only suspected agents (not Developer and not from AiDEV dataset)
        suspected = group[(group['author_aidev'] != 'Developer') & (~group['is_aidev_dataset'])]
        
        if len(suspected) > 0:
            report_lines.append(f"Project: {project}")
            report_lines.append("-" * 80)
            
            # Count occurrences of each suspected agent type
            agent_counts = suspected['author_aidev'].value_counts()
            
            for agent, count in agent_counts.items():
                report_lines.append(f"  {agent}: {count} occurrence(s)")
            
            report_lines.append(f"  Total suspected agents: {len(suspected)}")
            report_lines.append("")
    
    # Add AiDEV dataset agents for comparison
    report_lines.append("="*80)
    report_lines.append("AIDEV DATASET AGENTS (for reference)")
    report_lines.append("="*80)
    report_lines.append("")
    
    for project, group in output_df.groupby('full_name'):
        # Filter only AiDEV dataset agents
        aidev_agents = group[(group['author_aidev'] != 'Developer') & (group['is_aidev_dataset'])]
        
        if len(aidev_agents) > 0:
            report_lines.append(f"Project: {project}")
            report_lines.append("-" * 80)
            
            agent_counts = aidev_agents['author_aidev'].value_counts()
            
            for agent, count in agent_counts.items():
                report_lines.append(f"  {agent}: {count} occurrence(s)")
            
            report_lines.append(f"  Total AiDEV agents: {len(aidev_agents)}")
            report_lines.append("")
    
    return "\n".join(report_lines)

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

    for idx, row in df.iterrows():
        if row["num_prs"] < 35:
            continue

        repo = row["full_name"]
        language = row["language"]
        max_pr_number = row["number"]
        latest_merged_at = row["latest_merged_at"]
        
        print(f"\n[{idx + 1}/{len(df)}] Processing {repo} (all PRs up to #{max_pr_number})...")
        
        # Check if project should be analyzed based on agent proportion (35-65%)
        should_analyze, agent_pct, total_prs = check_agent_proportion(merged_prs, repo, latest_merged_at, token)
        
        if not should_analyze:
            print(f"  ⊗ Skipping {repo} - Agent proportion ({agent_pct:.2f}%) not in range [35%, 65%]")
            logging.info(f"Skipped {repo} - Agent proportion {agent_pct:.2f}% with {total_prs} total PRs")
            continue
        
        # Get all merged PRs up to max_pr_number using the function
        prs_list = get_all_merged_prs_until(merged_prs, repo, max_pr_number, language, token)
        
        # Add all PRs to output data
        for pr_info in prs_list:
            output_data.append(pr_info)
            # Reduced logging spam, uncomment if needed
            # logging.info(f"[{idx + 1}/{len(df)}] Added PR #{pr_info['pr_number']} for {repo}")
    
    # Create output DataFrame
    output_df = pd.DataFrame(output_data)
    
    # Save the detailed PR data to CSV
    output_path = os.path.join(results_02_path, "projects_with_pr_sha.csv")
    output_df.to_csv(output_path, index=False)
    
    print(f"\n✓ Main CSV saved to: {output_path}")
    print(f"  Total rows: {len(output_df)}")
    if not output_df.empty:
        print(f"  Rows with SHA: {output_df['sha'].notna().sum()}")
        print(f"  Rows from AiDEV Dataset: {output_df['is_aidev_dataset'].sum()}")
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
        
        # === GENERATE AND SAVE AIDEV DATASET SUMMARY CSV ===
        aidev_summary_df = generate_aidev_dataset_summary(output_df)
        
        # Define output path for the AiDEV dataset summary
        aidev_summary_csv_path = os.path.join(results_02_path, "projects_aidev_dataset_stats.csv")
        
        # Save to CSV
        aidev_summary_df.to_csv(aidev_summary_csv_path, index=False)
        
        print(f"\n✓ AiDEV Dataset Summary CSV saved to: {aidev_summary_csv_path}")
        print("  (Contains count of PRs in AiDEV Dataset vs PRs NOT in AiDEV Dataset per project)")
        
        # === GENERATE AND SAVE SUSPECTED AGENTS REPORT ===
        suspected_agents_report = generate_suspected_agents_report(output_df)
        
        # Define output path for the report
        report_txt_path = os.path.join(results_02_path, "suspected_agents_report.txt")
        
        # Save to TXT
        with open(report_txt_path, 'w', encoding='utf-8') as f:
            f.write(suspected_agents_report)
        
        print(f"\n✓ Suspected Agents Report saved to: {report_txt_path}")
        print("  (Contains count of suspected agents per project)")
    else:
        print("\nNo data to summarize.")

if __name__ == "__main__":
    main()