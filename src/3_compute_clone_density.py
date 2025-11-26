import os
import logging
import requests
import subprocess
import pandas as pd
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
from utils.compute_time import timed
from utils.languages import LANGUAGES
from utils.nicad_operations import run_nicad, NiCadTimeout, _nicad_timeout_handler
from utils.folders_paths import repos_path, nicad_results_path
import signal

# Set up logging
log_file = 'rq1/errors.log'
if os.path.exists(log_file):
    os.remove(log_file)  # Delete the existing log file when script runs again

logging.basicConfig(filename=log_file, level=logging.ERROR)

load_dotenv()  # Load environment variables from .env file
token = os.getenv("GITHUB_TOKEN")

# Set up folders
os.makedirs(repos_path, exist_ok=True)
os.makedirs(nicad_results_path, exist_ok=True)

# Count lines of code
def count_lines_of_code(directory, extension):
    total_lines = 0
    for root, _, files in os.walk(directory):
        for file_name in files:
            if file_name.endswith(extension):
                file_path = os.path.join(root, file_name)
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                        lines = file.readlines()
                        total_lines += len(lines)
                except Exception as e:
                    logging.error(f"Error reading {file_path}: {e}")
    return total_lines

# Calculate cloned lines of code
def calculate_lines_of_code(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    total_lines = 0
    for cls in root.findall('class'):
        for source in cls.findall('source'):
            startline = int(source.get('startline'))
            endline = int(source.get('endline'))
            total_lines += (endline - startline)
    return total_lines

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
    if token == None:
        logging.error(f" Don't finde the GitHub Token")
        return 

    # Initialize the DataFrame for storing results
    columns = ['full_name', 'number', 'language', 'clone_density', 'merge_commit']
    results_df = pd.DataFrame(columns=columns)

    # Read the CSV containing PR information
    csv_path = "rq1/last_merged_pr_per_project_rq1.csv"
    df = pd.read_csv(csv_path)
    projects = df[["full_name", "number", "language"]].drop_duplicates()

    os.makedirs(repos_path, exist_ok=True)

    for i, row in projects.iterrows():
        print(f"Execution {i} | Total {projects.shape[0]}")
        full_name = row["full_name"]
        pr_number = row["number"]
        pr_language = row["language"]

        repo_url = f"https://github.com/{full_name}.git"
        repo_local_path = os.path.join(repos_path, full_name.replace("/", "_"))

        print(f"\n=== Processing {full_name} (PR #{pr_number}) ===")

        # Clone repository if not already cloned
        if not os.path.exists(repo_local_path):
            try:
                print("  Cloning repository ...")
                subprocess.run(["git", "clone", repo_url, repo_local_path], check=True)
                print("  ✔ Repo cloned")
            except subprocess.CalledProcessError as e:
                logging.error(f"({i}) Error cloning {full_name}: {e}")
                continue
        else:
            print("  ✔ Repo already exists, using local copy")

        # Get merge commit using GitHub API
        print(f"  Resolving merge commit for PR #{pr_number} ...")
        try:
            merged_commit, _ = get_pr_commits(full_name, pr_number, token)
            if not merged_commit:
                raise ValueError(f"Could not resolve merge commit for PR #{pr_number}")
            print(f"  ✔ Merge commit found: {merged_commit}")
        except Exception as e:
            logging.error(f"({i}) Error resolving merge commit for {full_name} (PR #{pr_number}): {e}")
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

        # Run NiCad to detect clones
        print(f"  Running NiCad on {repo_local_path} ...")
        signal.signal(signal.SIGALRM, _nicad_timeout_handler)
        signal.alarm(60)  # 60 seconds

        try:
            run_nicad(repo_local_path, LANGUAGES[pr_language], nicad_results_path)
            signal.alarm(0)  # disable alarm if everything went fine
            print("  ✔ NiCad completed")

        except NiCadTimeout:
            logging.error(f"({i}) NiCad exceeded 1 minute for {full_name} (PR #{pr_number})")

        except Exception as e:
            logging.error(f"({i}) Error running NiCad for {full_name} (PR #{pr_number}): {e}")

        # Get clone data from NiCad result
        try:
            repo_name = full_name.replace("/","_")
            nicad_result_xml_path = f"{nicad_results_path}/{repo_name}_functions-clones-0.30-classes.xml"
            system_lines = count_lines_of_code(os.path.abspath(repo_local_path), LANGUAGES[pr_language])
            clones_lines = calculate_lines_of_code(nicad_result_xml_path)
            clone_density_by_repo = round((clones_lines * 100) / system_lines, 2)

            # Append the result to the DataFrame using pd.concat
            new_row = pd.DataFrame([{
                'full_name': full_name,
                'number': pr_number,
                'language': pr_language,
                'clone_density': clone_density_by_repo,
            }])

            results_df = pd.concat([results_df, new_row], ignore_index=True)
            results_df.to_csv('rq1/clone_density_results_rq1.csv', index=False)
        except:
            logging.error(f"({i}) Error computing Clone Density for {full_name} (PR #{pr_number}): {e}")

            
    print("\n=== All PRs processed ===")

# Execute main function
if __name__ == "__main__":
    main()
