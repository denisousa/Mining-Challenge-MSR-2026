"""
Docstring for prepare_human_prs

Esse script enriquece o dataset Human-PRs com informações language, forks, stars e url dos repositórios
"""

import os
import time
import requests
import pandas as pd
from typing import Dict, Optional
import os
import pandas as pd
import requests
from utils.folders_paths import aidev_path, main_results
from dotenv import load_dotenv
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from utils.languages import LANGUAGES

load_dotenv()
os.makedirs(main_results, exist_ok=True)

# === Load datasets ===
print("Loading datasets...")
human_prs_df = pd.read_csv(os.path.join(aidev_path, "human_pull_request.csv"))

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

def create_github_session(token: str) -> requests.Session:
    """
    Create a resilient GitHub API session with retries and backoff.
    """
    session = requests.Session()

    retries = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "MSR-2026-Collector"
    })

    return session

def get_repo_metadata(
    session: requests.Session,
    repo_api_url: str
) -> Dict[str, Optional[str]]:
    """
    Fetch repository metadata from GitHub API with fault tolerance.
    """
    try:
        response = session.get(
            repo_api_url,
            timeout=(5, 20)  # (connect timeout, read timeout)
        )

        if response.status_code != 200:
            return {
                "language": None,
                "stars": None,
                "forks": None,
                "url": None,
            }

        data = response.json()

        return {
            "language": data.get("language"),
            "stars": data.get("stargazers_count"),
            "forks": data.get("forks_count"),
            "url": data.get("html_url"),
        }

    except requests.exceptions.RequestException as e:
        print(f"[WARN] Network error for {repo_api_url}: {e}")
        return {
            "language": None,
            "stars": None,
            "forks": None,
            "url": None,
        }

def enrich_dataframe_with_repo_info(df: pd.DataFrame) -> pd.DataFrame:
    repo_urls = df["repo_url"].dropna().unique()

    session = create_github_session(GITHUB_TOKEN)

    repo_metadata_map = {}

    for repo_url in repo_urls:
        repo_metadata_map[repo_url] = get_repo_metadata(session, repo_url)

    repo_metadata_df = (
        pd.DataFrame.from_dict(repo_metadata_map, orient="index")
        .reset_index()
        .rename(columns={"index": "repo_url"})
    )

    return df.merge(repo_metadata_df, on="repo_url", how="left")

if __name__ == "__main__":
    human_prs_df = pd.read_csv(os.path.join(aidev_path, "human_pull_request.csv"))
    human_prs_df = human_prs_df[human_prs_df["merged_at"].notna()].copy()
    
    human_prs_df = enrich_dataframe_with_repo_info(human_prs_df)
    human_prs_df = human_prs_df[human_prs_df["language"].isin(LANGUAGES.keys())]
    human_prs_df['full_name'] = human_prs_df['repo_url'].apply(
        lambda x: x.split('repos/')[-1] if pd.notna(x) and 'repos/' in x else None
    )

    human_prs_df['pr_type'] = 'human'

    output_csv = os.path.join(main_results, "new_human_pull_request.csv")
    human_prs_df.to_csv(output_csv, index=False)
