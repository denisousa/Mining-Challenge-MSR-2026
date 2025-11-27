import subprocess
import logging

def git_fecth(i, commit, repo_path, repo_name):
    # Fetch the base commit
    print(f"  Fetch out commit {commit} ...")
    try:
        subprocess.run(["git", "fetch", "origin", commit], cwd=repo_path, check=True)
        print(f"  ✔ Checked out to commit {commit}")
    except subprocess.CalledProcessError as e:
        logging.error(f"({i}) Error checking out commit {commit} for {repo_name}: {e}")

def git_checkout(i, commit, repo_path, repo_name):
    # Checkout the base commit
    print(f"  Checking out commit {commit} ...")
    try:
        subprocess.run(["git", "checkout", commit], cwd=repo_path, check=True)
        print(f"  ✔ Checked out to commit {commit}")
    except subprocess.CalledProcessError as e:
        logging.error(f"({i}) Error checking out commit {commit} for {repo_name}: {e}")
