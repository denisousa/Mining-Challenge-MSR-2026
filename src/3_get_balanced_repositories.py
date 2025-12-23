import os
import pandas as pd
from utils.folders_paths import main_results
from dotenv import load_dotenv

load_dotenv()
token = os.getenv("GITHUB_TOKEN")

os.makedirs(main_results, exist_ok=True)

# === Load datasets ===
print("Loading datasets...")
agent_prs_df = pd.read_csv(os.path.join(main_results, "new_agent_pull_request.csv"))
human_prs_df = pd.read_csv(os.path.join(main_results, "new_human_pull_request.csv"))

# Load repository metadata to attach programming languages
repo_meta_path = os.path.join(os.path.abspath("AiDev_Dataset"), "repository.csv")
repository_df = pd.read_csv(repo_meta_path)[["full_name", "language"]].drop_duplicates(subset="full_name")

# Concatenate both dataframes
h_g_prs_merged = pd.concat([agent_prs_df, human_prs_df], ignore_index=True)
print(f"Total merged PRs in agent dataset: {len(agent_prs_df)}")
print(f"Total merged PRs in human dataset: {len(human_prs_df)}")
print(f"Total merged PRs combined: {len(h_g_prs_merged)}")

# === Calculate repository statistics ===
print("\nCalculating repository statistics...")

# Count agent PRs per repository
agent_prs_count = h_g_prs_merged[h_g_prs_merged['pr_type'] == 'agent'].groupby('full_name').size().reset_index(name='agent_prs')

# Count human PRs per repository  
human_prs_count = h_g_prs_merged[h_g_prs_merged['pr_type'] == 'human'].groupby('full_name').size().reset_index(name='human_prs')

# Count total PRs per repository
total_prs_count = h_g_prs_merged.groupby('full_name').size().reset_index(name='total_prs')

# Merge all statistics
repo_stats = total_prs_count.copy()
repo_stats = pd.merge(repo_stats, agent_prs_count, on='full_name', how='left')
repo_stats = pd.merge(repo_stats, human_prs_count, on='full_name', how='left')
repo_stats = pd.merge(repo_stats, repository_df, on='full_name', how='left')

# Fill NaN values with 0
repo_stats['agent_prs'] = repo_stats['agent_prs'].fillna(0).astype(int)
repo_stats['human_prs'] = repo_stats['human_prs'].fillna(0).astype(int)

# Calculate percentages
repo_stats['agent_percentage'] = (repo_stats['agent_prs'] / repo_stats['total_prs'] * 100).round(2)
repo_stats['human_percentage'] = (repo_stats['human_prs'] / repo_stats['total_prs'] * 100).round(2)

print(f"Total repositories with merged PRs: {len(repo_stats)}")

# === Apply filters ===
# Filter 1: At least 60 PRs
filtered = repo_stats[repo_stats['total_prs'] >= 60].copy()
print(f"Repositories with at least 60 PRs: {len(filtered)}")

# Filter 2: Agent percentage between 35% and 65%
filtered = filtered[
    (filtered['agent_percentage'] >= 35) & 
    (filtered['agent_percentage'] <= 65)
].copy()

print(f"Repositories with agent PRs between 35% and 65%: {len(filtered)}")

# Sort by total PRs descending and reorder columns to include language
filtered = filtered.sort_values('total_prs', ascending=False)
column_order = [
    'full_name', 'language', 'total_prs', 'agent_prs',
    'human_prs', 'agent_percentage', 'human_percentage'
]
filtered = filtered.reindex(columns=column_order)
output_csv = os.path.join(main_results, "balanced_repositories.csv")
filtered.to_csv(output_csv, index=False)

# === Filter h_g_prs_merged to keep only repositories in filtered ===
print("\n" + "="*80)
print("FILTERING PRs TO KEEP ONLY BALANCED REPOSITORIES")
print("="*80)

filtered_repo_names = filtered['full_name'].tolist()

print(f"\nBefore filtering by repository list: {len(h_g_prs_merged)} PRs")
h_g_prs_merged_filtered = h_g_prs_merged[h_g_prs_merged['full_name'].isin(filtered_repo_names)].copy()
print(f"After filtering by repository list: {len(h_g_prs_merged_filtered)} PRs")

# Save filtered PRs to CSV
filtered_prs_output = os.path.join(main_results, "human_agent_pull_request.csv")
h_g_prs_merged_filtered.to_csv(filtered_prs_output, index=False)
print(f"\n✓ Filtered PRs saved to: {filtered_prs_output}")