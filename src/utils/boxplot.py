import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
import json
import time
import requests

warnings.filterwarnings("ignore")


def create_individual_boxplots(merged_prs_per_project, output_dir="rq1/individual_boxplots/"):
    os.makedirs(output_dir, exist_ok=True)

    df = merged_prs_per_project.copy()
    df = df.dropna(subset=["language", "num_prs"])
    df["num_prs"] = pd.to_numeric(df["num_prs"], errors="coerce")
    df = df.dropna(subset=["num_prs"])

    languages = sorted(df["language"].unique())
    language_stats = {}

    colors = ["#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4", "#FFEAA7", "#DDA0DD", "#98D8C8", "#F7DC6F"]

    print("🎨 Generating individual boxplots for each language...")
    print("=" * 60)

    for i, language in enumerate(languages):
        lang_data = df.loc[df["language"] == language, "num_prs"].astype(float)
        if lang_data.empty:
            continue

        q1 = lang_data.quantile(0.25)
        q2 = lang_data.quantile(0.50)
        q3 = lang_data.quantile(0.75)

        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        outliers = lang_data[(lang_data < lower_bound) | (lang_data > upper_bound)]

        prs_from_q3 = lang_data[lang_data >= q3].sum()
        prs_from_outliers = outliers.sum()

        projects_from_q3 = int((lang_data >= q3).sum())
        projects_outliers = int(outliers.shape[0])

        language_stats[language] = {
            "total_projects": int(lang_data.shape[0]),
            "total_prs": float(lang_data.sum()),
            "q1": float(q1),
            "median": float(q2),
            "q3": float(q3),
            "iqr": float(iqr),
            "outliers_count": int(outliers.shape[0]),
            "outliers_values": outliers.tolist(),
            "prs_from_q3_and_above": int(prs_from_q3),
            "projects_from_q3_and_above": projects_from_q3,
            "prs_from_outliers": int(prs_from_outliers),
            "projects_outliers": projects_outliers,
            "min_value": float(lang_data.min()),
            "max_value": float(lang_data.max()),
            "mean": float(lang_data.mean()),
            "std": float(lang_data.std(ddof=1)),
        }

        plt.figure(figsize=(10, 8))

        box_plot = plt.boxplot(
            lang_data.values,
            patch_artist=True,
            showmeans=True,
            meanline=True,
            widths=0.6,
        )

        box = box_plot["boxes"][0]
        box.set_facecolor(colors[i % len(colors)])
        box.set_alpha(0.7)

        median_line = box_plot["medians"][0]
        median_line.set_color("red")
        median_line.set_linewidth(2)

        if box_plot.get("means"):
            box_plot["means"][0].set_color("black")
            box_plot["means"][0].set_linewidth(2)

        plt.title(f"Distribution of Merged Pull Requests\n{language}", fontsize=16, fontweight="bold", pad=20)
        plt.ylabel("Number of Merged Pull Requests", fontsize=12, fontweight="bold")
        plt.xlabel(f"{language} Projects", fontsize=12, fontweight="bold")

        plt.grid(True, alpha=0.3, axis="y")

        plt.text(
            1.15,
            q1,
            f"Q1: {q1:.1f}",
            fontsize=10,
            fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow", alpha=0.7),
        )
        plt.text(
            1.15,
            q2,
            f"Median: {q2:.1f}",
            fontsize=10,
            fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="orange", alpha=0.7),
        )
        plt.text(
            1.15,
            q3,
            f"Q3: {q3:.1f}",
            fontsize=10,
            fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgreen", alpha=0.7),
        )

        stats_text = (
            "Statistics Summary:\n"
            f"Projects: {len(lang_data)}\n"
            f"Total PRs: {lang_data.sum():,.0f}\n"
            f"Mean: {lang_data.mean():.1f}\n"
            f"Std Dev: {lang_data.std(ddof=1):.1f}\n"
            f"Outliers: {len(outliers)}\n"
            f"PRs from Q3+: {int(prs_from_q3):,}\n"
            f"PRs from Outliers: {int(prs_from_outliers):,}"
        )

        plt.text(
            0.02,
            0.98,
            stats_text,
            transform=plt.gca().transAxes,
            fontsize=9,
            va="top",
            bbox=dict(boxstyle="round,pad=0.5", facecolor="lightblue", alpha=0.8),
        )

        if len(outliers) > 0:
            outlier_positions = np.ones(len(outliers))
            plt.scatter(outlier_positions, outliers, color="red", s=50, alpha=0.7, label=f"Outliers ({len(outliers)})")
            plt.legend()

        plt.tight_layout()

        safe_lang = str(language).lower().replace("+", "plus").replace("#", "sharp").replace("/", "_")
        filename = os.path.join(output_dir, f"{safe_lang}_boxplot.png")
        plt.savefig(filename, dpi=300, bbox_inches="tight")

        print(f"✅ {language}: Boxplot saved as {filename}")

    return language_stats


def generate_summary_report(language_stats):
    print("\n" + "=" * 80)
    print("📊 COMPREHENSIVE SUMMARY REPORT")
    print("=" * 80)

    summary_data = []
    total_prs_q3 = 0
    total_prs_outliers = 0

    for lang, stats in language_stats.items():
        summary_data.append(
            {
                "Language": lang,
                "Total_Projects": stats["total_projects"],
                "Total_PRs": stats["total_prs"],
                "Q3_Value": stats["q3"],
                "Projects_Q3+": stats["projects_from_q3_and_above"],
                "PRs_from_Q3+": stats["prs_from_q3_and_above"],
                "Outliers_Count": stats["outliers_count"],
                "PRs_from_Outliers": stats["prs_from_outliers"],
                "Mean_PRs": stats["mean"],
                "Max_PRs": stats["max_value"],
            }
        )
        total_prs_q3 += stats["prs_from_q3_and_above"]
        total_prs_outliers += stats["prs_from_outliers"]

    summary_df = pd.DataFrame(summary_data)

    print("\n🔍 DETAILED BREAKDOWN BY LANGUAGE:")
    print("-" * 80)
    if not summary_df.empty:
        print(
            summary_df.to_string(
                index=False,
                formatters={
                    "Total_PRs": "{:,.0f}".format,
                    "PRs_from_Q3+": "{:,}".format,
                    "PRs_from_Outliers": "{:,}".format,
                    "Q3_Value": "{:.1f}".format,
                    "Mean_PRs": "{:.1f}".format,
                    "Max_PRs": "{:,.0f}".format,
                },
            )
        )

    print("\n🎯 FINAL ANSWER TO YOUR QUESTIONS:")
    print("=" * 50)
    print(f"📈 Total PRs from Third Quartile (Q3) and above: {total_prs_q3:,}")
    print(f"🔺 Total PRs from Outliers only: {total_prs_outliers:,}")

    if not summary_df.empty:
        print("\n💡 INSIGHTS:")
        print("-" * 30)

        max_q3_lang = summary_df.loc[summary_df["PRs_from_Q3+"].idxmax()]
        print(f"• Language with most PRs from Q3+: {max_q3_lang['Language']} ({int(max_q3_lang['PRs_from_Q3+']):,} PRs)")

        max_outliers_lang = summary_df.loc[summary_df["PRs_from_Outliers"].idxmax()]
        print(f"• Language with most outlier PRs: {max_outliers_lang['Language']} ({int(max_outliers_lang['PRs_from_Outliers']):,} PRs)")

        total_all_prs = float(summary_df["Total_PRs"].sum())
        pct_q3 = (total_prs_q3 / total_all_prs) * 100 if total_all_prs else 0.0
        pct_outliers = (total_prs_outliers / total_all_prs) * 100 if total_all_prs else 0.0
        print(f"• Q3+ PRs represent {pct_q3:.1f}% of all PRs")
        print(f"• Outlier PRs represent {pct_outliers:.1f}% of all PRs")

    os.makedirs("rq1", exist_ok=True)
    summary_df.to_csv("rq1/pr_statistics_summary.csv", index=False)
    print("\n📁 Summary report saved to: rq1/pr_statistics_summary.csv")

    return summary_df, total_prs_q3, total_prs_outliers


def main_individual_analysis(merged_prs_per_project):
    print("🚀 Starting Individual Boxplot Analysis...")
    print("=" * 60)

    language_stats = create_individual_boxplots(merged_prs_per_project)
    summary_df, total_prs_q3, total_prs_outliers = generate_summary_report(language_stats)

    return language_stats, summary_df, total_prs_q3, total_prs_outliers


def github_merged_pr_count(repo_full_name: str, token: str, timeout: int = 30) -> int:
    q = f"repo:{repo_full_name} is:pr is:merged"
    url = "https://api.github.com/graphql"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    payload = {
        "query": """
        query($q: String!) {
          search(query: $q, type: ISSUE, first: 1) { issueCount }
        }
        """,
        "variables": {"q": q},
    }
    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if r.status_code == 401:
        raise RuntimeError("Invalid GitHub token/permissions (401).")
    r.raise_for_status()
    data = r.json()
    if "errors" in data and data["errors"]:
        raise RuntimeError(f"GitHub GraphQL error: {data['errors']}")
    return int(data["data"]["search"]["issueCount"])


def enrich_projects_with_github_counts(
    projects_df: pd.DataFrame,
    token: str | None = None,
    cache_path: str = "rq1/github_pr_counts_cache.json",
    sleep_seconds: float = 0.2,
) -> pd.DataFrame:
    if token is None:
        token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("Set the token in GITHUB_TOKEN or pass token=...")

    df = projects_df.dropna(subset=["full_name", "language"]).copy()
    df["full_name"] = df["full_name"].astype(str)

    os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
    cache = {}
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            cache = json.load(f)

    pairs = []
    for repo in df["full_name"].unique():
        if repo not in cache:
            cache[repo] = github_merged_pr_count(repo, token)
            time.sleep(sleep_seconds)
        pairs.append((repo, cache[repo]))

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    counts_df = pd.DataFrame(pairs, columns=["full_name", "github_num_merged_prs"])
    return df.merge(counts_df, on="full_name", how="left")


def github_merged_pr_count_until(repo_full_name: str, token: str, until_dt, timeout: int = 30) -> int:
    # until_dt can be datetime/string; we use only the date part
    dt = pd.to_datetime(until_dt, errors="coerce")
    if pd.isna(dt):
        return 0
    date_str = dt.date().isoformat()

    q = f"repo:{repo_full_name} is:pr is:merged merged:<={date_str}"
    url = "https://api.github.com/graphql"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    payload = {
        "query": """
        query($q: String!) {
          search(query: $q, type: ISSUE, first: 1) { issueCount }
        }
        """,
        "variables": {"q": q},
    }

    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if r.status_code == 401:
        raise RuntimeError("Invalid GitHub token/permissions (401).")
    r.raise_for_status()
    data = r.json()
    if "errors" in data and data["errors"]:
        raise RuntimeError(f"GitHub GraphQL error: {data['errors']}")
    return int(data["data"]["search"]["issueCount"])


def enrich_projects_with_github_counts_until_date(
    projects_df: pd.DataFrame,
    date_col: str = "latest_merged_at",
    token: str | None = None,
    cache_path: str = "rq1/github_pr_counts_until_cache.json",
    sleep_seconds: float = 0.2,
) -> pd.DataFrame:
    if token is None:
        token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("Set the token in GITHUB_TOKEN or pass token=...")

    df = projects_df.dropna(subset=["full_name", "language"]).copy()
    if date_col not in df.columns:
        raise RuntimeError(f"Column '{date_col}' does not exist in the dataframe (it must include latest_merged_at).")

    df["full_name"] = df["full_name"].astype(str)
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
    cache = {}
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            cache = json.load(f)

    unique_pairs = (
        df.dropna(subset=[date_col])[["full_name", date_col]]
        .drop_duplicates()
        .values
        .tolist()
    )

    rows = []
    for repo, dt in unique_pairs:
        date_key = pd.to_datetime(dt).date().isoformat()
        key = f"{repo}@{date_key}"
        if key not in cache:
            cache[key] = github_merged_pr_count_until(repo, token, dt)
            time.sleep(sleep_seconds)
        rows.append((repo, date_key, cache[key]))

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    counts_df = pd.DataFrame(rows, columns=["full_name", "latest_merged_date", "github_num_merged_prs_until_latest"])
    df["latest_merged_date"] = df[date_col].dt.date.astype(str)

    df = df.merge(counts_df, on=["full_name", "latest_merged_date"], how="left").drop(columns=["latest_merged_date"])
    return df


def export_outlier_projects_csv(
    merged_prs_per_project: pd.DataFrame,
    output_path: str = "rq1/outlier_projects_by_language.csv",
    token: str | None = None,
) -> pd.DataFrame:
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    df = merged_prs_per_project.dropna(subset=["full_name", "language", "num_prs"]).copy()
    df["num_prs"] = pd.to_numeric(df["num_prs"], errors="coerce")
    df = df.dropna(subset=["num_prs"])

    q1 = df.groupby("language")["num_prs"].quantile(0.25)
    q3 = df.groupby("language")["num_prs"].quantile(0.75)
    iqr = q3 - q1

    bounds = pd.DataFrame({
        "language": q1.index,
        "lower_bound": (q1 - 1.5 * iqr).values,
        "upper_bound": (q3 + 1.5 * iqr).values,
    })

    outliers_df = (
        df.merge(bounds, on="language", how="left")
        .loc[lambda x: (x["num_prs"] < x["lower_bound"]) | (x["num_prs"] > x["upper_bound"])]
        .drop(columns=["lower_bound", "upper_bound"])
        .sort_values(["language", "num_prs"], ascending=[True, False])
    )

    outliers_df = enrich_projects_with_github_counts(outliers_df, token=token)
    outliers_df.to_csv(output_path, index=False)
    print(f"\nCSV saved as: {output_path}")
    return outliers_df


def export_q3plus_projects_csv(
    merged_prs_per_project: pd.DataFrame,
    output_path: str = "rq1/q3plus_projects_by_language.csv",
    token: str | None = None,
) -> pd.DataFrame:
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    df = merged_prs_per_project.dropna(subset=["full_name", "language", "num_prs"]).copy()
    df["num_prs"] = pd.to_numeric(df["num_prs"], errors="coerce")
    df = df.dropna(subset=["num_prs"])

    q3 = df.groupby("language")["num_prs"].quantile(0.75).rename("q3").reset_index()

    q3plus_df = (
        df.merge(q3, on="language", how="left")
        .loc[lambda x: x["num_prs"] >= x["q3"]]
        .drop(columns=["q3"])
        .sort_values(["language", "num_prs"], ascending=[True, False])
    )

    q3plus_df = enrich_projects_with_github_counts(q3plus_df, token=token)
    q3plus_df.to_csv(output_path, index=False)
    print(f"\nCSV saved as: {output_path}")
    return q3plus_df
