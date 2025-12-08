import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
import json
import time
import requests
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

def get_merged_pr_counts_batch(repo_list: list[str], token: str, batch_size: int = 50) -> dict[str, int]:
    """Recupera a contagem total de PRs para uma lista de repos usando um único request por lote."""
    url = "https://api.github.com/graphql"
    headers = {"Authorization": f"Bearer {token}"}
    results = {}

    for i in range(0, len(repo_list), batch_size):
        chunk = repo_list[i : i + batch_size]
        query_parts = []
        
        # Monta a query com aliases (repo_0, repo_1...)
        for idx, full_name in enumerate(chunk):
            try:
                owner, name = full_name.split("/")
                part = f"""
                repo_{idx}: repository(owner: "{owner}", name: "{name}") {{
                    pullRequests(states: MERGED) {{ totalCount }}
                }}
                """
                query_parts.append(part)
            except ValueError:
                continue

        if not query_parts: continue
        
        full_query = "query { " + " ".join(query_parts) + " }"
        
        # Tentativa com retry simples para Rate Limit
        while True:
            try:
                r = requests.post(url, headers=headers, json={"query": full_query}, timeout=30)
                if r.status_code == 200:
                    data = r.json()
                    if "errors" in data: print(f"Errors in batch: {data['errors'][0]['message']}")
                    
                    # Mapeia de volta para o nome do repo
                    for idx, full_name in enumerate(chunk):
                        alias = f"repo_{idx}"
                        repo_data = data.get("data", {}).get(alias)
                        results[full_name] = repo_data["pullRequests"]["totalCount"] if repo_data else -1
                    break
                elif r.status_code in [403, 429]:
                    print("Rate Limit. Sleeping 60s...")
                    time.sleep(60)
                else:
                    print(f"Error {r.status_code}: {r.text}")
                    break
            except Exception as e:
                print(f"Exception: {e}. Sleeping 5s...")
                time.sleep(5)
                
    return results

def get_until_date_counts_batch(repo_date_pairs: list[tuple], token: str, batch_size: int = 40) -> dict[str, int]:
    """
    Recupera contagem de PRs até uma data específica usando Search API via GraphQL em lote.
    Input: [(repo_name, date_string), ...]
    """
    url = "https://api.github.com/graphql"
    headers = {"Authorization": f"Bearer {token}"}
    results = {}

    for i in range(0, len(repo_date_pairs), batch_size):
        chunk = repo_date_pairs[i : i + batch_size]
        query_parts = []
        
        # Monta query de busca dinâmica
        for idx, (repo, date_str) in enumerate(chunk):
            # Alias precisa começar com letra, usamos s_INDEX
            q_str = f"repo:{repo} is:pr is:merged merged:<={date_str}"
            part = f"""
            s_{idx}: search(query: "{q_str}", type: ISSUE, first: 0) {{ issueCount }}
            """
            query_parts.append(part)

        full_query = "query { " + " ".join(query_parts) + " }"
        
        while True:
            try:
                r = requests.post(url, headers=headers, json={"query": full_query}, timeout=45)
                if r.status_code == 200:
                    data = r.json()
                    for idx, (repo, date_str) in enumerate(chunk):
                        key = f"{repo}@{date_str}"
                        val = data.get("data", {}).get(f"s_{idx}", {}).get("issueCount", -1)
                        results[key] = val
                    break
                elif r.status_code in [403, 429]:
                    print("Rate Limit (Search). Sleeping 60s...")
                    time.sleep(60)
                else:
                    print(f"Error {r.status_code}")
                    break
            except Exception as e:
                time.sleep(5)
                
    return results

def enrich_projects_with_github_counts(
    projects_df: pd.DataFrame,
    token: str | None = None,
    cache_path: str = "01_results/github_pr_counts_cache.json",
    sleep_seconds: float = 0.2, # Não é mais usado, mas mantido p/ compatibilidade
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

    # Identifica quais repos não estão no cache
    unique_repos = df["full_name"].unique().tolist()
    missing_repos = [r for r in unique_repos if r not in cache]

    if missing_repos:
        print(f"Fetching total PR counts for {len(missing_repos)} repos in batches...")
        # === AQUI ESTÁ A MUDANÇA MÁGICA ===
        new_data = get_merged_pr_counts_batch(missing_repos, token)
        cache.update(new_data)
        
        # Salva o cache atualizado
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)

    # Cria o dataframe final a partir do cache
    # map retorna NaN se não achar, então fillna(0) ou trate como quiser
    df["total_merged_prs"] = df["full_name"].map(cache)
    
    # Limpeza final
    df["total_merged_prs"] = pd.to_numeric(df["total_merged_prs"], errors="coerce")
    out = df[(df["total_merged_prs"].fillna(0) != 0)].copy()
    
    return out

def enrich_projects_with_github_counts_until_date(
    projects_df: pd.DataFrame,
    date_col: str = "latest_merged_at",
    token: str | None = None,
    cache_path: str = "01_results/github_pr_counts_until_cache.json",
    sleep_seconds: float = 2,
) -> pd.DataFrame:
    if token is None:
        token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("Set the token in GITHUB_TOKEN or pass token=...")

    df = projects_df.dropna(subset=["full_name", "language"]).copy()
    if date_col not in df.columns:
        raise RuntimeError(f"Column '{date_col}' missing.")

    df["full_name"] = df["full_name"].astype(str)
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df["latest_merged_date"] = df[date_col].dt.date.astype(str)

    os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
    cache = {}
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            cache = json.load(f)

    # Prepara lista de (repo, data) que faltam no cache
    pairs_to_fetch = []
    unique_pairs = df[["full_name", "latest_merged_date"]].drop_duplicates().values.tolist()
    
    for repo, date_str in unique_pairs:
        if pd.isna(date_str): continue
        key = f"{repo}@{date_str}"
        if key not in cache:
            pairs_to_fetch.append((repo, date_str))

    if pairs_to_fetch:
        print(f"Fetching time-based PR counts for {len(pairs_to_fetch)} items in batches...")
        # === AQUI ESTÁ A MUDANÇA MÁGICA ===
        new_results = get_until_date_counts_batch(pairs_to_fetch, token)
        cache.update(new_results)

        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)

    # Aplica os dados do cache ao DataFrame
    def get_val(row):
        k = f"{row['full_name']}@{row['latest_merged_date']}"
        return cache.get(k, 0)

    df["number_prs_merged_up_to_date"] = df.apply(get_val, axis=1)

    # Lógica original de cálculo e ordenação
    df["difference_num_prs"] = (
        pd.to_numeric(df["number_prs_merged_up_to_date"], errors="coerce")
        - pd.to_numeric(df["num_prs"], errors="coerce")
    )

    df = df.sort_values("difference_num_prs", ascending=True)
    
    # ... Restante da sua lógica de ordenação e prop ...
    df["prop_num_prs"] = np.where(
        df["number_prs_merged_up_to_date"] > 0,
        df["num_prs"] / df["number_prs_merged_up_to_date"],
        np.nan
    )
    df = df.sort_values("prop_num_prs", ascending=False).reset_index(drop=True)
    
    # Reindex columns (mantive sua lógica original)
    desired_order = [
        "full_name", "language", "difference_num_prs", "prop_num_prs",
        "num_prs", "number_prs_merged_up_to_date", "total_merged_prs",
        "latest_merged_at", "number"
    ]
    df = df.reindex(columns=[c for c in desired_order if c in df.columns])

    return df

def export_q3plus_projects_csv(
    merged_prs_per_project: pd.DataFrame,
    output_path: str = "01_results/q3plus_projects_by_language.csv",
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
