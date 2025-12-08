import os
import pandas as pd
import xml.etree.ElementTree as ET
from collections import defaultdict
from utils.folders_paths import results_03_path, results_04_path

# Ensure output directory exists
os.makedirs(results_04_path, exist_ok=True)

# --- Data Structures ---

# 1. Creation Stats
stats_creation = defaultdict(int)

# 2. Update Stats (Global)
stats_updates_global = defaultdict(lambda: {
    "change": defaultdict(int),
    "evolution": defaultdict(int)
})

# 3. Update Stats (By Agent)
stats_updates_agent = defaultdict(lambda: {
    "change": defaultdict(int),
    "evolution": defaultdict(int)
})

# 4. Update Stats (By Language & Group)
stats_updates_language = defaultdict(lambda: defaultdict(lambda: {
    "change": defaultdict(int),
    "evolution": defaultdict(int)
}))

print("Starting XML file processing...")

# --- Helper to identify 'None' ---
def is_start_of_lineage(val):
    if val is None: return True
    s_val = str(val).strip().lower()
    return s_val == "none" or s_val == ""

# --- Processing XML Files ---
for filename in os.listdir(results_03_path):
    if not filename.endswith(".xml"):
        continue
    
    parts = filename.split('_')
    
    # 1. Extract Language (First part of filename)
    try:
        language_name = parts[0].capitalize() # Ex: 'java' -> 'Java'
    except IndexError:
        language_name = "Unknown"

    # 2. Extract Agent Name
    try:
        specific_agent_name = parts[1]
    except IndexError:
        specific_agent_name = "UnknownAgent"

    file_path = os.path.join(results_03_path, filename)
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
    except ET.ParseError:
        print(f"Error parsing XML: {filename}")
        continue

    for version in root.findall(".//version"):
        raw_author = version.get("author")
        evolution = version.get("evolution") 
        change = version.get("change")       
        
        # --- Define Groups ---
        if raw_author == "Developer":
            author_group = "Developer"
            agent_key = "Developer"
        else:
            author_group = "Agent"
            agent_key = specific_agent_name

        # --- LOGIC SPLIT: Creation vs Update ---
        if is_start_of_lineage(change):
            # Creation (Table 1)
            stats_creation[author_group] += 1
        else:
            # Updates (Tables 2, 3, 4, 5, 6)
            
            # A. Global Stats
            stats_updates_global[author_group]["change"][change] += 1
            if not is_start_of_lineage(evolution):
                stats_updates_global[author_group]["evolution"][evolution] += 1
            
            # B. Specific Agent Stats
            stats_updates_agent[agent_key]["change"][change] += 1
            if not is_start_of_lineage(evolution):
                stats_updates_agent[agent_key]["evolution"][evolution] += 1

            # C. Language Stats
            stats_updates_language[language_name][author_group]["change"][change] += 1
            if not is_start_of_lineage(evolution):
                stats_updates_language[language_name][author_group]["evolution"][evolution] += 1

# ==========================================
# HELPER 1: Standard Percentages (Include Same)
# WITH SINGLE TOTAL COLUMN
# ==========================================
def get_pct_row(label_dict, type_dict):
    total_change = sum(type_dict["change"].values())
    total_evolution = sum(type_dict["evolution"].values())

    def pct(val, tot):
        return f"{(val/tot)*100:.2f}%" if tot > 0 else "0.00%"

    row = label_dict.copy()
    row.update({
        # Change Patterns
        "Consistent": pct(type_dict['change']['Consistent'], total_change),
        "Inconsistent": pct(type_dict['change']['Inconsistent'], total_change),
        "Same (Change)": pct(type_dict['change']['Same'], total_change),
        
        # Evolution Patterns
        "Add": pct(type_dict['evolution']['Add'], total_evolution),
        "Subtract": pct(type_dict['evolution']['Subtract'], total_evolution),
        "Same (Evolution)": pct(type_dict['evolution']['Same'], total_evolution),
        
        # SINGLE TOTAL (Based on Change count, which implies Total Updates)
        "Total": total_change
    })
    return row

columns_pct = [
    "Consistent", "Inconsistent", "Same (Change)", 
    "Add", "Subtract", "Same (Evolution)", 
    "Total"
]

# ==========================================
# HELPER 2: Percentages EXCLUDING SAME (Active)
# Left with specific totals for clarity on what is "Active"
# ==========================================
def get_pct_row_no_same(label_dict, type_dict):
    count_consistent = type_dict["change"]['Consistent']
    count_inconsistent = type_dict["change"]['Inconsistent']
    total_change_no_same = count_consistent + count_inconsistent

    count_add = type_dict["evolution"]['Add']
    count_subtract = type_dict["evolution"]['Subtract']
    total_evolution_no_same = count_add + count_subtract

    def pct(val, tot):
        return f"{(val/tot)*100:.2f}%" if tot > 0 else "0.00%"

    row = label_dict.copy()
    row.update({
        "Consistent": pct(count_consistent, total_change_no_same),
        "Inconsistent": pct(count_inconsistent, total_change_no_same),
        "Add": pct(count_add, total_evolution_no_same),
        "Subtract": pct(count_subtract, total_evolution_no_same),
        "Total Change Count (Active)": total_change_no_same,
        "Total Evolution Count (Active)": total_evolution_no_same
    })
    return row

columns_pct_no_same = [
    "Consistent", "Inconsistent", 
    "Add", "Subtract", 
    "Total Change Count (Active)", "Total Evolution Count (Active)"
]

# ==========================================
# OUTPUT GENERATION
# ==========================================

# --- 1. CLONES CREATED ---
rows_creation = []
for group, count in stats_creation.items():
    rows_creation.append({"Group": group, "New Clones Created": count})
df_creation = pd.DataFrame(rows_creation)
df_creation.to_csv(os.path.join(results_04_path, "1_clones_created_dev_vs_agent.csv"), index=False)
print("Table 1 Saved.")

# --- 2. GLOBAL PATTERNS (Standard - Single Total) ---
rows_global = []
for group, types in stats_updates_global.items():
    rows_global.append(get_pct_row({"Group": group}, types))
df_global = pd.DataFrame(rows_global, columns=["Group"] + columns_pct)
df_global.to_csv(os.path.join(results_04_path, "2_global_patterns_dev_vs_agent.csv"), index=False)
print("Table 2 Saved (Single Total).")

# --- 3. AGENT PATTERNS (Standard - Single Total) ---
rows_agent = []
for agent_name, types in stats_updates_agent.items():
    rows_agent.append(get_pct_row({"Author PR": agent_name}, types))
df_agent = pd.DataFrame(rows_agent, columns=["Author PR"] + columns_pct)
df_agent["is_dev"] = df_agent["Author PR"] == "Developer"
df_agent = df_agent.sort_values(by=["is_dev", "Author PR"], ascending=[False, True]).drop(columns=["is_dev"])
df_agent.to_csv(os.path.join(results_04_path, "3_agent_patterns_breakdown.csv"), index=False)
print("Table 3 Saved.")

# --- 4. GLOBAL PATTERNS (No Same) ---
rows_global_ns = []
for group, types in stats_updates_global.items():
    rows_global_ns.append(get_pct_row_no_same({"Group": group}, types))
df_global_ns = pd.DataFrame(rows_global_ns, columns=["Group"] + columns_pct_no_same)
df_global_ns.to_csv(os.path.join(results_04_path, "4_global_patterns_dev_vs_agent_NO_SAME.csv"), index=False)
print("Table 4 Saved.")

# --- 5. AGENT PATTERNS (No Same) ---
rows_agent_ns = []
for agent_name, types in stats_updates_agent.items():
    rows_agent_ns.append(get_pct_row_no_same({"Author PR": agent_name}, types))
df_agent_ns = pd.DataFrame(rows_agent_ns, columns=["Author PR"] + columns_pct_no_same)
df_agent_ns["is_dev"] = df_agent_ns["Author PR"] == "Developer"
df_agent_ns = df_agent_ns.sort_values(by=["is_dev", "Author PR"], ascending=[False, True]).drop(columns=["is_dev"])
df_agent_ns.to_csv(os.path.join(results_04_path, "5_agent_patterns_breakdown_NO_SAME.csv"), index=False)
print("Table 5 Saved.")

# --- 6. LANGUAGE PATTERNS (Standard - Single Total) ---
rows_lang = []
for language, groups in stats_updates_language.items():
    for group, types in groups.items():
        rows_lang.append(get_pct_row({"Language": language, "Group": group}, types))

df_lang = pd.DataFrame(rows_lang, columns=["Language", "Group"] + columns_pct)
df_lang = df_lang.sort_values(by=["Language", "Group"])

output_csv_lang = os.path.join(results_04_path, "6_language_patterns_dev_vs_agent.csv")
df_lang.to_csv(output_csv_lang, index=False)

print(f"Table 6 (Language Breakdown) saved (Single Total).")
print(df_lang.to_markdown(index=False))