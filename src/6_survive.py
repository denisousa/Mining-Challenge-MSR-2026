import os
import glob
import pandas as pd
import xml.etree.ElementTree as ET

# --- Configuration ---
INPUT_FOLDER = "03_results"
OUTPUT_FOLDER = "06_results"

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def analyze_clone_evolution(file_path):
    """
    Parses a single XML file to determine lineage survival and author type.
    """
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
    except ET.ParseError:
        print(f"Error reading XML: {file_path}")
        return None

    lineage_status = []
    
    # 1. Find the highest 'nr' in the entire file (Final Project Version)
    all_versions_nr = []
    for version in root.findall('.//version'):
        nr = version.get('nr')
        if nr:
            all_versions_nr.append(int(nr))
            
    if not all_versions_nr:
        return None # Empty file or no versions found
        
    max_project_nr = max(all_versions_nr)

    # 2. Analyze each lineage
    for i, lineage in enumerate(root.findall('lineage')):
        survived = False
        author_category = "Unknown"
        found_birth = False

        # --- A. Determine Author (Birth of Lineage) ---
        # Logic: Find version with evolution="None" and change="None"
        for version in lineage.findall('version'):
            evolution = version.get('evolution')
            change = version.get('change')
            
            if evolution == "None" and change == "None":
                raw_author = version.get('author')
                # Classification Logic
                if raw_author == "Developer":
                    author_category = "Developer"
                else:
                    author_category = "Agent" # Groups all agents together
                found_birth = True
                break
        
        # If for some reason no birth version is found, skip or mark unknown
        if not found_birth:
            continue 

        # --- B. Determine Survival ---
        # Check if ANY version in this lineage matches the max_project_nr
        for version in lineage.findall('version'):
            nr = int(version.get('nr'))
            if nr == max_project_nr:
                survived = True
                break
        
        lineage_status.append({
            'file': os.path.basename(file_path),
            'lineage_index': i + 1,
            'author_type': author_category,
            'survived': survived,
            'max_project_nr': max_project_nr
        })

    return lineage_status

def print_stats(title, df_subset):
    """Helper function to print stats for a specific group"""
    total = len(df_subset)
    if total == 0:
        print(f"\n--- {title} (No Data) ---")
        return {
            'total': 0, 'survived': 0, 'died': 0, 
            'survival_rate': 0, 'death_rate': 0
        }

    survived = df_subset['survived'].sum()
    died = total - survived
    surv_rate = (survived / total) * 100
    death_rate = (died / total) * 100

    print(f"\n--- {title} ---")
    print(f"Total Lineages: {total}")
    print(f"Survived:       {survived}")
    print(f"Died:           {died}")
    print(f"Survival Rate:  {surv_rate:.2f}%")
    print(f"Death Rate:     {death_rate:.2f}%")
    
    return {
        'total': total, 'survived': survived, 'died': died, 
        'survival_rate': surv_rate, 'death_rate': death_rate
    }

# --- Main Execution ---
all_files = glob.glob(os.path.join(INPUT_FOLDER, "*.xml"))
all_data = []

print(f"Processing {len(all_files)} XML files in '{INPUT_FOLDER}'...")

for file_path in all_files:
    result = analyze_clone_evolution(file_path)
    if result:
        all_data.extend(result)

# --- Result Consolidation ---
if all_data:
    df = pd.DataFrame(all_data)
    
    print("=" * 40)
    print("      SURVIVAL ANALYSIS BY AUTHOR")
    print("=" * 40)

    # 1. Global Stats
    global_stats = print_stats("GLOBAL RESULTS", df)
    
    # 2. Developer Stats
    dev_df = df[df['author_type'] == 'Developer']
    dev_stats = print_stats("DEVELOPER RESULTS", dev_df)

    # 3. Agent Stats
    agent_df = df[df['author_type'] == 'Agent']
    agent_stats = print_stats("AGENT RESULTS", agent_df)

    print("=" * 40)

    # --- Saving Results ---
    
    # 1. Detailed CSV
    output_csv = os.path.join(OUTPUT_FOLDER, "survival_analysis_by_author.csv")
    df.to_csv(output_csv, index=False)
    print(f"Detailed CSV saved at: {output_csv}")
    
    # 2. Summary Text File
    output_txt = os.path.join(OUTPUT_FOLDER, "survival_summary_by_author.txt")
    with open(output_txt, "w") as f:
        f.write("=== SURVIVAL ANALYSIS REPORT ===\n\n")
        
        f.write("--- DEVELOPERS ---\n")
        f.write(f"Total: {dev_stats['total']}\n")
        f.write(f"Survived: {dev_stats['survived']} ({dev_stats['survival_rate']:.2f}%)\n")
        f.write(f"Died: {dev_stats['died']} ({dev_stats['death_rate']:.2f}%)\n\n")
        
        f.write("--- AGENTS ---\n")
        f.write(f"Total: {agent_stats['total']}\n")
        f.write(f"Survived: {agent_stats['survived']} ({agent_stats['survival_rate']:.2f}%)\n")
        f.write(f"Died: {agent_stats['died']} ({agent_stats['death_rate']:.2f}%)\n\n")

        f.write("--- GLOBAL ---\n")
        f.write(f"Total: {global_stats['total']}\n")
        f.write(f"Survived: {global_stats['survived']} ({global_stats['survival_rate']:.2f}%)\n")
        f.write(f"Died: {global_stats['died']} ({global_stats['death_rate']:.2f}%)\n")

    print(f"Summary Text saved at: {output_txt}")

else:
    print("No valid data found or input folder is empty.")