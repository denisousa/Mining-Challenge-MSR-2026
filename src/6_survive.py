import os
import glob
import pandas as pd
import xml.etree.ElementTree as ET

# --- Configuration ---
INPUT_FOLDER = "03_results"
OUTPUT_FOLDER = "06_results"
PR_CSV_PATH = "02_results/projects_with_pr_sha.csv"

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Load PR counts for each project
def load_pr_counts():
    """
    Loads the PR CSV and calculates the final nr for each project.
    Returns a dictionary: {full_name: final_nr}
    """
    df = pd.read_csv(PR_CSV_PATH)
    # Count PRs per project and add 1
    pr_counts = df.groupby('full_name').size().to_dict()
    # final_nr = number of PRs + 1
    return {project: count + 1 for project, count in pr_counts.items()}

# Load PR counts at module level
PROJECT_FINAL_NR = load_pr_counts()

def extract_project_info(filename):
    """
    Extracts language and full_name from XML filename.
    Example: 'cs_OpenAI_Codex_wieslawsoltes_Dock.xml' 
    -> language='C#', full_name='wieslawsoltes/Dock'
    """
    # Remove .xml extension
    name = filename.replace('.xml', '')
    
    # Split by underscore
    parts = name.split('_')
    
    if len(parts) < 4:
        return None, None
    
    # Language mapping
    lang_map = {
        'cs': 'C#',
        'py': 'Python',
        'java': 'Java',
        'rb': 'Ruby',
        'php': 'PHP'
    }
    
    language = lang_map.get(parts[0])
    
    # Author and repo are the last two parts before extension
    # Handle cases with underscores in author/repo names
    author = parts[-2]
    repo = parts[-1]
    full_name = f"{author}/{repo}"
    
    return language, full_name

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

    # Extract project information from filename
    filename = os.path.basename(file_path)
    language, full_name = extract_project_info(filename)
    
    if not full_name or full_name not in PROJECT_FINAL_NR:
        print(f"Warning: Could not find PR count for project in {filename}")
        return None
    
    # Get the final nr from PR count
    max_project_nr = PROJECT_FINAL_NR[full_name]
    
    lineage_status = []

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