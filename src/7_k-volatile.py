import os
import glob
import xml.etree.ElementTree as ET
import matplotlib.pyplot as plt

# --- Directory Configuration ---
INPUT_FOLDER = '03_results'
OUTPUT_FOLDER = '07_results'

# Ensure the output folder exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def parse_and_plot(file_path):
    """
    Reads an XML file, calculates volatility metrics based on Clone Genealogies,
    and saves the resulting plot.
    """
    filename = os.path.basename(file_path)
    project_name = os.path.splitext(filename)[0]  # Removes the .xml extension
    
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
    except ET.ParseError:
        print(f"[ERROR] Failed to parse XML: {filename}")
        return

    lineages_data = []
    all_versions_global = set()

    # 1. Lineage Data Extraction
    # Iterate over each <lineage> found in the file
    for lineage in root.findall('lineage'):
        versions = []
        for v in lineage.findall('version'):
            try:
                # Extract version number (nr)
                versions.append(int(v.get('nr')))
            except (ValueError, TypeError):
                continue
        
        if versions:
            start_v = min(versions)
            end_v = max(versions)
            lineages_data.append({'start': start_v, 'end': end_v})
            all_versions_global.update(versions)

    if not lineages_data:
        print(f"[WARNING] No lineages found in: {filename}")
        return

    # 2. Alive vs Dead Definition
    # The last version found in THIS file defines the "present" (System Age).
    # Genealogies that do not reach this version are considered "Dead".
    if not all_versions_global:
        print(f"[WARNING] No version data found in: {filename}")
        return

    last_system_version = max(all_versions_global)
    
    dead_genealogies_ages = []
    total_genealogies = len(lineages_data)
    
    for l in lineages_data:
        # Age calculation: Number of versions elapsed
        age = l['end'] - l['start']
        
        # Check if the genealogy is Dead or Alive
        if l['end'] < last_system_version:
            dead_genealogies_ages.append(age)
        else:
            # It is 'Alive' (still present in the last version)
            # Alive genealogies are excluded from the "Disappearance Pace" metric
            pass

    dead_count = len(dead_genealogies_ages)
    
    print(f"Processing {project_name}...")
    print(f"  > Total Genealogies: {total_genealogies} | Dead: {dead_count} | Alive: {total_genealogies - dead_count}")

    if dead_count == 0:
        print(f"  > [SKIP] Not enough dead genealogies to generate volatility plot.\n")
        return

    # 3. Metric Calculation (English Nomenclature)
    dead_genealogies_ages.sort()
    max_age_dead = max(dead_genealogies_ages)
    
    # X-Axis: Age k (from 0 to the maximum age found in dead genealogies)
    k_values = range(0, max_age_dead + 2) 
    
    disappearance_pace = []  # Formerly CDF (Focuses on dead clones only)
    overall_volatility = []  # Formerly R-Volatile (Focuses on total project impact)

    for k in k_values:
        # Count how many dead genealogies lasted 'k' versions or less
        dead_upto_k = sum(1 for age in dead_genealogies_ages if age <= k)
        
        # Metric 1: Disappearance Pace
        # "Of those who died, what fraction died by age K?"
        ratio_dead = dead_upto_k / dead_count if dead_count > 0 else 0
        disappearance_pace.append(ratio_dead)
        
        # Metric 2: Overall Volatility
        # "Of ALL clones in history (alive + dead), what fraction was discarded by age K?"
        ratio_total = dead_upto_k / total_genealogies if total_genealogies > 0 else 0
        overall_volatility.append(ratio_total)

    # 4. Plot Generation and Saving
    plt.figure(figsize=(10, 6))
    
    # Plotting curves with clear English labels
    # Solid Blue Line: Internal decay rate of dead clones
    plt.plot(k_values, disappearance_pace, marker='o', markersize=4, 
             label='Disappearance Pace (dead only)', color='blue')
             
    # Dashed Black Line: System-wide discard rate
    plt.plot(k_values, overall_volatility, marker='x', markersize=4, linestyle='--', 
             label='Overall Volatility (of total)', color='black')
    
    plt.title(f"Volatility Analysis: {project_name}")
    plt.xlabel("Age (in versions)")
    plt.ylabel("Accumulated Ratio")
    plt.ylim(0, 1.05) # Slight upper margin for better visualization
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.legend()
    
    # Save the file
    output_filename = f"{project_name}_volatility.png"
    output_path = os.path.join(OUTPUT_FOLDER, output_filename)
    plt.savefig(output_path)
    plt.close() # Close figure to free memory
    
    print(f"  > Plot saved at: {output_path}\n")

def main():
    # Search for all .xml files in the input folder
    search_pattern = os.path.join(INPUT_FOLDER, '*.xml')
    xml_files = glob.glob(search_pattern)
    
    if not xml_files:
        print(f"No XML files found in folder '{INPUT_FOLDER}'.")
        print("Please ensure the '03_results' folder exists and contains valid .xml files.")
        return

    print(f"Found {len(xml_files)} files. Starting processing...\n")
    
    for file_path in xml_files:
        parse_and_plot(file_path)

    print("Processing complete.")

if __name__ == "__main__":
    main()