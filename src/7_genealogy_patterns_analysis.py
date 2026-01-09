import os
import pandas as pd
import xml.etree.ElementTree as ET
from utils.folders_paths import genealogy_results_path, metrics_path

os.makedirs(metrics_path, exist_ok=True)

print("Starting analysis of clones modifications (Human & Agent)...")

def analyze_clones_modifications(results_folder, clone_creator_type):
    project_results = []
    
    if not os.path.exists(results_folder):
        print(f"Error: The input directory '{results_folder}' does not exist.")
        return None
    
    # Iterate over files in the input folder
    for filename in os.listdir(results_folder):
        if not filename.endswith(".xml"):
            continue
        
        file_path = os.path.join(results_folder, filename)
        
        # Extract project name from filename
        try:
            name_without_ext = os.path.splitext(filename)[0]
            parts = name_without_ext.split('_')
            
            if len(parts) >= 4:
                project_name = f"{parts[-2]}_{parts[-1]}"
                language = parts[0]
            else:
                project_name = name_without_ext
                language = "Unknown"
        except Exception:
            project_name = filename
            language = "Unknown"
        
        # Track patterns by who modified clones in this project
        clone_modifications = {
            "human": {"Consistent": 0, "Inconsistent": 0, "Add": 0, "Subtract": 0},
            "agent": {"Consistent": 0, "Inconsistent": 0, "Add": 0, "Subtract": 0}
        }
        
        has_target_clones = False  # Track if project has clones of target type
        
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Process each lineage (clone genealogy) separately
            for lineage in root.findall(".//lineage"):
                versions = lineage.findall("version")
                
                if not versions:
                    continue
                
                # Check if first version is created by the target type
                first_version = versions[0]
                first_evolution = first_version.get("evolution")
                first_change = first_version.get("change")
                first_author = first_version.get("author")
                
                # Determine if clone was created by target type
                is_target_clone = False
                if clone_creator_type == "human":
                    # Human clone: author == "human"
                    is_target_clone = (first_evolution == "None" and first_change == "None" and 
                                      first_author == "human")
                else:  # agent
                    # Agent clone: author != "human"
                    is_target_clone = (first_evolution == "None" and first_change == "None" and 
                                      first_author != "human")
                
                if is_target_clone:
                    has_target_clones = True  # Mark that this project has target-type clones
                    
                    # Now analyze all subsequent versions (updates)
                    for version in versions:
                        evolution = version.get("evolution")
                        change = version.get("change")
                        author = version.get("author")
                        
                        # Skip creation (None values)
                        if evolution == "None" and change == "None":
                            continue
                        
                        # Define author group
                        if author == "human":
                            author_group = "human"
                        else:
                            author_group = "agent"
                        
                        # Count change patterns (only Consistent and Inconsistent, exclude Same)
                        if change in ["Consistent", "Inconsistent"]:
                            clone_modifications[author_group][change] += 1
                        
                        # Count evolution patterns (only Add and Subtract, exclude Same)
                        if evolution in ["Add", "Subtract"]:
                            clone_modifications[author_group][evolution] += 1
            
            # Calculate totals for this project
            human_consistent = clone_modifications["human"]["Consistent"]
            human_inconsistent = clone_modifications["human"]["Inconsistent"]
            human_add = clone_modifications["human"]["Add"]
            human_subtract = clone_modifications["human"]["Subtract"]
            
            agent_consistent = clone_modifications["agent"]["Consistent"]
            agent_inconsistent = clone_modifications["agent"]["Inconsistent"]
            agent_add = clone_modifications["agent"]["Add"]
            agent_subtract = clone_modifications["agent"]["Subtract"]
            
            # Separate data for change and evolution patterns
            change_total = human_consistent + human_inconsistent + agent_consistent + agent_inconsistent
            evolution_total = human_add + human_subtract + agent_add + agent_subtract
            
            # Add project to results regardless of whether it has target-type clones
            project_results.append({
                "Project": project_name,
                "Human_Consistent": human_consistent,
                "Human_Inconsistent": human_inconsistent,
                "Agent_Consistent": agent_consistent,
                "Agent_Inconsistent": agent_inconsistent,
                "Total_Change": change_total,
                "Human_Add": human_add,
                "Human_Subtract": human_subtract,
                "Agent_Add": agent_add,
                "Agent_Subtract": agent_subtract,
                "Total_Evolution": evolution_total
            })
            
        except ET.ParseError:
            print(f"Warning: Could not parse {filename}. Skipping.")
        except Exception as e:
            print(f"Error processing {filename}: {e}")
    
    df = pd.DataFrame(project_results)
    return df


def save_and_display_results(df, clone_creator_type, output_path):
    creator_label = clone_creator_type.upper()
    
    if df is None or df.empty:
        print(f"No data found for {clone_creator_type}-created clones.")
        return
    
    # Split into two separate tables and sort alphabetically
    df_change_patterns = df[["Project", "Human_Consistent", "Human_Inconsistent", 
                              "Agent_Consistent", "Agent_Inconsistent", "Total_Change"]].sort_values('Project')
    df_evolution_patterns = df[["Project", "Human_Add", "Human_Subtract", 
                                 "Agent_Add", "Agent_Subtract", "Total_Evolution"]].sort_values('Project')
    
    # Display Change Patterns
    print("\n" + "="*100)
    print(f"CHANGE PATTERNS ON {creator_label}-CREATED CLONES (By Project)")
    print("="*100)
    print(df_change_patterns.to_string(index=False))
    
    # Save Change Patterns
    change_output_path = os.path.join(output_path, f"{clone_creator_type}_clones_change_patterns.csv")
    df_change_patterns.to_csv(change_output_path, index=False)
    print(f"\n✓ Saved: {change_output_path}")
    
    # Display Evolution Patterns
    print("\n" + "="*100)
    print(f"EVOLUTION PATTERNS ON {creator_label}-CREATED CLONES (By Project)")
    print("="*100)
    print(df_evolution_patterns.to_string(index=False))
    
    # Save Evolution Patterns
    evolution_output_path = os.path.join(output_path, f"{clone_creator_type}_clones_evolution_patterns.csv")
    df_evolution_patterns.to_csv(evolution_output_path, index=False)
    print(f"\n✓ Saved: {evolution_output_path}")
    
    # Calculate and display summary
    print("\n" + "="*100)
    print(f"SUMMARY - {creator_label}-CREATED CLONES")
    print("="*100)
    print(f"Total projects analyzed: {len(df)}")
    print(f"Total change patterns: {df_change_patterns['Total_Change'].sum()}")
    print(f"Total evolution patterns: {df_evolution_patterns['Total_Evolution'].sum()}")
    print("="*100)


if __name__ == '__main__':
    # --- Execution ---
    print("\n" + "="*100)
    print("ANALYZING HUMAN-CREATED CLONES")
    print("="*100)
    df_human_clones = analyze_clones_modifications(genealogy_results_path, "human")
    save_and_display_results(df_human_clones, "human", metrics_path)

    print("\n" + "="*100)
    print("ANALYZING AGENT-CREATED CLONES")
    print("="*100)
    df_agent_clones = analyze_clones_modifications(genealogy_results_path, "agent")
    save_and_display_results(df_agent_clones, "agent", metrics_path)

    print("\n" + "="*100)
    print("ANALYSIS COMPLETE")
    print("="*100)
