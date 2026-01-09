import os
import xml.etree.ElementTree as ET
import pandas as pd
from pathlib import Path
from utils.folders_paths import genealogy_results_path, metrics_path

def extract_patterns_from_xml(xml_file):
    """
    Extract evolution and change patterns from an XML file
    Separates by lineage creator type (human or agent)
    """
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    results = {
        'human_lineages': {
            'evolution_subtract': [],
            'evolution_add': [],
            'change_inconsistent': [],
            'change_consistent': []
        },
        'agent_lineages': {
            'evolution_subtract': [],
            'evolution_add': [],
            'change_inconsistent': [],
            'change_consistent': []
        }
    }
    
    # Iterate over all lineages
    for lineage in root.findall('.//lineage'):
        versions = lineage.findall('version')
        
        if not versions:
            continue
        
        # Determine lineage creator (first version author)
        first_version = versions[0]
        first_author = first_version.get('author', '')
        
        if first_author == 'human':
            lineage_creator = 'human_lineages'
        else:
            lineage_creator = 'agent_lineages'
        
        # Iterate over all versions in this lineage
        for version in versions:
            evolution = version.get('evolution', '')
            change = version.get('change', '')
            n_evo = int(version.get('n_evo', '0'))
            n_cha = int(version.get('n_cha', '0'))
            author = version.get('author', '')
            number_pr = version.get('number_pr', '')
            
            # Evolution Patterns
            if evolution == 'Subtract':
                results[lineage_creator]['evolution_subtract'].append({
                    'n_evo': n_evo,
                    'author': author,
                    'pr': number_pr
                })
            elif evolution == 'Add':
                results[lineage_creator]['evolution_add'].append({
                    'n_evo': n_evo,
                    'author': author,
                    'pr': number_pr
                })
            
            # Change Patterns
            if change == 'Inconsistent':
                results[lineage_creator]['change_inconsistent'].append({
                    'n_cha': n_cha,
                    'author': author,
                    'pr': number_pr
                })
            elif change == 'Consistent':
                results[lineage_creator]['change_consistent'].append({
                    'n_cha': n_cha,
                    'author': author,
                    'pr': number_pr
                })
    
    return results

def process_all_xml_files(directory):
    """
    Process all XML files in a directory
    Separates results by lineage creator type (human or agent)
    """
    all_evolution_data = []
    all_change_data = []
    
    xml_files = list(Path(directory).glob('*.xml'))
    print(f"Processing {len(xml_files)} XML files from {directory}...")
    
    for xml_file in xml_files:
        print(f"  Processing {xml_file.name}...")
        
        # Extract information from filename
        filename = xml_file.stem  # Remove .xml extension
        parts = filename.split('_')
        
        if len(parts) >= 3:
            repo_owner = parts[1]
            repo_name = '_'.join(parts[2:])
        else:
            repo_owner = 'unknown'
            repo_name = filename
        
        results = extract_patterns_from_xml(xml_file)
        
        # Process both human and agent lineages
        for lineage_type in ['human_lineages', 'agent_lineages']:
            lineage_creator = 'human' if lineage_type == 'human_lineages' else 'agent'
            
            # Evolution Patterns
            for pattern_type, items in [('Subtract', results[lineage_type]['evolution_subtract']), 
                                         ('Add', results[lineage_type]['evolution_add'])]:
                total_n_evo = sum(item['n_evo'] for item in items)
                count_occurrences = len(items)
                
                # Separate by author (who modified the lineage)
                human_items = [item for item in items if item['author'] == 'human']
                agent_items = [item for item in items if item['author'] == 'agent']
                
                all_evolution_data.append({
                    'project': f"{repo_owner}/{repo_name}",
                    'lineage_creator': lineage_creator,
                    'evolution_pattern': pattern_type,
                    'total_occurrences': count_occurrences,
                    'sum_n_evo': total_n_evo,
                    'human_occurrences': len(human_items),
                    'human_sum_n_evo': sum(item['n_evo'] for item in human_items),
                    'agent_occurrences': len(agent_items),
                    'agent_sum_n_evo': sum(item['n_evo'] for item in agent_items)
                })
            
            # Change Patterns
            for pattern_type, items in [('Inconsistent', results[lineage_type]['change_inconsistent']), 
                                         ('Consistent', results[lineage_type]['change_consistent'])]:
                total_n_cha = sum(item['n_cha'] for item in items)
                count_occurrences = len(items)
                
                # Separate by author (who modified the lineage)
                human_items = [item for item in items if item['author'] == 'human']
                agent_items = [item for item in items if item['author'] == 'agent']
                
                all_change_data.append({
                    'project': f"{repo_owner}/{repo_name}",
                    'lineage_creator': lineage_creator,
                    'change_pattern': pattern_type,
                    'total_occurrences': count_occurrences,
                    'sum_n_cha': total_n_cha,
                    'human_occurrences': len(human_items),
                    'human_sum_n_cha': sum(item['n_cha'] for item in human_items),
                    'agent_occurrences': len(agent_items),
                    'agent_sum_n_cha': sum(item['n_cha'] for item in agent_items)
                })
    
    return all_evolution_data, all_change_data

if __name__ == '__main__':
    base_dir = Path(__file__).resolve().parent.parent
    xml_dir = genealogy_results_path
    
    evolution_data, change_data = process_all_xml_files(xml_dir)
    
    # Create DataFrames
    df_evolution = pd.DataFrame(evolution_data)
    df_change = pd.DataFrame(change_data)
    
    # Process Human Lineages
    print("\n" + "="*100)
    print("ANALYSIS OF LINEAGES CREATED BY HUMANS")
    print("="*100)
    
    df_evolution_human = df_evolution[df_evolution['lineage_creator'] == 'human'].sort_values(['project', 'evolution_pattern'])
    df_change_human = df_change[df_change['lineage_creator'] == 'human'].sort_values(['project', 'change_pattern'])
    
    print("\n" + "-"*100)
    print("EVOLUTION PATTERNS (Human Lineages)")
    print("-"*100)
    
    if not df_evolution_human.empty:
        evolution_summary_human = df_evolution_human.groupby('evolution_pattern').agg({
            'total_occurrences': 'sum',
            'sum_n_evo': 'sum',
            'human_occurrences': 'sum',
            'human_sum_n_evo': 'sum',
            'agent_occurrences': 'sum',
            'agent_sum_n_evo': 'sum'
        })
        print(evolution_summary_human)
        
        # Add total row
        totals_evolution_human = pd.DataFrame({
            'total_occurrences': [evolution_summary_human['total_occurrences'].sum()],
            'sum_n_evo': [evolution_summary_human['sum_n_evo'].sum()],
            'human_occurrences': [evolution_summary_human['human_occurrences'].sum()],
            'human_sum_n_evo': [evolution_summary_human['human_sum_n_evo'].sum()],
            'agent_occurrences': [evolution_summary_human['agent_occurrences'].sum()],
            'agent_sum_n_evo': [evolution_summary_human['agent_sum_n_evo'].sum()]
        }, index=['TOTAL'])
        
        print("-"*100)
        print(totals_evolution_human)
    else:
        print("No evolution patterns found for human lineages.")
    
    print("\n" + "-"*100)
    print("CHANGE PATTERNS (Human Lineages)")
    print("-"*100)
    
    if not df_change_human.empty:
        change_summary_human = df_change_human.groupby('change_pattern').agg({
            'total_occurrences': 'sum',
            'sum_n_cha': 'sum',
            'human_occurrences': 'sum',
            'human_sum_n_cha': 'sum',
            'agent_occurrences': 'sum',
            'agent_sum_n_cha': 'sum'
        })
        print(change_summary_human)
        
        # Add total row
        totals_change_human = pd.DataFrame({
            'total_occurrences': [change_summary_human['total_occurrences'].sum()],
            'sum_n_cha': [change_summary_human['sum_n_cha'].sum()],
            'human_occurrences': [change_summary_human['human_occurrences'].sum()],
            'human_sum_n_cha': [change_summary_human['human_sum_n_cha'].sum()],
            'agent_occurrences': [change_summary_human['agent_occurrences'].sum()],
            'agent_sum_n_cha': [change_summary_human['agent_sum_n_cha'].sum()]
        }, index=['TOTAL'])
        
        print("-"*100)
        print(totals_change_human)
    else:
        print("No change patterns found for human lineages.")
    
    # Process Agent Lineages
    print("\n" + "="*100)
    print("ANALYSIS OF LINEAGES CREATED BY AGENTS")
    print("="*100)
    
    df_evolution_agent = df_evolution[df_evolution['lineage_creator'] == 'agent'].sort_values(['project', 'evolution_pattern'])
    df_change_agent = df_change[df_change['lineage_creator'] == 'agent'].sort_values(['project', 'change_pattern'])
    
    print("\n" + "-"*100)
    print("EVOLUTION PATTERNS (Agent Lineages)")
    print("-"*100)
    
    if not df_evolution_agent.empty:
        evolution_summary_agent = df_evolution_agent.groupby('evolution_pattern').agg({
            'total_occurrences': 'sum',
            'sum_n_evo': 'sum',
            'human_occurrences': 'sum',
            'human_sum_n_evo': 'sum',
            'agent_occurrences': 'sum',
            'agent_sum_n_evo': 'sum'
        })
        print(evolution_summary_agent)
        
        # Add total row
        totals_evolution_agent = pd.DataFrame({
            'total_occurrences': [evolution_summary_agent['total_occurrences'].sum()],
            'sum_n_evo': [evolution_summary_agent['sum_n_evo'].sum()],
            'human_occurrences': [evolution_summary_agent['human_occurrences'].sum()],
            'human_sum_n_evo': [evolution_summary_agent['human_sum_n_evo'].sum()],
            'agent_occurrences': [evolution_summary_agent['agent_occurrences'].sum()],
            'agent_sum_n_evo': [evolution_summary_agent['agent_sum_n_evo'].sum()]
        }, index=['TOTAL'])
        
        print("-"*100)
        print(totals_evolution_agent)
    else:
        print("No evolution patterns found for agent lineages.")
    
    print("\n" + "-"*100)
    print("CHANGE PATTERNS (Agent Lineages)")
    print("-"*100)
    
    if not df_change_agent.empty:
        change_summary_agent = df_change_agent.groupby('change_pattern').agg({
            'total_occurrences': 'sum',
            'sum_n_cha': 'sum',
            'human_occurrences': 'sum',
            'human_sum_n_cha': 'sum',
            'agent_occurrences': 'sum',
            'agent_sum_n_cha': 'sum'
        })
        print(change_summary_agent)
        
        # Add total row
        totals_change_agent = pd.DataFrame({
            'total_occurrences': [change_summary_agent['total_occurrences'].sum()],
            'sum_n_cha': [change_summary_agent['sum_n_cha'].sum()],
            'human_occurrences': [change_summary_agent['human_occurrences'].sum()],
            'human_sum_n_cha': [change_summary_agent['human_sum_n_cha'].sum()],
            'agent_occurrences': [change_summary_agent['agent_occurrences'].sum()],
            'agent_sum_n_cha': [change_summary_agent['agent_sum_n_cha'].sum()]
        }, index=['TOTAL'])
        
        print("-"*100)
        print(totals_change_agent)
    else:
        print("No change patterns found for agent lineages.")
    
    # Save the CSVs
    os.makedirs(metrics_path, exist_ok=True)
    
    evolution_csv = os.path.join(metrics_path, 'evolution_patterns_summary.csv')
    change_csv = os.path.join(metrics_path, 'change_patterns_summary.csv')
    
    df_evolution.to_csv(evolution_csv, index=False)
    df_change.to_csv(change_csv, index=False)
    
    print(f"\n✅ Files saved:")
    print(f"   - {evolution_csv}")
    print(f"   - {change_csv}")

