import xml.etree.ElementTree as ET
from pathlib import Path
import pandas as pd
from utils.folders_paths import genealogy_results_path, main_results

def analyze_xml_file(xml_path):
    """
    Analyze a single XML file to count lineages and clones created by humans and agents.
    Also determine which lineages are alive (present in latest version) vs dead.
    
    Returns:
        dict: Dictionary with lineage count, human clones, agent clones, and alive/dead lineages by author
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()
    
    total_lineages = 0
    human_created_clones = 0
    agent_created_clones = 0
    
    # Find the maximum version number across all lineages
    max_version_nr = 0
    
    for lineage in root.findall('lineage'):
        for version in lineage.findall('version'):
            version_nr = int(version.get('nr', 0))
            if version_nr > max_version_nr:
                max_version_nr = version_nr
    
    # Now analyze each lineage - track alive/dead by author
    alive_lineages = 0
    dead_lineages = 0
    human_alive = 0
    human_dead = 0
    agent_alive = 0
    agent_dead = 0
    total_versions = 0  # Track total number of versions across all lineages
    evolution_count = 0  # Count Add or Subtract
    change_count = 0  # Count Consistent or Inconsistent
    
    for lineage in root.findall('lineage'):
        total_lineages += 1
        
        # Find the first version (the one with the smallest nr value)
        versions = lineage.findall('version')
        total_versions += len(versions)  # Count versions in this lineage
        creator_author = None
        
        # Count evolution and change patterns across all versions in this lineage
        for version in versions:
            evolution = version.get('evolution', '')
            change = version.get('change', '')
            
            if evolution in ['Add', 'Subtract']:
                evolution_count += 1
            
            if change in ['Consistent', 'Inconsistent']:
                change_count += 1
        
        if versions:
            # Sort versions by nr to find the first one
            first_version = min(versions, key=lambda v: int(v.get('nr', 0)))
            
            evolution = first_version.get('evolution')
            change = first_version.get('change')
            author = first_version.get('author')
            
            # Check if it's a clone creation (evolution="None" and change="None")
            if evolution == "None" and change == "None":
                creator_author = author
                if author == "human":
                    human_created_clones += 1
                elif author == "agent":
                    agent_created_clones += 1
        
        # Check if this lineage has a version with the maximum version number
        has_max_version = False
        for version in lineage.findall('version'):
            if int(version.get('nr', 0)) == max_version_nr:
                has_max_version = True
                break
        
        if has_max_version:
            alive_lineages += 1
            if creator_author == "human":
                human_alive += 1
            elif creator_author == "agent":
                agent_alive += 1
        else:
            dead_lineages += 1
            if creator_author == "human":
                human_dead += 1
            elif creator_author == "agent":
                agent_dead += 1
    
    # Calculate average lineage size
    avg_versions_per_lineage = total_versions / total_lineages if total_lineages > 0 else 0
    
    return {
        'total_lineages': total_lineages,
        'human_created_clones': human_created_clones,
        'agent_created_clones': agent_created_clones,
        'alive_lineages': alive_lineages,
        'dead_lineages': dead_lineages,
        'human_alive': human_alive,
        'human_dead': human_dead,
        'agent_alive': agent_alive,
        'agent_dead': agent_dead,
        'max_version_nr': max_version_nr,
        'total_versions': total_versions,
        'avg_versions_per_lineage': avg_versions_per_lineage,
        'evolution_count': evolution_count,
        'change_count': change_count
    }


def main():
    results_dir = Path(genealogy_results_path)
    csv_path = Path(main_results) / "balanced_repositories.csv"
    
    # Load PR counts from CSV
    df_repos = pd.read_csv(csv_path)
    pr_counts = {}
    for _, row in df_repos.iterrows():
        repo_name = row['full_name'].replace('/', '_')
        pr_counts[repo_name] = row['total_prs']
    
    # Find all XML files
    xml_files = sorted(results_dir.glob('*.xml'))
    
    print("=" * 80)
    print("CLONE LINEAGE ANALYSIS")
    print("=" * 80)
    print()
    
    all_results = {}
    
    for xml_file in xml_files:
        project_name = xml_file.stem  # Get filename without extension
        result = analyze_xml_file(xml_file)
        
        # Extract repo identifier from filename (e.g., "cs_dotnet_aspire" -> "dotnet_aspire")
        parts = project_name.split('_')
        if len(parts) >= 3:
            repo_key = f"{parts[1]}_{parts[2]}"
        else:
            repo_key = project_name
        
        # Add total PRs from CSV
        result['total_prs'] = pr_counts.get(repo_key, 0)
        all_results[project_name] = result
    
    # Summary statistics
    print("=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)
    print()
    
    total_lineages = sum(r['total_lineages'] for r in all_results.values())
    total_human_clones = sum(r['human_created_clones'] for r in all_results.values())
    total_agent_clones = sum(r['agent_created_clones'] for r in all_results.values())
    total_alive = sum(r['alive_lineages'] for r in all_results.values())
    total_dead = sum(r['dead_lineages'] for r in all_results.values())
    total_human_alive = sum(r['human_alive'] for r in all_results.values())
    total_human_dead = sum(r['human_dead'] for r in all_results.values())
    total_agent_alive = sum(r['agent_alive'] for r in all_results.values())
    total_agent_dead = sum(r['agent_dead'] for r in all_results.values())
    
    print(f"Total Projects Analyzed: {len(all_results)}")
    print(f"Total Lineages: {total_lineages}")
    print(f"Total Clones Created by Humans: {total_human_clones}")
    print(f"Total Clones Created by Agents: {total_agent_clones}")
    print(f"Total Alive Lineages: {total_alive}")
    print(f"Total Dead Lineages: {total_dead}")
    print()
    
    if total_human_clones + total_agent_clones > 0:
        human_percentage = (total_human_clones / (total_human_clones + total_agent_clones)) * 100
        agent_percentage = (total_agent_clones / (total_human_clones + total_agent_clones)) * 100
        print(f"Human Created: {human_percentage:.2f}%")
        print(f"Agent Created: {agent_percentage:.2f}%")
    
    if total_lineages > 0:
        alive_percentage = (total_alive / total_lineages) * 100
        dead_percentage = (total_dead / total_lineages) * 100
        print(f"Alive Lineages: {alive_percentage:.2f}%")
        print(f"Dead Lineages: {dead_percentage:.2f}%")
    
    # Lineage survival by creator
    print()
    print("LINEAGE SURVIVAL BY CREATOR:")
    if total_human_clones > 0:
        human_alive_pct = (total_human_alive / total_human_clones) * 100
        human_dead_pct = (total_human_dead / total_human_clones) * 100
        print(f"  Human-created lineages: {total_human_alive} alive ({human_alive_pct:.2f}%), {total_human_dead} dead ({human_dead_pct:.2f}%)")
    
    if total_agent_clones > 0:
        agent_alive_pct = (total_agent_alive / total_agent_clones) * 100
        agent_dead_pct = (total_agent_dead / total_agent_clones) * 100
        print(f"  Agent-created lineages: {total_agent_alive} alive ({agent_alive_pct:.2f}%), {total_agent_dead} dead ({agent_dead_pct:.2f}%)")
    
    print()
    print("=" * 80)
    print("DETAILED PROJECT BREAKDOWN")
    print("=" * 80)
    print()
    
    # Create a formatted table
    print(f"{'Project':<40} {'Lineages':<10} {'Human':<8} {'Agent':<8} {'Alive':<8} {'Dead':<8} {'Avg Size':<10} {'Total PRs':<10} {'Evolution':<11} {'Change':<11}")
    print("-" * 130)
    
    total_prs_sum = 0
    total_evolution = 0
    total_change = 0
    
    for project_name, result in sorted(all_results.items()):
        total_prs_sum += result.get('total_prs', 0)
        total_evolution += result.get('evolution_count', 0)
        total_change += result.get('change_count', 0)
        print(f"{project_name:<40} "
              f"{result['total_lineages']:<10} "
              f"{result['human_created_clones']:<8} "
              f"{result['agent_created_clones']:<8} "
              f"{result['alive_lineages']:<8} "
              f"{result['dead_lineages']:<8} "
              f"{int(round(result['avg_versions_per_lineage'])):<10} "
              f"{result.get('total_prs', 0):<10} "
              f"{result.get('evolution_count', 0):<11} "
              f"{result.get('change_count', 0):<11}")
    
    # Calculate overall average
    total_versions = sum(r['total_versions'] for r in all_results.values())
    overall_avg = total_versions / total_lineages if total_lineages > 0 else 0
    
    print("-" * 130)
    print(f"{'TOTAL':<40} "
          f"{total_lineages:<10} "
          f"{total_human_clones:<8} "
          f"{total_agent_clones:<8} "
          f"{total_alive:<8} "
          f"{total_dead:<8} "
          f"{int(round(overall_avg)):<10} "
          f"{total_prs_sum:<10} "
          f"{total_evolution:<11} "
          f"{total_change:<11}")
    
    print()


if __name__ == '__main__':
    main()
