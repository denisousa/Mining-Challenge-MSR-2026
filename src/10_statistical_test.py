'''
Docstring for 5_statistical_test
This code was made using Gemini 3: https://gemini.google.com/share/4ea36988955e
'''

import os
import glob
import xml.etree.ElementTree as ET
import pandas as pd
from scipy.stats import chi2_contingency

# Configuration
INPUT_FOLDER = "03_results"
OUTPUT_FOLDER = "09_results"

def parse_filename(filename):
    """
    Extracts the language and project name from the filename.
    Expected format: <language>_<project_name>.xml
    """
    basename = os.path.basename(filename)
    name_without_ext = os.path.splitext(basename)[0]
    
    # Split by the first underscore only
    parts = name_without_ext.split('_', 1)
    
    if len(parts) >= 2:
        language = parts[0]
        project_name = parts[1]
    else:
        language = "Unknown"
        project_name = name_without_ext
        
    return language, project_name

def normalize_author(author_name):
    """
    Normalizes the author name.
    If author is 'Developer', returns 'Developer'.
    Everything else is considered an 'Agent'.
    """
    if author_name and author_name.strip() == "human":
        return "human"
    return "agent"

def load_data(folder_path):
    """
    Iterates through all XML files in the folder and aggregates data.
    """
    all_data = []
    project_counts = {}

    # Find all XML files
    files = glob.glob(os.path.join(folder_path, "*.xml"))
    
    print(f"Found {len(files)} XML files in '{folder_path}'. Processing...")

    for file_path in files:
        language, project_name = parse_filename(file_path)
        
        # Count projects per language
        if language not in project_counts:
            project_counts[language] = set()
        project_counts[language].add(project_name)

        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            for lineage in root.findall('lineage'):
                for version in lineage.findall('version'):
                    raw_author = version.get('author')
                    change = version.get('change')
                    evolution = version.get('evolution')
                    
                    # Normalize Author (Developer vs Agent)
                    author = normalize_author(raw_author)
                    
                    # Filter out initialization steps (None)
                    if change != "None" and evolution != "None" and raw_author is not None:
                        all_data.append({
                            'Language': language,
                            'Project': project_name,
                            'Author': author,  # Now standardized
                            'ChangePattern': change,
                            'EvolutionPattern': evolution
                        })
                        
        except ET.ParseError:
            print(f"Error parsing file: {file_path}")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    return pd.DataFrame(all_data), project_counts

def run_chi_square(df, context_name, pattern_column, exclude_same=False):
    """
    Performs the Chi-Square test for a specific pattern column.
    Returns a dictionary with the test results.
    """
    print(f"\n  > Analyzing relationship between AUTHOR and {pattern_column} ({context_name})...")
    
    # Filter out "Same" if requested
    df_filtered = df.copy()
    if exclude_same:
        df_filtered = df_filtered[df_filtered[pattern_column] != 'Same']
        print(f"    [Excluding 'Same' pattern - {len(df_filtered)} records remaining]")
    
    # Create Contingency Table
    contingency_table = pd.crosstab(df_filtered['Author'], df_filtered[pattern_column])
    
    print(f"    Contingency Table:")
    print(contingency_table)
    print()
    
    # Check if we have enough variance
    if contingency_table.shape[0] < 2 or contingency_table.shape[1] < 2:
        print(f"    [!] Not enough variance to run statistics (Data is uniform).")
        return {
            'context': context_name,
            'pattern_type': pattern_column,
            'chi2': None,
            'p_value': None,
            'dof': None,
            'significant': False,
            'correlation_exists': 'No - Insufficient variance',
            'notes': 'Not enough variance'
        }

    # Run Chi-Square Test
    chi2, p, dof, expected = chi2_contingency(contingency_table)
    
    print(f"    Chi-Square Statistics:")
    print(f"    - χ² (chi-square) = {chi2:.4f}")
    print(f"    - p-value = {p:.5f}")
    print(f"    - degrees of freedom = {dof}")
    
    # Determine if correlation exists
    significant = p < 0.05
    correlation_interpretation = ""
    
    if significant:
        correlation_interpretation = "YES - Significant correlation exists"
        print(f"\n    *** CORRELATION FOUND: There IS a statistically significant relationship")
        print(f"        between AUTHOR and {pattern_column} (p < 0.05) ***")
        
        # Calculate percentages to see WHO performs more of which pattern
        percentages = pd.crosstab(df_filtered['Author'], df_filtered[pattern_column], normalize='index') * 100
        print(f"\n    Distribution by Author (%):")
        print(percentages.round(2))
        print()
    else:
        correlation_interpretation = "NO - No significant correlation"
        print(f"\n    Result: No statistically significant relationship found")
        print(f"            between AUTHOR and {pattern_column} (p >= 0.05)")
        print()



    
    result = {
        'context': context_name,
        'pattern_type': pattern_column,
        'chi2': chi2,
        'p_value': p,
        'dof': dof,
        'significant': significant,
        'correlation_exists': correlation_interpretation,
        'notes': 'Significant correlation found' if significant else 'No significant correlation'
    }
    
    return result

def generate_summary_report(all_results, output_folder):
    """
    Generates a summary report showing which projects have correlations in table format.
    """
    summary_path = os.path.join(output_folder, "correlation_summary.txt")
    
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write("="*100 + "\n")
        f.write("STATISTICAL CORRELATION SUMMARY REPORT\n")
        f.write("="*100 + "\n\n")
        
        # Separate per-project results from overall results
        project_results = [r for r in all_results if r['project'] != 'ALL']
        overall_results = [r for r in all_results if r['project'] == 'ALL']
        
        # Group by project and test type
        projects_dict = {}
        for result in project_results:
            key = f"{result['language']}_{result['project']}"
            if key not in projects_dict:
                projects_dict[key] = {'with_same': {}, 'without_same': {}}
            
            if "NO_SAME" in result['context']:
                if result['pattern_type'] == 'ChangePattern':
                    projects_dict[key]['without_same']['change'] = result
                else:
                    projects_dict[key]['without_same']['evolution'] = result
            else:
                if result['pattern_type'] == 'ChangePattern':
                    projects_dict[key]['with_same']['change'] = result
                else:
                    projects_dict[key]['with_same']['evolution'] = result
        
        # TABLE 1: With 'Same'
        f.write("TABLE 1: CORRELATION RESULTS (WITH 'Same' pattern)\n")
        f.write("-"*100 + "\n")
        f.write(f"{'Project':<60} {'Change Pattern':<20} {'Evolution Pattern':<20}\n")
        f.write("-"*100 + "\n")
        
        for project_key in sorted(projects_dict.keys()):
            data = projects_dict[project_key]['with_same']
            
            change_status = "YES" if data.get('change', {}).get('significant', False) else "NO"
            evolution_status = "YES" if data.get('evolution', {}).get('significant', False) else "NO"
            
            f.write(f"{project_key:<60} {change_status:<20} {evolution_status:<20}\n")
        
        # Add TOTAL row for with_same
        f.write("-"*100 + "\n")
        overall_with_same = {}
        for r in overall_results:
            if "NO_SAME" not in r['context']:
                if r['pattern_type'] == 'ChangePattern':
                    overall_with_same['change'] = r
                else:
                    overall_with_same['evolution'] = r
        
        change_status_total = "YES" if overall_with_same.get('change', {}).get('significant', False) else "NO"
        evolution_status_total = "YES" if overall_with_same.get('evolution', {}).get('significant', False) else "NO"
        
        f.write(f"{'TOTAL (All Projects Combined)':<60} {change_status_total:<20} {evolution_status_total:<20}\n")
        f.write("-"*100 + "\n\n\n")
        
        # TABLE 2: Without 'Same'
        f.write("TABLE 2: CORRELATION RESULTS (WITHOUT 'Same' pattern)\n")
        f.write("-"*100 + "\n")
        f.write(f"{'Project':<60} {'Change Pattern':<20} {'Evolution Pattern':<20}\n")
        f.write("-"*100 + "\n")
        
        for project_key in sorted(projects_dict.keys()):
            data = projects_dict[project_key]['without_same']
            
            change_status = "YES" if data.get('change', {}).get('significant', False) else "NO"
            evolution_status = "YES" if data.get('evolution', {}).get('significant', False) else "NO"
            
            f.write(f"{project_key:<60} {change_status:<20} {evolution_status:<20}\n")
        
        # Add TOTAL row for without_same
        f.write("-"*100 + "\n")
        overall_without_same = {}
        for r in overall_results:
            if "NO_SAME" in r['context']:
                if r['pattern_type'] == 'ChangePattern':
                    overall_without_same['change'] = r
                else:
                    overall_without_same['evolution'] = r
        
        change_status_total = "YES" if overall_without_same.get('change', {}).get('significant', False) else "NO"
        evolution_status_total = "YES" if overall_without_same.get('evolution', {}).get('significant', False) else "NO"
        
        f.write(f"{'TOTAL (All Projects Combined)':<60} {change_status_total:<20} {evolution_status_total:<20}\n")
        f.write("-"*100 + "\n\n")
        
        f.write("="*100 + "\n")
        f.write("Note: YES = Significant correlation found (p < 0.05)\n")
        f.write("      NO  = No significant correlation (p >= 0.05)\n")
        f.write("="*100 + "\n")

def main():
    # Create output folder
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    # Open output file for writing all prints
    output_txt_path = os.path.join(OUTPUT_FOLDER, "statistical_test_output.txt")
    
    with open(output_txt_path, 'w', encoding='utf-8') as output_file:
        # Redirect all prints to the file
        import sys
        original_stdout = sys.stdout
        sys.stdout = output_file
        
        try:
            # 1. Load Data
            if not os.path.exists(INPUT_FOLDER):
                print(f"Error: Folder '{INPUT_FOLDER}' not found.")
                return

            df, project_counts = load_data(INPUT_FOLDER)
    
            if df.empty:
                print("No valid data found in XML files.")
                return
    
            # List to store all statistical test results
            all_results = []

            # ===================================================================
            # PART 1: Per-Project Analysis
            # ===================================================================
            print("\n" + "="*70)
            print("PART 1: STATISTICAL ANALYSIS PER PROJECT")
            print("="*70)
    
            unique_projects = df[['Language', 'Project']].drop_duplicates()
    
            for _, row in unique_projects.iterrows():
                language = row['Language']
                project = row['Project']
        
                print(f"\n{'='*70}")
                print(f"PROJECT: {language}_{project}")
                print(f"{'='*70}")
        
                # Filter data for this project
                project_df = df[(df['Language'] == language) & (df['Project'] == project)]
                print(f"Records: {len(project_df)}")
        
                if len(project_df) < 10:
                    print("  [!] Insufficient data for statistical testing.")
                    continue
        
                # TEST 1: Change Pattern (with Same)
                print(f"\n{'─'*70}")
                print(f"TEST 1: AUTHOR vs CHANGE PATTERNS (with 'Same')")
                print(f"{'─'*70}")
                result = run_chi_square(project_df, f"{language}_{project}", 'ChangePattern', exclude_same=False)
                if result:
                    result['language'] = language
                    result['project'] = project
                    result['n_projects'] = 1
                    result['n_records'] = len(project_df)
                    all_results.append(result)
        
                # TEST 2: Evolution Pattern (with Same)
                print(f"\n{'─'*70}")
                print(f"TEST 2: AUTHOR vs EVOLUTION PATTERNS (with 'Same')")
                print(f"{'─'*70}")
                result = run_chi_square(project_df, f"{language}_{project}", 'EvolutionPattern', exclude_same=False)
                if result:
                    result['language'] = language
                    result['project'] = project
                    result['n_projects'] = 1
                    result['n_records'] = len(project_df)
                    all_results.append(result)
        
                # TEST 3: Change Pattern (without Same)
                print(f"\n{'─'*70}")
                print(f"TEST 3: AUTHOR vs CHANGE PATTERNS (without 'Same')")
                print(f"{'─'*70}")
                result = run_chi_square(project_df, f"{language}_{project}_NO_SAME", 'ChangePattern', exclude_same=True)
                if result:
                    result['language'] = language
                    result['project'] = project
                    result['n_projects'] = 1
                    result['n_records'] = len(project_df[project_df['ChangePattern'] != 'Same'])
                    all_results.append(result)
        
                # TEST 4: Evolution Pattern (without Same)
                print(f"\n{'─'*70}")
                print(f"TEST 4: AUTHOR vs EVOLUTION PATTERNS (without 'Same')")
                print(f"{'─'*70}")
                result = run_chi_square(project_df, f"{language}_{project}_NO_SAME", 'EvolutionPattern', exclude_same=True)
                if result:
                    result['language'] = language
                    result['project'] = project
                    result['n_projects'] = 1
                    result['n_records'] = len(project_df[project_df['EvolutionPattern'] != 'Same'])
                    all_results.append(result)
        
                print("-" * 70)
    
            # ===================================================================
            # PART 2: Overall Analysis (All data combined)
            # ===================================================================
            print("\n" + "="*70)
            print("PART 2: OVERALL STATISTICAL ANALYSIS (ALL PROJECTS COMBINED)")
            print("="*70)
    
            total_projects = sum(len(p) for p in project_counts.values())
            print(f"Total Projects: {total_projects}")
            print(f"Total Records: {len(df)}")
    
            # TEST 1: Author vs Change Pattern (Overall) - WITH "Same"
            print(f"\n{'─'*70}")
            print(f"TEST 1: Relationship between AUTHOR and CHANGE PATTERNS (with 'Same')")
            print(f"{'─'*70}")
            result_change_all = run_chi_square(df, "ALL_DATA", 'ChangePattern', exclude_same=False)
    
            if result_change_all:
                result_change_all['language'] = 'ALL'
                result_change_all['project'] = 'ALL'
                result_change_all['n_projects'] = total_projects
                result_change_all['n_records'] = len(df)
                all_results.append(result_change_all)
    
            # TEST 2: Author vs Evolution Pattern (Overall) - WITH "Same"
            print(f"\n{'─'*70}")
            print(f"TEST 2: Relationship between AUTHOR and EVOLUTION PATTERNS (with 'Same')")
            print(f"{'─'*70}")
            result_evolution_all = run_chi_square(df, "ALL_DATA", 'EvolutionPattern', exclude_same=False)
    
            if result_evolution_all:
                result_evolution_all['language'] = 'ALL'
                result_evolution_all['project'] = 'ALL'
                result_evolution_all['n_projects'] = total_projects
                result_evolution_all['n_records'] = len(df)
                all_results.append(result_evolution_all)
    
            # TEST 3: Author vs Change Pattern (Overall) - WITHOUT "Same"
            print(f"\n{'─'*70}")
            print(f"TEST 3: Relationship between AUTHOR and CHANGE PATTERNS (without 'Same')")
            print(f"{'─'*70}")
            result_change_no_same = run_chi_square(df, "ALL_DATA_NO_SAME", 'ChangePattern', exclude_same=True)
    
            if result_change_no_same:
                result_change_no_same['language'] = 'ALL'
                result_change_no_same['project'] = 'ALL'
                result_change_no_same['n_projects'] = total_projects
                result_change_no_same['n_records'] = len(df[df['ChangePattern'] != 'Same'])
                all_results.append(result_change_no_same)
    
            # TEST 4: Author vs Evolution Pattern (Overall) - WITHOUT "Same"
            print(f"\n{'─'*70}")
            print(f"TEST 4: Relationship between AUTHOR and EVOLUTION PATTERNS (without 'Same')")
            print(f"{'─'*70}")
            result_evolution_no_same = run_chi_square(df, "ALL_DATA_NO_SAME", 'EvolutionPattern', exclude_same=True)
    
            if result_evolution_no_same:
                result_evolution_no_same['language'] = 'ALL'
                result_evolution_no_same['project'] = 'ALL'
                result_evolution_no_same['n_projects'] = total_projects
                result_evolution_no_same['n_records'] = len(df[df['EvolutionPattern'] != 'Same'])
                all_results.append(result_evolution_no_same)
    
            print("\n" + "="*70)
            print("ANALYSIS COMPLETE")
            print("="*70)
    
            # 4. Save results to CSV
            if all_results:
                results_df = pd.DataFrame(all_results)
                output_path = os.path.join(OUTPUT_FOLDER, "statistical_test_results.csv")
                results_df.to_csv(output_path, index=False)
                print(f"\n✓ Results saved to: {output_path}")
            else:
                print("\n[!] No results to save.")
                
        finally:
            # Restore original stdout
            sys.stdout = original_stdout
    
    # Generate summary report
    if all_results:
        generate_summary_report(all_results, OUTPUT_FOLDER)
    
    # Print confirmation to console
    print(f"Detailed output saved to: {output_txt_path}")
    print(f"Summary report saved to: {os.path.join(OUTPUT_FOLDER, 'correlation_summary.txt')}")

if __name__ == "__main__":
    main()