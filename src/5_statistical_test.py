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
    if author_name and author_name.strip() == "Developer":
        return "Developer"
    return "Agent"

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

def run_chi_square(df, context_name, pattern_column):
    """
    Performs the Chi-Square test for a specific pattern column.
    """
    print(f"  > Analyzing {pattern_column} ({context_name})...")
    
    # Create Contingency Table
    contingency_table = pd.crosstab(df['Author'], df[pattern_column])
    
    # Check if we have enough variance
    if contingency_table.shape[0] < 2 or contingency_table.shape[1] < 2:
        print(f"    [!] Not enough variance to run statistics (Data is uniform).")
        # print(f"    Distribution:\n{contingency_table}\n")
        return

    # Run Test
    chi2, p, dof, expected = chi2_contingency(contingency_table)
    
    print(f"    p-value: {p:.5f}")
    if p < 0.05:
        print(f"    *** RESULT: SIGNIFICANT difference found between Developer and Agent. ***")
        
        # Calculate percentages to see WHO performs more of which pattern
        percentages = pd.crosstab(df['Author'], df[pattern_column], normalize='index') * 100
        print(f"    Proportions (%):\n{percentages.round(2)}\n")
    else:
        print(f"    Result: No significant difference observed.\n")

def main():
    # 1. Load Data
    if not os.path.exists(INPUT_FOLDER):
        print(f"Error: Folder '{INPUT_FOLDER}' not found.")
        return

    df, project_counts = load_data(INPUT_FOLDER)
    
    if df.empty:
        print("No valid data found in XML files.")
        return

    # 2. Iterate through each Language (Per Language Analysis)
    unique_languages = df['Language'].unique()
    
    print("\n" + "="*50)
    print("PART 1: STATISTICAL ANALYSIS PER LANGUAGE")
    print("="*50)

    for language in unique_languages:
        print(f"\n[[ LANGUAGE: {language} ]]")
        
        # Project Count Report
        n_projects = len(project_counts.get(language, []))
        print(f"Projects: {n_projects}")
        
        # Filter data for this language
        lang_df = df[df['Language'] == language]
        print(f"Records: {len(lang_df)}")

        if len(lang_df) < 10:
            print("  [!] Insufficient data.")
            continue

        # Statistical Tests
        run_chi_square(lang_df, language, 'ChangePattern')
        run_chi_square(lang_df, language, 'EvolutionPattern')
        
        print("-" * 30)

    # 3. Overall Analysis (All Languages Combined)
    print("\n" + "="*50)
    print("PART 2: OVERALL STATISTICAL ANALYSIS (ALL LANGUAGES)")
    print("="*50)
    
    total_projects = sum(len(p) for p in project_counts.values())
    print(f"Total Projects: {total_projects}")
    print(f"Total Records: {len(df)}")
    
    run_chi_square(df, "ALL DATA", 'ChangePattern')
    run_chi_square(df, "ALL DATA", 'EvolutionPattern')
    
    print("="*50)
    print("Analysis Complete.")

if __name__ == "__main__":
    main()