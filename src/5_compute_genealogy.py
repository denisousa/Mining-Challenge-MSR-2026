import os
import pandas as pd
import xml.etree.ElementTree as ET
from collections import defaultdict
from collections import defaultdict
from utils.folders_paths import rq2_path, rq3_path

# Output directory
os.makedirs(rq3_path, exist_ok=True)
output_csv = os.path.join(rq3_dir, "evolution_change_stats.csv")

# Storage: author → evolution/change → count
stats = defaultdict(lambda: {
    "evolution": defaultdict(int),
    "change": defaultdict(int)
})

# Parse XML files
for filename in os.listdir(rq2_path):
    if not filename.endswith(".xml"):
        continue
    
    tree = ET.parse(os.path.join(rq2_path, filename))
    root = tree.getroot()

    for version in root.findall(".//version"):
        author = version.get("author")
        evolution = version.get("evolution")
        change = version.get("change")
        
        stats[author]["evolution"][evolution] += 1
        stats[author]["change"][change] += 1

# Build dataframe rows
rows = []
for author, data in stats.items():
    for evo, count in data["evolution"].items():
        rows.append([author, "evolution", evo, count])
    for ch, count in data["change"].items():
        rows.append([author, "change", ch, count])

# Create DataFrame
df = pd.DataFrame(rows, columns=["author", "type", "category", "count"])

# Save CSV
df.to_csv(output_csv, index=False)

print(f"CSV saved at: {output_csv}")
