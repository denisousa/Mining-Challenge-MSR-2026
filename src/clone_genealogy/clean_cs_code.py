import os
import re

def convert_filescoped_namespace(filepath):
    """
    Converts C# 10 File-scoped namespaces to old-style Block-scoped namespaces.
    Ex: 'namespace X;' -> 'namespace X { ... }'
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # Regex to find 'namespace Name;' (File-scoped)
        # pattern matches: start of line, optional indent, 'namespace', name, semicolon
        pattern = r'^(?P<indent>\s*)namespace\s+(?P<name>[\w\.]+)\s*;'
        
        match = re.search(pattern, content, re.MULTILINE)
        
        if match:
            print(f"[FIXING] Found file-scoped namespace in: {filepath}")
            
            # 1. Replace 'namespace Name;' with 'namespace Name {'
            def replacer(m):
                return f"{m.group('indent')}namespace {m.group('name')} {{"

            new_content = re.sub(pattern, replacer, content, count=1, flags=re.MULTILINE)
            
            # 2. Append the closing brace '}' at the very end of the file
            if not new_content.endswith('\n'):
                new_content += '\n'
            new_content += '}\n'
            
            # Write changes back to file
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(new_content)
                
            return True
            
        return False

    except Exception as e:
        print(f"[ERROR] Could not process {filepath}: {e}")
        return False

def process_directory_cs(directory):
    count = 0
    errors = 0
    print(f"Scanning C# files in: {directory}")
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".cs"):
                path = os.path.join(root, file)
                if convert_filescoped_namespace(path):
                    count += 1
    print(f"\nDone. Files converted: {count}")

