from pathlib import Path
import subprocess
import shutil
import os

class NiCadTimeout(Exception):
    """Exceção para timeout do NiCad."""
    pass

def _nicad_timeout_handler(signum, frame):
    raise NiCadTimeout("NiCad execution exceeded timeout")

def remove_logs_and_xml_files(directory):
    for file_name in os.listdir(directory):
        file_path = os.path.join(directory, file_name)
        
        if os.path.isfile(file_path) and (file_name.endswith('.log') or file_name.endswith('.xml')):
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"Remove error {file_path}: {e}")

def run_nicad(git_repository_path, languague, result_path):
    print(" >>> Running nicad6...")
    repo_name = git_repository_path.split("/")[-1]
    git_repository_path = os.path.abspath(git_repository_path)
    subprocess.run(["./nicad6", "functions", languague, git_repository_path],
                cwd="NiCad",
                check=True)

    nicad_xml = f"{git_repository_path}_functions-clones/{repo_name}_functions-clones-0.30-classes.xml"
    shutil.move(nicad_xml, result_path)
    clones_dir = Path(f"{git_repository_path}_functions-clones")
    shutil.rmtree(clones_dir, ignore_errors=True)
    remove_logs_and_xml_files("repos")

    print("Finished clone detection.\n")