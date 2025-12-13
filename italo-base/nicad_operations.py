import subprocess
import shutil

def run_nicad(git_repository_path, result_path):
    print(" >>> Running nicad6...")
    subprocess.run(["./nicad6", "functions", "java", git_repository_path],
                cwd="NiCad",
                check=True)

    nicad_xml = f"{git_repository_path}_functions-clones/production_functions-clones-0.20-classes.xml"
    shutil.move(nicad_xml, result_path)
    clones_dir = Path(f"{git_repository_path}_functions-clones")
    shutil.rmtree(clones_dir, ignore_errors=True)

    # data_dir = Path(ctx.paths.data_dir)
    # for log_file in data_dir.glob("*.log"):
    #     try:
    #         log_file.unlink()
    #     except FileNotFoundError:
    #         pass
    #     except PermissionError:
    #         pass

    print("Finished clone detection.\n")