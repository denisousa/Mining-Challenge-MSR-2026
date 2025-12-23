import os
import time
import shutil
import logging
import subprocess
from pathlib import Path
from xml.dom import minidom
import xml.etree.ElementTree as ET
from typing import List, Iterable, Optional
from clone_genealogy.CloneFragment import CloneFragment
from clone_genealogy.CloneClass import CloneClass
from clone_genealogy.CloneVersion import CloneVersion
from clone_genealogy.Lineage import Lineage
from dataclasses import dataclass, field
from clone_genealogy.utils import safe_rmtree
from clone_genealogy.clone_density import compute_clone_density, WriteCloneDensity
from clone_genealogy.git_operations import SetupRepo, GitCheckout, GitFecth
from clone_genealogy.prints_operations import printError, printInfo
from clone_genealogy.compute_time import timed, timeToString
from clone_genealogy.git_operations import get_last_merged_pr_commit
from clone_genealogy.clean_py_code import process_directory_py
from clone_genealogy.clean_cs_code import process_directory_cs
from clone_genealogy.clean_rb_code import process_directory_rb
from utils.folders_paths import genealogy_results_path
from dotenv import load_dotenv


load_dotenv()
token = os.getenv("GITHUB_TOKEN")

os.makedirs(genealogy_results_path, exist_ok=True)

log_file = f'{genealogy_results_path}/errors.log'
if os.path.exists(log_file):
    os.remove(log_file)  # D

logging.basicConfig(filename=log_file, level=logging.INFO)

# =========================
# Configuration models
# =========================

@dataclass
class Paths:
    ws_dir: str = "workspace"  # legacy default; overwritten in get_clone_genealogyain()
    repo_dir: str = field(default_factory=lambda: os.path.join("workspace", "repo"))  # overwritten in get_clone_genealogyain()
    data_dir: str = field(default_factory=lambda: os.path.join("workspace", "dataset"))  # overwritten in get_clone_genealogyain()
    prod_data_dir: str = field(default_factory=lambda: os.path.join("workspace", "dataset", "production"))  # overwritten in get_clone_genealogyain()
    hist_file: str = field(default_factory=lambda: os.path.join("workspace", "githistory.txt"))  # overwritten in get_clone_genealogyain()

@dataclass
class State:
    genealogy_data: List["Lineage"] = field(default_factory=list)

@dataclass
class Context:
    paths: Paths
    git_url: str
    state: State

def GetPattern(v1: CloneVersion, v2: CloneVersion):
    n_evo = 0
    evolution = "None"
    if len(v1.cloneclass.fragments) == len(v2.cloneclass.fragments):
        evolution = "Same"
    elif len(v1.cloneclass.fragments) > len(v2.cloneclass.fragments):
        evolution = "Subtract"
        n_evo = len(v1.cloneclass.fragments) - len(v2.cloneclass.fragments)
    else:
        evolution = "Add"
        n_evo = len(v2.cloneclass.fragments) - len(v1.cloneclass.fragments)

    def matches_count(a: Iterable[CloneFragment], b: Iterable[CloneFragment]):
        n = 0
        for f2 in b:
            for f1 in a:
                if f1.hash == f2.hash:
                    n += 1
                    break
        return n

    change = "None"
    n_change = 0
    nr_of_matches = matches_count(v1.cloneclass.fragments, v2.cloneclass.fragments)
    if evolution in ("Same", "Subtract"):
        if nr_of_matches == len(v2.cloneclass.fragments):
            change = "Same"
        elif nr_of_matches == 0:
            change = "Consistent"
            n_change = len(v2.cloneclass.fragments)
        else:
            change = "Inconsistent"
            n_change = len(v2.cloneclass.fragments) - nr_of_matches

    elif evolution == "Add":
        if nr_of_matches == len(v1.cloneclass.fragments):
            change = "Same"
        elif nr_of_matches == 0:
            change = "Consistent"
            n_change = len(v2.cloneclass.fragments)
        else:
            change = "Inconsistent"
            n_change = len(v2.cloneclass.fragments) - nr_of_matches

    v2_clones_loc = sum([frag.le - frag.ls for frag in v2.cloneclass.fragments])
    v1_clones_loc = sum([frag.le - frag.ls for frag in v1.cloneclass.fragments])
    clones_loc = v2_clones_loc - v1_clones_loc

    return (evolution, change, n_evo, n_change, clones_loc)

def PrepareSourceCode(ctx: "Context", language: str, hash_index) -> bool:
    paths = ctx.paths
    print("Preparing source code")
    found = False

    repo_root = os.path.abspath(paths.repo_dir)
    if not os.path.isdir(repo_root):
        printError(f"Repository directory not found: {repo_root}")
        logging.error(f"Project: {ctx.git_url} | Index: {hash_index} | Function: 'PrepareSourceCode' | Error: {e}")
        return False

    # Reset output dirs
    if os.path.exists(paths.data_dir):
        safe_rmtree(paths.data_dir)
    os.makedirs(paths.clone_detector_dir, exist_ok=True)
    os.makedirs(paths.data_dir, exist_ok=True)
    os.makedirs(paths.prod_data_dir, exist_ok=True)

    repo_path = Path(repo_root)

    # Pick only files that end with .java; skip .git and *test* files
    for src in repo_path.rglob("*"):
        if not src.is_file():
            continue

        if any(part == ".git" for part in src.parts):
            continue

        name_lower = src.name.lower()

        # Must end with .java (and not just contain ".java" in the middle)
        if not name_lower.endswith(language):
            continue

        # Skip test files
        if "test" in name_lower:
            continue

        rel_dir = os.path.relpath(str(src.parent), repo_root)
        dst_dir = paths.prod_data_dir if rel_dir == "." else os.path.join(paths.prod_data_dir, rel_dir)

        os.makedirs(dst_dir, exist_ok=True)
        try:
            shutil.copy2(str(src), os.path.join(dst_dir, src.name))
        except:
            logging.error(f"Project: {ctx.git_url} | Index: {hash_index} | Function: 'PrepareSourceCode' | Copy file: {str(src)} | Error: {e}")
        else:
            found = True

    print("Source code ready for clone analysis.\n")
    return found

# =========================
# Clone detection (cross‑platform)
# =========================

def RunCloneDetection(ctx: "Context", hash_index: str, language: str):
    try:
        paths = ctx.paths
        print("Starting clone detection:")

        # Normalize paths
        out_dir = Path(paths.clone_detector_dir)
        out_xml = Path(paths.clone_detector_xml)
        data_dir = Path(paths.data_dir)

        # Prepare output folder (clean files, keep folder)
        out_dir.mkdir(parents=True, exist_ok=True)
        for item in out_dir.iterdir():
            if item.is_file():
                item.unlink()
        out_xml.parent.mkdir(parents=True, exist_ok=True)

        out_dir = Path(paths.clone_detector_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        for item in out_dir.iterdir():
            if item.is_file():
                item.unlink()

        if language == "py":
            process_directory_py(paths.prod_data_dir)
        elif language == "cs":
            process_directory_cs(paths.prod_data_dir)
        elif language == "rb":
            process_directory_rb(paths.prod_data_dir)

        print(" >>> Running nicad6...")
        subprocess.run(["./nicad6", "functions", language, paths.prod_data_dir],
                    cwd="NiCad",
                    check=True)

        nicad_xml = f"{paths.prod_data_dir}_functions-clones/production_functions-clones-0.30-classes.xml"
        shutil.move(nicad_xml, paths.clone_detector_xml)
        clones_dir = Path(f"{paths.prod_data_dir}_functions-clones")
        shutil.rmtree(clones_dir, ignore_errors=True)

        data_dir = Path(ctx.paths.data_dir)
        for log_file in data_dir.glob("*.log"):
            try:
                log_file.unlink()
            except FileNotFoundError:
                pass
            except PermissionError:
                pass

        print("Finished clone detection.\n")
    except Exception as e:
        logging.error(f"Project: {ctx.git_url} | Index: {hash_index} | Function: 'RunCloneDetection' | Error: {e}")


def parseCloneClassFile(cloneclass_filename: str) -> List[CloneClass]:
    cloneclasses: List[CloneClass] = []
    try:
        file_xml = ET.parse(cloneclass_filename)
        root = file_xml.getroot()
        for child in root:
            cc = CloneClass()
            fragments = list(child)
            if not fragments:
                continue
            for fragment in fragments:
                file_path = fragment.get("file")
                startline = int(fragment.get("startline"))
                endline = int(fragment.get("endline"))
                cf = CloneFragment(file_path, startline, endline)
                cc.fragments.append(cf)
            cloneclasses.append(cc)
    except Exception as e:
        printError("Something went wrong while parsing the clonepair dataset:")
        raise e
    return cloneclasses

def RunGenealogyAnalysis(ctx: "Context", commitNr: int, hash_: str, number_pr: int, author_pr: str, hash_index: str):
    try:
        paths, st = ctx.paths, ctx.state
        print(f"Extract Code Code Genealogy (CCG) - Hash Commit {hash_}")
        pcloneclasses = parseCloneClassFile(paths.clone_detector_xml)

        if not st.genealogy_data:
            for pcc in pcloneclasses:
                v = CloneVersion(pcc, hash_, commitNr, number_pr, author_pr)
                l = Lineage()
                l.versions.append(v)
                st.genealogy_data.append(l)
        else:
            for pcc in pcloneclasses:
                found = False
                for lineage in st.genealogy_data:
                    if lineage.matches(pcc):

                        if lineage.versions[-1].nr == commitNr:
                            continue

                        evolution, change, n_evo, n_change, clones_loc = GetPattern(lineage.versions[-1], CloneVersion(pcc, hash_, commitNr, number_pr, author_pr))
                        lineage.versions.append(CloneVersion(pcc, hash_, commitNr, number_pr, author_pr, evolution, change, n_evo, n_change, clones_loc))
                        found = True
                        break
                if not found:
                    v = CloneVersion(pcc, hash_, commitNr, number_pr, author_pr)
                    l = Lineage()
                    l.versions.append(v)
                    st.genealogy_data.append(l)
    except Exception as e:
        logging.error(f"Project: {ctx.git_url} | Index: {hash_index} | Function: 'RunGenealogyAnalysis' | Error: {e}")


def build_no_clones_message(detector: Optional[str]) -> str:
    detector_name = (detector or "unspecified").strip() or "unspecified"

    root = ET.Element("result")
    ET.SubElement(root, "status").text = "no_clones_found"
    ET.SubElement(root, "detector").text = detector_name

    # Try modern pretty print (Python 3.9+)
    try:
        ET.indent(root, space="  ", level=0)  # type: ignore[attr-defined]
        return ET.tostring(root, encoding="utf-8", xml_declaration=True).decode("utf-8")
    except Exception:
        # Fallback: use minidom for pretty printing
        rough = ET.tostring(root, encoding="utf-8")
        reparsed = minidom.parseString(rough)
        return reparsed.toprettyxml(indent="  ", encoding="utf-8").decode("utf-8")

def WriteLineageFile(ctx: "Context", lineages: List[Lineage], filename: str):
    xml_txt = "<lineages>\n"
    path_intro = ctx.paths.ws_dir.split("cloned_repositories/")[0]

    with open(filename, "w+", encoding="utf-8") as output_file:
        output_file.write("<lineages>\n")
        for lineage in lineages:
            lineage_xml = lineage.toXML().replace(path_intro, "")
            output_file.write(lineage_xml)
            xml_txt += lineage_xml
        output_file.write("</lineages>\n")
        xml_txt += "</lineages>\n"

    return xml_txt

# =========================
# Settings initialization from user dictionary
# =========================

def _derive_repo_name(ctx: Context) -> str:
    """
    Determine a stable repository folder name from git_url or local_path.
    Falls back to 'repo' if nothing can be inferred.
    """
    url = (ctx.git_url or "").rstrip("/")
    base = os.path.basename(url) or "repo"
    if base.endswith(".git"):
        base = base[:-4]
    base = os.path.splitext(base)[0] or base
    return base or "repo"

@timed()
def get_clone_genealogy(full_name, merged_commits) -> str:
    # Sort merged_commits by pr_number
    merged_commits = sorted(merged_commits, key=lambda x: x.get("pr_number", 0))
    
    git_url = full_name
    paths = Paths()
    state = State()
    ctx = Context(git_url=git_url, paths=paths, state=state)

    # --- NEW: make all folders live inside the installed package directory ---
    pkg_root = Path(__file__).resolve().parent
    pkg_root_str = str(pkg_root)

    repo_name = _derive_repo_name(ctx)
    base_dir = os.path.join(pkg_root_str, "cloned_repositories", repo_name)
    paths.ws_dir = base_dir
    paths.repo_dir = os.path.join(base_dir, "repo")
    paths.data_dir = os.path.join(base_dir, "dataset")
    paths.prod_data_dir = os.path.join(paths.data_dir, "production")
    paths.hist_file = os.path.join(base_dir, "githistory.txt")
    paths.genealogy_xml = os.path.join(base_dir, "genealogy.xml")

    # Results & detector output
    paths.clone_detector_dir = os.path.join(base_dir, "aggregated_results")
    paths.clone_detector_xml = os.path.join(paths.clone_detector_dir, "result.xml")

    # Ensure folders exist
    os.makedirs(paths.clone_detector_dir, exist_ok=True)
    os.makedirs(base_dir, exist_ok=True)

    print("STARTING DATA COLLECTION SCRIPT\n")
    SetupRepo(ctx)
    total_time = 0
    hash_index = 0
    total_commits = len(merged_commits)
    clone_density_rows: List[dict] = []

    for commit_context in merged_commits:
        language = commit_context["language"]
        author_pr = commit_context["pr_type"]
        commit_pr = commit_context["sha"]
        number_pr = commit_context["pr_number"]

        iteration_start_time = time.time()
        hash_index += 1

        printInfo(
            f"Analyzing commit nr.{hash_index} (PR #{number_pr}) with hash {commit_pr} | "
            f"total commits: {total_commits} | author: {author_pr}"
        )

        # Ensure we are at the correct commit
        GitFecth(commit_pr, ctx, hash_index, logging)
        GitCheckout(commit_pr, ctx, hash_index, logging)

        # Prepare source code
        if not PrepareSourceCode(ctx, language, hash_index):
            logging.error(f"Don't have files '{language}' type in {full_name} (PR #{number_pr})")
            continue

        RunCloneDetection(ctx, hash_index, language)
        RunGenealogyAnalysis(ctx, hash_index, commit_pr, number_pr, author_pr, hash_index)
        WriteLineageFile(ctx, ctx.state.genealogy_data, paths.genealogy_xml)

        clone_density_by_repo = compute_clone_density(ctx, language, repo_name, git_url, number_pr, commit_pr, author_pr)
        clone_density_rows.append(clone_density_by_repo)

        # Timing
        iteration_end_time = time.time()
        iteration_time = iteration_end_time - iteration_start_time
        total_time += iteration_time

        print("Iteration finished in " + timeToString(int(iteration_time)))
        avg = int(total_time / hash_index) if hash_index else 0
        remaining = int((total_time / hash_index) * (len(merged_commits) - hash_index)) if hash_index else 0
        print(" >>> Average iteration time: " + timeToString(avg))
        print(" >>> Estimated remaining time: " + timeToString(remaining))

    repo_complete_name = full_name.split(".com/")[-1].replace("/","_")

    if len(ctx.state.genealogy_data) == 0:
        logging.error(f"Don't have code clones {full_name}")
        return build_no_clones_message("nicad"), None, None

    WriteCloneDensity(clone_density_rows,
                      language,
                      repo_complete_name)

    WriteLineageFile(ctx,
                    ctx.state.genealogy_data,
                    f"{genealogy_results_path}/{language}_{repo_complete_name}.xml")

    print("\nDONE")
