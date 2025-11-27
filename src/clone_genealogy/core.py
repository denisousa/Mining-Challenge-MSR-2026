import os
import time
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from xml.dom import minidom
import xml.etree.ElementTree as ET
from typing import List, Iterable, Optional
from git import Repo
from clone_genealogy.CloneFragment import CloneFragment
from clone_genealogy.CloneClass import CloneClass
from clone_genealogy.CloneVersion import CloneVersion
from clone_genealogy.Lineage import Lineage
from clone_genealogy.utils import safe_rmtree
from clone_genealogy.git_operations import SetupRepo, GitCheckout, GitFecth
from clone_genealogy.prints_operations import printError, printInfo
from clone_genealogy.compute_time import timed, timeToString
from utils.folders_paths import rq2_path

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

# =========================
# xxxxxxxxx
# =========================

def GetPattern(v1: CloneVersion, v2: CloneVersion):
    evolution = "None"
    if len(v1.cloneclass.fragments) == len(v2.cloneclass.fragments):
        evolution = "Same"
    elif len(v1.cloneclass.fragments) > len(v2.cloneclass.fragments):
        evolution = "Subtract"
    else:
        evolution = "Add"

    def matches_count(a: Iterable[CloneFragment], b: Iterable[CloneFragment]):
        n = 0
        for f2 in b:
            for f1 in a:
                if f1.hash == f2.hash:
                    n += 1
                    break
        return n

    change = "None"
    if evolution in ("Same", "Subtract"):
        nr_of_matches = matches_count(v1.cloneclass.fragments, v2.cloneclass.fragments)
        if nr_of_matches == len(v2.cloneclass.fragments):
            change = "Same"
        elif nr_of_matches == 0:
            change = "Consistent"
        else:
            change = "Inconsistent"
    elif evolution == "Add":
        nr_of_matches = matches_count(v2.cloneclass.fragments, v1.cloneclass.fragments)
        if nr_of_matches == len(v1.cloneclass.fragments):
            change = "Same"
        elif nr_of_matches == 0:
            change = "Consistent"
        else:
            change = "Inconsistent"

    return (evolution, change)

def PrepareSourceCode(ctx: "Context", language: str) -> bool:
    paths = ctx.paths
    print("Preparing source code")
    found = False

    repo_root = os.path.abspath(paths.repo_dir)
    if not os.path.isdir(repo_root):
        printError(f"Repository directory not found: {repo_root}")
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
            # Ignore copy errors
            pass
        else:
            found = True

    print("Source code ready for clone analysis.\n")
    return found

# =========================
# Clone detection (cross‑platform)
# =========================

def RunCloneDetection(ctx: "Context", language: str):
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

def CheckDoubleMatch(cc_original: CloneClass, cc1: CloneClass, cc2: CloneClass) -> int:
    cc1_strict_match = False
    cc2_strict_match = False
    for fragment in cc_original.fragments:
        for f1 in cc1.fragments:
            if fragment.matchesStrictly(f1):
                cc1_strict_match = True
        for f2 in cc2.fragments:
            if fragment.matchesStrictly(f2):
                cc2_strict_match = True
    if cc1_strict_match == cc2_strict_match:
        return 0
    if cc1_strict_match:
        return 1
    elif cc2_strict_match:
        return 2
    return 0

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

def RunGenealogyAnalysis(ctx: "Context", commitNr: int, hash_: str, author_pr: str):
    paths, st = ctx.paths, ctx.state
    print(f"Extract Code Code Genealogy (CCG) - Hash Commit {hash_}")
    pcloneclasses = parseCloneClassFile(paths.clone_detector_xml)

    if not st.genealogy_data:
        for pcc in pcloneclasses:
            v = CloneVersion(pcc, hash_, commitNr, author_pr)
            l = Lineage()
            l.versions.append(v)
            st.genealogy_data.append(l)
    else:
        for pcc in pcloneclasses:
            found = False
            for lineage in st.genealogy_data:
                if lineage.matches(pcc):
                    if lineage.versions[-1].nr == commitNr:
                        
                        if len(lineage.versions) <= 1:
                            continue

                        checkDoubleMatch = CheckDoubleMatch(
                            lineage.versions[-2].cloneclass,
                            lineage.versions[-1].cloneclass,
                            pcc,
                        )
                        if checkDoubleMatch == 1:
                            continue
                        elif checkDoubleMatch == 2:
                            pcloneclasses.append(lineage.versions[-1].cloneclass)

                    evolution, change = GetPattern(lineage.versions[-1], CloneVersion(pcc))
                    if (
                        evolution == "Same"
                        and change == "Same"
                        and lineage.versions[-1].evolution_pattern == "Same"
                        and lineage.versions[-1].change_pattern == "Same"
                    ):
                        lineage.versions[-1].nr = commitNr
                        lineage.versions[-1].hash = hash_
                    else:
                        lineage.versions.append(CloneVersion(pcc, hash_, commitNr, author_pr, evolution, change))
                    found = True
                    break
            if not found:
                v = CloneVersion(pcc, hash_, commitNr, author_pr)
                l = Lineage()
                l.versions.append(v)
                st.genealogy_data.append(l)

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

    with open(filename, "w+", encoding="utf-8") as output_file:
        output_file.write("<lineages>\n")
        for lineage in lineages:
            output_file.write(lineage.toXML())
            xml_txt += lineage.toXML()
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
def get_clone_genealogy(full_name, methodology_commits) -> str:
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
    for methodology_commits_item in methodology_commits:
        total_commits = len(methodology_commits)*2
        language = methodology_commits_item["language"]
        set_commits = methodology_commits_item["commits"]
        for author_pr, commit_pr in set_commits.items(): 
            if author_pr == "coding_agent":
                author_pr = methodology_commits_item["agent"]

            iteration_start_time = time.time()
            hash_index += 1

            printInfo(f"Analyzing commit nr.{hash_index} with hash {hash_index} | total commits: {total_commits} | author: {author_pr}")

            # Ensure we are at the correct commit
            GitFecth(commit_pr, ctx)
            GitCheckout(commit_pr, ctx)

            # Prepare source code
            if not PrepareSourceCode(ctx, language):
                continue

            RunCloneDetection(ctx, language)
            RunGenealogyAnalysis(ctx, hash_index, commit_pr, author_pr)
            WriteLineageFile(ctx, ctx.state.genealogy_data, paths.genealogy_xml)

            # Timing
            iteration_end_time = time.time()
            iteration_time = iteration_end_time - iteration_start_time
            total_time += iteration_time

            print("Iteration finished in " + timeToString(int(iteration_time)))
            avg = int(total_time / hash_index) if hash_index else 0
            remaining = int((total_time / hash_index) * (len(methodology_commits) - hash_index)) if hash_index else 0
            print(" >>> Average iteration time: " + timeToString(avg))
            print(" >>> Estimated remaining time: " + timeToString(remaining))

            WriteLineageFile(ctx, ctx.state.genealogy_data, paths.genealogy_xml)

        # If nothing was accumulated, return a clear XML message
        if len(ctx.state.genealogy_data) == 0:
            return build_no_clones_message("nicad"), None, None

        # Otherwise, finalize outputs
        repo_complete_name = full_name.split(".com/")[-1].replace("/","_")
        lineages_xml = WriteLineageFile(ctx,
                                        ctx.state.genealogy_data,
                                        f"{rq2_path}/{repo_complete_name}.xml")

        print("\nDONE")
