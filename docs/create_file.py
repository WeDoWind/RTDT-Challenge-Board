import pandas as pd
import random
from os import listdir
from pathlib import Path
import subprocess
from io import StringIO
from typing import Dict, List, Tuple, Optional
from datetime import datetime

char_set = ["0","0","0","0", "1", "2", "3", "4", "5", "6", "7"]
ENABLE_GIT_PUSH = True
GITHUB_REPO = Path().cwd()
GIT = "git"
DEPLOY_DIR = Path("docs")

def create_random_overall_file(participantes_num = 10, data_subsets = 10):
    
    data = []
    
    for i in range(participantes_num):
        row = {"Participant_ID": i}
        row["Submission_ID"]= f"Submission_{i}_Sub_i"
        for j in range(data_subsets):
            row[f"{j}"] = random.choice(char_set)
        data.append(row)
    df = pd.DataFrame(data)
    df.to_csv("ranking.csv",index=False)
    print(df)

def create_random_individual_submissions(participantes_num = 10, sessions_num = 10):

    for p in range(participantes_num):
        data = {
            "session_id": [],
            "data_quality": []
        }
        for i in range(sessions_num):
            data["session_id"].append(i)
            data["data_quality"].append(random.choice(char_set))
        df = pd.DataFrame(data)
        df.to_csv(f"Submission/participant_{p}-result_1.csv", index=False)



def load_submissions_csv(rclone_remote="switchdrive", folder="RTDT-Corrupted/Submissions"):
    """
    Download all CSV files from a given rclone remote folder and return as a pandas DataFrame.
    
    Args:
        rclone_remote (str): Name of the rclone remote (e.g., "switchdrive_shared").
        folder (str): Folder path inside the remote.
    
    Returns:
        pd.DataFrame: Concatenated dataframe of all CSVs.
    """
    # List all CSV files in the folder
    try:
        result = subprocess.run(
            ["rclone", "lsf", f"{rclone_remote}:{folder}", "--include", "*.csv"],
            capture_output=True,
            text=True,
            check=True
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to list files: {e.stderr}")
    
    files = result.stdout.strip().splitlines()
    if not files:
        raise FileNotFoundError(f"No CSV files found in {folder}")
    
    # Download each CSV into a pandas DataFrame and concatenate
    dfs = []
    for file in files:
        try:
            csv_result = subprocess.run(
                ["rclone", "cat", f"{rclone_remote}:{folder}/{file}"],
                capture_output=True,
                text=True,
                check=True
            )
            df = pd.read_csv(StringIO(csv_result.stdout))
            df["Submission"] = file.replace(".csv", "")
            dfs.append(df)
        except subprocess.CalledProcessError as e:
            print(f"Warning: Failed to download {file}: {e.stderr}")
    
    if not dfs:
        raise RuntimeError("No CSVs could be downloaded successfully.")
    
    return pd.concat(dfs, ignore_index=True)


def get_all_df( path_to_dir, suffix=".csv" ):
    dfs = []
    filenames = listdir(path_to_dir)
    
    for f in [ filename for filename in filenames if filename.endswith( suffix ) ]:
        df = pd.read_csv(Path(path_to_dir)/Path(f))
        df["Submission"] = f.replace(".csv", "")
        dfs.append(df)
    return pd.concat(dfs)

def create_overall_df(df_combined: pd.DataFrame):
    return df_combined.pivot(index='Submission', columns='session_id', values='data_quality')

def run_command(cmd: List[str], check: bool = False) -> Tuple[int, str, str]:
    """Execute shell command and return output."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, cmd, result.stderr)
    return result.returncode, result.stdout, result.stderr

def push_to_github():
    """Push updated CSV to GitHub."""
    if not ENABLE_GIT_PUSH:
        print("Git push disabled", "INFO")
        return False
    
    try:
        run_command([str(GIT), "add", str(DEPLOY_DIR / "ranking.csv")], check=True)
        
        commit_msg = f"Update rankings - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        rc, out, _ = run_command([str(GIT), "commit", "-m", commit_msg])
        
        if "nothing to commit" in out:
            print("No changes to commit", "INFO")
            return True
        
        run_command([str(GIT), "push"], check=True)
        print("Pushed to GitHub successfully", "SUCCESS")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"Git error: {e}", "ERROR")
        return False
    except Exception as e:
        print(f"Push failed: {e}", "ERROR")
        return False


if __name__ == "__main__":
    df_combined = load_submissions_csv()
    combined = create_overall_df(df_combined)
    combined.to_csv("docs/ranking.csv")
    push_to_github()