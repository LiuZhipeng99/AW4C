import os, json
import pandas as pd
import numpy as np

from pydriller import Repository
from pydriller.domain.commit import ModificationType
import subprocess
import concurrent.futures
from typing import List, Tuple, Optional, Dict, Union

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
logging.getLogger('pydriller').setLevel(logging.WARNING)

def extract_code_from_warningLine(source_code: str, warningLineNumber: int) -> str:
    # The warning only gave a line number. 1) simply use the line number to extract the code, 2) extract codes from ast where line numbers are located, 3) others. like path 
    snippet = []
    lines = source_code.split('\n')
    line_numbers = {warningLineNumber}
    # line_numbers = {warningLineNumber - 1, warningLineNumber, warningLineNumber + 1}
    for i, line in enumerate(lines, start=1):
        if i in line_numbers:
            snippet.append(line)
    return '\n'.join(snippet)
    
def extract_code_from_patches(source_code: str, warningLineNumber: int, patches: Dict[str, List[Tuple[int, str]]]) -> str:
    def extract_closest_relative_continuous_subsequence(line_numbers, target) -> List:
        """ 
        Extracts the closest subsequence of relatively continuous line numbers from the list,  even if the target number is not in the list. 
        'Relatively continuous' means the difference between any two consecutive numbers in the subsequence is at most 2.
        """
        # Find the closest index to the target number
        if not line_numbers: 
            return []
        closest_index = min(range(len(line_numbers)), key=lambda i: abs(line_numbers[i] - target))
        # Extract the subsequence
        subsequence = [line_numbers[closest_index]]
        i = closest_index - 1
        # Check backwards
        while i >= 0 and line_numbers[i] >= line_numbers[i + 1] - 2:
            subsequence.insert(0, line_numbers[i])
            i -= 1
        # Check forwards
        i = closest_index + 1
        while i < len(line_numbers) and line_numbers[i] <= line_numbers[i - 1] + 2:
            subsequence.append(line_numbers[i])
            i += 1
        return subsequence
    
    lines_list = extract_closest_relative_continuous_subsequence([t[0] for t in patches['added'] + patches['deleted']], warningLineNumber)
    line_numbers = {warningLineNumber - 1, warningLineNumber, warningLineNumber + 1} | set(lines_list)
    lines = source_code.split('\n')
    snippet = []
    for i, line in enumerate(lines, start=1):
        if i in line_numbers:
            snippet.append(line)
    return '\n'.join(snippet)

def get_difftext_warningContext_fromLocal(repository_url, commit_id, warning_fileName, line_number, isIntroduced) ->Tuple[str, str]:
    for commit in Repository(repository_url,clone_repo_to="./tmp_github/").traverse_commits():
        if commit.hash == commit_id:
            for modified_file in commit.modified_files:  # Doc https://github.com/ishepard/pydriller/blob/master/pydriller/domain/commit.py
                if modified_file.new_path is not None and modified_file.new_path != "/dev/null":
                    diff_file_path = modified_file.new_path
                else:
                    assert modified_file.old_path
                    diff_file_path = modified_file.old_path
                # diff_file_path similar to modified_file.filename
                if diff_file_path.replace('\\', '/') == warning_fileName: 
                    patches = (modified_file.diff_parsed) 
                    patches['change_type'] = modified_file.change_type.name
                    if isIntroduced: 
                        if modified_file.change_type in [ModificationType.MODIFY, ModificationType.ADD]:  
                            warningContextCode = extract_code_from_warningLine(modified_file.source_code, line_number) # The context of the introduced warning is not obtained from the difftext
                            if modified_file.change_type == ModificationType.ADD:
                                patches['added'] = [] # Reduced storage, source code available from Github
                            return patches, warningContextCode 
                    else:
                        if modified_file.change_type == ModificationType.MODIFY:  
                            warningContextCode = extract_code_from_patches(modified_file.source_code_before, line_number, patches) 
                            return patches, warningContextCode
            return "WarningFileNotModified", "WarningFileNotModified" 
    return "UnkonwnCommit","UnkonwnCommit" 


def read_json_files(file_path,isIntroduced): 
    def extract_repo_and_commit_id(githubCommitLink):
        parts = githubCommitLink.split('/') # example: https://github.com/ishepard/pydriller/commit/fecaac2607
        repo_url = '/'.join(parts[:5])  
        commit_id = parts[-1]  
        repository_name = parts[3]+"/"+parts[4]
        return repo_url, commit_id, repository_name
    with open(file_path, 'r', encoding='utf-8') as file:
        logging.info(f'Reading JSON file: {file_path}')
        json_list = json.load(file)
        update_json_list = []
        for record in json_list:
            # record['actionableLabel'] =  not isIntroduced # 两个文件分开或新增一个字段.
            
            if not record['githubCommitLink'].startswith(('http://', 'https://')):  # "githubCommitLink does not start with 'http' or 'https'"
                logging.error(f"Error decoding record: {record['githubCommitLink']}")
                continue
            if not record['filePath'].startswith('tmp_github/') or  "Cppcheck failed to extract a valid configuration. Use -v for more details." in record['warningMessage'] or "Please note:" in record['warningMessage']:  #同时linenumber为0 type为noValidConfiguration
                logging.debug("Filter1: noValidConfiguration") 
                continue # Filter1 
            
            repo_url, commit_id, repository_name = extract_repo_and_commit_id(record['githubCommitLink'])
            record['repositoryName'] = repository_name
            warning_fileName = '/'.join(record['filePath'].split('/')[2:])
            warning_lineNumber = int(record['lineNumber'])
            outs = get_difftext_warningContext_fromLocal(repo_url, commit_id, warning_fileName ,warning_lineNumber , isIntroduced) 
            if outs[0] == "WarningFileNotModified": 
                logging.debug("Filter2: " + warning_fileName +  "not in" + record['githubCommitLink'])  
                continue  # Filter2: Filter those cases where the source file has not been changed but caused the warning to disappear. https://github.com/danielwaterworth/Raphters/commit/1803ff1947945e3aee261820f7e99bbab9eda92f
            if outs[0] == "UnkonwnCommit": 
                logging.error("Update your local branch : " + record['githubCommitLink']) # the local repository is not up-to-date
                continue
            record['difftext'], record['warningContext'] = outs
            update_json_list.append(record)
        
        logging.info(f'Finished reading JSON file: {file_path}')
        return (update_json_list)



def read_json_files_parallel(folder_path):
    json_data = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor: 
        futures = []
        for filename in os.listdir(folder_path):
            if filename.endswith('.json'):
                file_path = os.path.join(folder_path, filename)
                if "NonActionableWarning" in folder_path: 
                    futures.append(executor.submit(read_json_files, file_path, True ))
                else:
                    futures.append(executor.submit(read_json_files, file_path, False ))

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result is not None:
                json_data.extend(result)
            else:
                logging.error("Received None from a future, skipping...")

    return json_data



if __name__ == '__main__':
    output_gzip_aw = './compressed_ActionableWarning.json.gz'
    output_gzip_naw = './compressed_NonActionableWarning.json.gz'
    Generated_acfolder = './GeneratedDataset/ActionableWarning'
    Generated_nacfolder = './GeneratedDataset/NonActionableWarning'

    def get_dataframe(folder_path, output_file): # -> DataFrame
        if os.path.exists(output_file):
            return pd.read_json(output_file, compression='gzip')
        else:
            # Convert JSON data to dataframes
            df = pd.DataFrame(read_json_files_parallel(folder_path))
            df.to_json(output_file, compression='gzip')
            return df
    
    # with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    #     future_aw = executor.submit(get_dataframe, Generated_acfolder, output_gzip_aw)
    #     future_naw = executor.submit(get_dataframe, Generated_nacfolder, output_gzip_naw)
    #     df_aw = future_aw.result()
    #     df_naw = future_naw.result()

    df_aw = get_dataframe(Generated_acfolder, output_gzip_aw)
    df_naw = get_dataframe(Generated_nacfolder, output_gzip_naw)

    print("AW & NAW")
    print( len(df_aw), len(df_naw)) 
    # Display the dataframes
    print("Actionable Warning DataFrame:")
    print(df_aw)
    print("Non-Actionable Warning DataFrame:")
    print(df_naw)

    # Analyze the difference between the two files
    print(df_aw['warningContext'].str.len().mean())
    print(df_naw['warningContext'].str.len().mean())
