# -*- coding: utf-8 -*-
"""
@author: adnan
"""

import pandas as pd
import os
import json
from tqdm import tqdm
import time
import concurrent.futures

def process_file(file):
    """
    This function processes a json file from the Cord-19 dataset, which contains full-text
    data for a particular paper. The function extracts all relevant information related to 
    the authors (and their affiliations) of the paper and returns the information as a 
    dataframe.

    Parameters
    ----------
    file : a string containing the name of a json file.

    Returns
    -------
    authors_df_temp : a dataframe containing authors and affiliations data of the paper.

    """
    #source = 'D:\\Datasets\\Cord-19\\document_parses\\pdf_json'
    source = 'D:\\Datasets\\Cord-19\\document_parses\\pmc_json'
    
    with open(os.path.join(source, file)) as jf:
        temp_data = json.load(jf)
    
    metadata = temp_data['metadata']
    paper_id = temp_data['paper_id']
    
    # Getting all author data
    authors_df_temp = pd.json_normalize(metadata, record_path=['authors'])
    authors_df_temp['paper_id'] = paper_id
    
    return authors_df_temp
    
def main():
    authors_df = pd.DataFrame()
    
    print('Fetching all author data')
    print('-----------------------------------------------------------------')

    with concurrent.futures.ProcessPoolExecutor(6) as executor:
        results = list(tqdm(executor.map(process_file, files), total=len(files)))

    # list of dataframes are concatenated to create full dataframe
    authors_df = pd.concat(results, ignore_index=True)

    
    # for pdf parses
    # Set affiliation id to a default null value
    authors_df['affiliation_id'] = 'null'
    
    # Separate the affiliation columns and create a new df
    aff_cols = ['affiliation.laboratory', 'affiliation.institution', 'affiliation.location.settlement', 'affiliation.location.country']
    aff_df = authors_df[aff_cols]
    aff_df = aff_df.dropna(axis=0, how='all')
    aff_df = aff_df.drop_duplicates()
    aff_df = aff_df.reset_index(drop=True)
    aff_df['affiliation_id'] = aff_df.index.map(lambda idx: f'aff_{idx + 1}')
    
    
    authors_df.to_csv('authors_df_pmc_full.csv', index=False)
    """
    all the files in 'pmc_json' folder has empty author affiliations. Thus comment the following
    lines when precessing the 'pmc_json' files.
    """
    #aff_df.to_csv('affiliation_df_final.csv', index=False)
    #print('\naff_df rows: ', len(aff_df))
    
    print('\nauthors_df rows: ', len(authors_df))
    

    print('Processing complete!')

if __name__ == "__main__":  
    start_time = time.time()
    #source = 'D:\\Datasets\\Cord-19\\document_parses\\pdf_json'
    source = 'D:\\Datasets\\Cord-19\\document_parses\\pmc_json'
    
    files = os.listdir(source)
    print('total files: ' , len(files),'\n')
    
    main()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time / 60:.2f} minutes")
    