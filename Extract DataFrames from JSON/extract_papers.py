# -*- coding: utf-8 -*-
"""
Created on Fri Sep  1 18:35:42 2023

@author: USER
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
    data for a particular paper. The function extracts title and paper_id of the paper 
    and returns the information as a dataframe.
    Parameters
    ----------
    file : a string containing the name of a json file.

    Returns
    -------
    df_temp : a single row dataframe containing paper title and id.

    """
    #source = 'D:\\Datasets\\Cord-19\\document_parses\\pdf_json'
    source = 'D:\\Datasets\\Cord-19\\document_parses\\pmc_json'
    
    with open(os.path.join(source, file)) as jf:
        data = json.load(jf)
        
    metadata = data['metadata']
    
    paper_id = data['paper_id']
    title = metadata['title']
    
    temp_data_dict = {
        'paper_id' : paper_id,
        'title'    : title
    }
    
    df_temp = pd.DataFrame([temp_data_dict])
    
    return df_temp

def main():
    papers_df = pd.DataFrame()
    
    print('Fetching all paper data')
    print('-----------------------------------------------------------------')
    
    with concurrent.futures.ProcessPoolExecutor(6) as executor:
        results = list(tqdm(executor.map(process_file, files), total=len(files)))
        
    # list of dataframes are concatenated to create full dataframe
    papers_df = pd.concat(results, ignore_index=True)
    
    papers_df.to_csv('D:\\Datasets\\Cord-19\\DataFrames\\papers_pmc_df.csv', index=False)
    
    print('-----------------------------------------------------------------')
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
    print(f"\nElapsed time: {elapsed_time / 60:.2f} minutes")