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
    the abstract of the paper and returns the information as a dataframe.
    Parameters
    ----------
    file : a string containing the name of a json file.

    Returns
    -------
    temp_df : a single row dataframe containing abstract data of the paper.

    """
    # switch source folder when needed
    #source = 'D:\\Datasets\\Cord-19\\document_parses\\pdf_json'
    source = 'D:\\Datasets\\Cord-19\\document_parses\\pmc_json'
    
    with open(os.path.join(source, file)) as jf:
        data = json.load(jf)
        
    paper_id = data['paper_id']    
    
    #check if abstract key exists and abstract is not empty
    if 'abstract' in data and data['abstract']:       
        
        temp_df = pd.DataFrame.from_dict(data['abstract'])
        temp_df['paper_id'] = paper_id

       
    else:
        """
        files in 'pmc_json' folder does not contain abstracts. As a result a placeholder
        dataframe is created which we will fill later by taking abstracts from the 
        'metadata.csv' file. The code for this is written in 'extract_abstracts_2.py' file.
        """
        temp_data_dict = {
            'text' : [],
            'cite_spans': [],
            'ref_spans' : [],
            'section': 'Abstract',
            'paper_id': paper_id
        }
        
        temp_df = pd.DataFrame([temp_data_dict])
        
    return temp_df

def main():
    abstracts_df = pd.DataFrame()
    
    print('Fetching all abstract data')
    print('-----------------------------------------------------------------')
    
    with concurrent.futures.ProcessPoolExecutor(6) as executor:
        results = list(tqdm(executor.map(process_file, files), total=len(files)))

    # list of dataframes are concatenated to create full dataframe
    abstracts_df = pd.concat(results, ignore_index=True)
    
    abstracts_df.to_csv('D:\\Datasets\\Cord-19\\DataFrames\\abstracts_pmc_df.csv', index=False)
    
    print('-----------------------------------------------------------------')
    print('Processing complete!')
    
    
if __name__ == "__main__":  
    start_time = time.time()
    # switch source folder when needed
    #source = 'D:\\Datasets\\Cord-19\\document_parses\\pdf_json'
    source = 'D:\\Datasets\\Cord-19\\document_parses\\pmc_json'
    
    files = os.listdir(source)
    print('total files: ' , len(files),'\n')
    
    main()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\nElapsed time: {elapsed_time / 60:.2f} minutes")
    