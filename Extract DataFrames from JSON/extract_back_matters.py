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
    data for a particular paper. The function extracts all relevant information related to the 
    back_matters of the paper and returns the information as a dataframe.

    Parameters
    ----------
    file : a string containing the name of a json file.

    Returns
    -------
    df_temp : a dataframe the back_matters data of the paper.

    """
    #source = 'D:\\Datasets\\Cord-19\\document_parses\\pdf_json'
    source = 'D:\\Datasets\\Cord-19\\document_parses\\pmc_json'
    
    with open(os.path.join(source, file)) as jf:
        data = json.load(jf)
        
    paper_id = data['paper_id']
    df_temp = pd.DataFrame.from_dict(data['back_matter'])
    df_temp['paper_id'] = paper_id
    
    return df_temp

def main():
    back_matter_df = pd.DataFrame()
    
    print('Fetching all back matter data')
    print('-----------------------------------------------------------------')
    
    with concurrent.futures.ProcessPoolExecutor(9) as executor:
        results = list(tqdm(executor.map(process_file, files), total=len(files)))
        
    back_matter_df = pd.concat(results, ignore_index=True)
    
    back_matter_df.to_csv('D:\\Datasets\\Cord-19\\DataFrames\\back_matters_df_pmc.csv', index=False)
    
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