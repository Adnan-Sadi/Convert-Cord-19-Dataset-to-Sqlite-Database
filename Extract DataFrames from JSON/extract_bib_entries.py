# -*- coding: utf-8 -*-
"""
@author: adnan
"""
import pandas as pd
import os
from tqdm import tqdm
import json
import time
import concurrent.futures

def process_file(file):
    """
    This function processes a json file from the Cord-19 dataset, which contains full-text
    data for a particular paper. The function extracts all relevant information related to the
    bib_entries of the paper and returns the information as a dataframe.

    Parameters
    ----------
    file : a string containing the name of a json file.

    Returns
    -------
    df_temp : a dataframe the bib_entries data of the paper.

    """
    #source = 'D:\\Datasets\\Cord-19\\document_parses\\pdf_json'
    source = 'D:\\Datasets\\Cord-19\\document_parses\\pmc_json'
    
    with open(os.path.join(source, file)) as jf:
        data = json.load(jf)
        
    paper_id = data['paper_id']
    
    df_temp = pd.DataFrame.from_dict(data['bib_entries'], orient='index')
    df_temp['paper_id'] = paper_id
    
    return df_temp

def main():
    bib_entries_df = pd.DataFrame()
    
    print('Fetching all bib entry data')
    print('-----------------------------------------------------------------')
    
    # adjust chucksize based on the available system memory. 
    chunksize = 30000
    chunks = [files[i:i + chunksize] for i in range(0, len(files), chunksize)]
    
    for count, chunk in enumerate(chunks):
        
        print(f'Processing Chunk {count}\n')
        with concurrent.futures.ProcessPoolExecutor(9) as executor:
            results = list(tqdm(executor.map(process_file, chunk), total=len(chunk)))
        
        bib_entries_df = pd.concat(results)
    
        #reset and rename index to bibent_id
        bib_entries_df.reset_index(inplace=True)
        bib_entries_df = bib_entries_df.rename(columns = {'index':'bibent_id'})
    
        bib_entries_df.to_csv(f'D:\\Datasets\\Cord-19\\DataFrames\\bib_entries\\bib_entries_df_pmc_{count}.csv', index=False)
    
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
