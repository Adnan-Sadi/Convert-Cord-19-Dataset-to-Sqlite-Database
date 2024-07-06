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
    data for a particular paper. The function extracts all the body text data of the paper 
    and returns the information as a dataframe.

    Parameters
    ----------
    file : a string containing the name of a json file.

    Returns
    -------
    df_temp : a dataframe containing the body text data of the paper.

    """
    #source = 'D:\\Datasets\\Cord-19\\document_parses\\pdf_json'
    source = 'D:\\Datasets\\Cord-19\\document_parses\\pmc_json'
    
    
    with open(os.path.join(source, file)) as jf:
        data = json.load(jf)
        
    paper_id = data['paper_id']
    
    df_temp = pd.DataFrame.from_dict(data['body_text'])
    df_temp['paper_id'] = paper_id
        
    return df_temp
    
    
def main():
    body_texts_df = pd.DataFrame()
    
    print('Fetching all body text data')
    print('-----------------------------------------------------------------')
    
    # adjust chucksize based on the available system memory. 
    chunksize = 30000
    chunks = [files[i:i + chunksize] for i in range(0, len(files), chunksize)]
    
    """
    We first extracted the files in the 'pdf_json' using this script. Then ran the script
    again to extract 'pmc_json' files. To maintain continuity of the 'text_id' column we 
    set the start variable to 13868684 (this was the number of rows extracted from the 'pdf_json'
    files). Set the start variable to 0 if you have all your files in a single folder or if are
    running the script for the first time.
    """
    start = 13868684
    
    for count, chunk in enumerate(chunks):
        
        print(f'Processing Chunk {count}\n')
        with concurrent.futures.ProcessPoolExecutor(9) as executor:
            results = list(tqdm(executor.map(process_file, chunk), total=len(chunk)))
            
            # list of dataframes are concatenated to create full dataframe
            body_texts_df = pd.concat(results, ignore_index=True)
            body_texts_df['text_id'] = body_texts_df.index.map(lambda idx: f'text_{idx +start+ 1}')
    
            body_texts_df.to_csv(f'D:\\Datasets\\Cord-19\\DataFrames\\body_texts\\body_text_df_pmc_{count}.csv',
                                 index=False)
        start = start + len(body_texts_df)
    
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