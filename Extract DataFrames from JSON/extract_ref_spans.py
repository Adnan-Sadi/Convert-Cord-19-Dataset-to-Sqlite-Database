# -*- coding: utf-8 -*-
"""
@author: adnan
"""

import pandas as pd
import os
from tqdm import tqdm
import time
from natsort import natsorted
import concurrent.futures
import gc

def process_row(row):
    """
    This function takes a row from a body text dataframe, which contains a chunk of text from 
    a paper along with the paper_id, text_id, cite_spans and ref_spans. This function creates a
    separate dataframe by extracting the ref_spans information.
    
    Parameters
    ----------
    row : a row from a dataframe

    Returns
    -------
    df_temp : a dataframe containing ref_spans information

    """
    df_temp = pd.DataFrame.from_dict(list(eval(row['ref_spans'])))
    df_temp['text_id'] = row['text_id']
    
    return df_temp

def main():
    path = 'D:\\Datasets\\Cord-19\\DataFrames\\body_texts'
    btext_files = os.listdir(path)
    print('Number of Body Text DataFrames: ',len(btext_files))
    
    sorted_btext_files = natsorted(btext_files)
    print('File Names: ', sorted_btext_files)
    
    # Columns to drop from body text dataframes
    drop_cols = ['text','cite_spans', 'section', 'paper_id']
    
    print('Fetching ref span data')
    print('-----------------------------------------------------------------')
    
    
    for count, file in tqdm(enumerate(sorted_btext_files)):
        temp_ref_spans = pd.read_csv(os.path.join(path, file))
        temp_ref_spans = temp_ref_spans.drop(drop_cols, axis=1)
        
        # a boolean mask to identify rows with empty ref_spans
        empty_cs_mask = temp_ref_spans['ref_spans'].map(lambda x: len(x) == 2)
    
        #select only rows that contains non-empty ref_spans
        json_ref_spans = temp_ref_spans[~empty_cs_mask]
        
        
        del temp_ref_spans
        del empty_cs_mask 
    
        print("Number of Rows of Json ref Spans: ", len(json_ref_spans))
        ref_spans_df = pd.DataFrame()
        row_list = []
    
        print('Extracting ref span data from json')
        print('-----------------------------------------------------------------')
    
        #getting all rows
        for index, row in json_ref_spans.iterrows():
            row_list.append(row)
    
        del json_ref_spans
        gc.collect()
    
        print("\nRow List Length: ", len(row_list))    
        
        with concurrent.futures.ProcessPoolExecutor(9) as executor:
            results = list(executor.map(process_row, row_list))
        
        # list of dataframes are concatenated to create full dataframe
        ref_spans_df = pd.concat(results, ignore_index=True)
        
        ref_spans_df.to_csv(f'btext_ref_spans_df_{count}.csv', index=False)
        
        print(f'\nFile-{count} Complete!!!')
    
    print('-----------------------------------------------------------------')
    print('Processing complete!')
    
if __name__ == "__main__":  
    start_time = time.time()
    
    main()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time / 60:.2f} minutes")