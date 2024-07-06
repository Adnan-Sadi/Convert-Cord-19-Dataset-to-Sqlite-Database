# -*- coding: utf-8 -*-
"""
@author: adnan
"""

import pandas as pd

def main():
    # load metadata dataframe
    metadata_df = pd.read_csv('D:\\Datasets\\Cord-19\\metadata.csv')
    metadata_df = metadata_df.drop(['mag_id'], axis=1)
    
    # placeholder dataframe created using 'extract_abstracts.py'
    pmc_papers = pd.read_csv('D:\\Datasets\\Cord-19\\DataFrames\\abstracts\\abstracts_pmc_df.csv')
    pmc_papers = pmc_papers.drop(columns=['text', 'cite_spans', 'ref_spans'], axis=1)
    
    cols = ['abstract', 'pmcid']

    merged_df = pmc_papers.merge(metadata_df[cols], left_on='paper_id', right_on='pmcid', 
                                 how='inner')
    
    merged_df.drop(columns=['pmcid'], inplace=True)
    merged_df.rename(columns={"abstract": "text"}, inplace= True)
    
    merged_df.to_csv('abstracts_pmc_df_final.csv')
    
if __name__ == "__main__":
    main()