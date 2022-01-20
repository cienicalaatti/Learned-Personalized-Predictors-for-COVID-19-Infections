import pandas as pd
import missingno as msno
import matplotlib.pyplot as plt
import glob
'''Module to preprocess the raw dataset'''
def main():
    path = 'data_17_08'
    all_files = glob.glob(path + "/*.parquet")

    li = []

    for filename in all_files:
        df = pd.read_parquet(filename)
        li.append(df)

    df = pd.concat(li, axis=0, ignore_index=True)
    #df = pd.read_parquet("data.parquet")

    #visualize the missing data
    msno.matrix(df)

    #selecting subset of the data where symptom information exists:
    #print('Number of datapoints in the raw dataframe:')
    #print(len(df.index),'\n')

    columns = [  
                        'pna_yn',
                        'abxchest_yn',
                        'acuterespdistress_yn',
                        'chills_yn',
                        'myalgia_yn',
                        'runnose_yn',
                        'sthroat_yn',
                        'cough_yn',
                        'sob_yn',
                        'nauseavomit_yn',
                        'headache_yn',
                        'abdom_yn',
                        'diarrhea_yn',
                        'medcond_yn',
                        'age_group',
                        'hosp_yn'
    ]

    df = df[columns].copy()
    df.replace(['Missing','Unknown','NA','N/A'], pd.NA, inplace=True)
    df.dropna(subset=columns,inplace=True)
    #out of 15*10^6 datapoints only around 250000 have all of the above records
    # allowing 5 missing values yields c.a. 3 million datapoints, could try to work with that + data imputation
    #print('Number of datapoints after removing rows with no symptom information:')
    #print(len(df.index),'\n')

    df.replace({'No': 0, 'Yes': 1}, inplace=True)

    #map the age group from 1 to 8
    df['age_group'].replace({'0 - 9 Years': 1, '10 - 19 Years': 2, '20 - 29 Years': 3, '30 - 39 Years': 4, '40 - 49 Years': 5, '50 - 59 Years': 6, '60 - 69 Years': 7, '70 - 79 Years': 8, '80+ Years': 9}, inplace=True)
    
    #save as .parquet
    df.to_csv('preprocessed_data.csv')
    print('Preprocessing ready.')
if __name__ == '__main__':
    main()

