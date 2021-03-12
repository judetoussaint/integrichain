import pandas as pd
import numpy as np
import xlsxwriter
from csv import reader
import logging


logging.basicConfig(filename='scale.log', level=logging.INFO)


def add_flag(first_col, second_col):
    """This function creates a flag in a DataFrame"""
    if first_col and second_col != None:
        return 1
    return 0

def bmi_calc(col1, col2):
    "This function calculates the BMI inndex for players"
    if col2 ==0:
        return 0
    return ((col1/col2**2 )) * 703


def deduplicate(file_path):
    """This function deduplicate a dataframe and keep records with the most data"""

    try:
        logging.info("reading first file and turn to dataframe")
        df = pd.read_csv(file, header=0, names=['Name','Team','Position','Height','Weight','Age'])
        
        # replace field that's entirely space (or empty) with NaN
        df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
        
        #Turning Nan into None
        df = df.where((pd.notnull(df)), None)
        
        #We add the flag in the dataframe
        df['flag'] = df.apply(lambda row: add_flag(row['Height'],row['Weight']),axis=1)
        
        #Sorting the dataframe
        df.sort_values(by=['flag'], inplace=True, ascending=False)
        
        #Deduplicate the Dataframe
        df.drop_duplicates(subset=['Name','Team','Position'], keep='first', inplace=True)
        df.fillna(value=np.nan, inplace=True) #Put nan value back
        df['Weight'] = pd.to_numeric(df['Weight'])
        df['Age'] = pd.to_numeric(df['Age'])
        df['Height'] = pd.to_numeric(df['Height'])
        
        return df[['Name','Team','Position','Height','Weight','Age']]

    except Exception as e:
        logging.error(e)


def empty_to_average(df, col1, col2): #Height & Weight
    """This function fills the missing values with the average"""
    # df[col1].fillna(value=df[col1].mean(), inplace=True)
    # df[col2].fillna(value=df[col2].mean(), inplace=True)
    df[col1] = df[col1].fillna(value=df[col1].mean())
    df[col2] = df[col2].fillna(value=df[col2].mean())
    return df


def players_by_position(df,col):#Position
    group_df = df.groupby(col).size().reset_index()
    group_df.columns = [col, 'Count']
    group_df.to_excel('players_by_positions.xlsx',index=False)


def second_output(file, df):#teams.csv, average_df
    """This function generates the final output"""
    try:
        logging.info("reading second file")
        teams_df = pd.read_csv(file, header=0, names=['Team','Payroll','Wins'])
        df['bmi'] = df.apply(lambda row: bmi_calc(row['Weight'],row['Height']),axis=1)
        df.merge(teams_df, on='Team').to_excel('final_output.xlsx', index=False)

    except Exception as e:
        logging.error(e)
    

if __name__ =="__main__":
    file = 'players.csv'
    dedup_df = deduplicate(file)
    
    average_df = empty_to_average(dedup_df,'Height','Weight')
    
    players_by_position(average_df,'Position')

    second_output('teams.csv',average_df)



    

