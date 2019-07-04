from multiprocessing import  Pool
from functools import partial
import numpy as np 
import pandas as pd

def parallelize_dataframe(df, func, n_pool=4, col_subsets=None, join_how='outer',kwargs_list=None):
    ''' 
    Function to take a df, a function with args, and a list of column subsets to apply function to. 
    Resulting list of df's are then joined back together based on the df index.
    '''
    # create empty list of df's
    df_list = []
    # if col subsets not defined then just use all cols
    if col_subsets == None:
        col_subsets = [[col for col in df.columns]]
    # loop over each subset of cols, make a subset df, and append it to df_list
    for col_subset in col_subsets:
        df_list.append(df[col_subset])
    # define pool params
    pool = Pool(n_pool)
    # apply func via map to each of the df's in df_list, zip up the kwargs for the function call as well
    map_return_list = pool.map(func, zip(df_list,kwargs_list))
    # join back all the resulting df's into one df based on joining back together based on index
    for i in range(len(map_return_list)):
        if i == 0:
            df_out = map_return_list[i]
        else:
            df_out = df_out.merge(map_return_list[i],join_how,left_index=True,right_index=True)
    # multiprocessing clean up
    pool.close()
    pool.join()
    return df_out

def do_work(zipped):
    ''' Function (which we want to parallelize across different cols of the df) 
    to do some operations on a df.
    '''
    df = zipped[0]
    kwarg_dict = zipped[1]
    # pass any args in as a dict to be parsed out as needed (not used in this example)
    if kwarg_dict:
        print(kwarg_dict)
    # for every col in the subseted df we want to do some operations specific to each subset df
    for col in df._get_numeric_data().columns:
        df[f'{col}_output'] = df[col]*kwarg_dict['multiply_by']
    return df