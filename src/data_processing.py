
 # Libraries
import traceback

# Data
# File Management
import os # Operating system library
import pathlib # file paths
import json
import requests
import math
import numpy as np
import pandas as pd # Dataframe manipulations

import sqlite3
import datetime
from datetime import datetime, timedelta

from collections import OrderedDict
# import local modules
from config_settings import *

# ----------------------------------------------------------------------------
# Get dataframes and parameters
# ----------------------------------------------------------------------------

def get_time_parameters(end_report, report_days_range = 7):
    today = datetime.now()
    start_report = end_report - timedelta(days=report_days_range)
    start_report_text = str(start_report.date()) #dt.strftime('%m/%d/%Y')
    end_report_text = str(end_report.date()) #dt.strftime('%m/%d/%Y')
    report_range_msg = 'This report generated on: ' + str(datetime.today().date()) + ' covering the previous ' + str(report_days_range) + ' days.'
    report_date_msg = 'This report generated on: ' + str(datetime.today().date())
    return today, start_report, end_report, report_date_msg, report_range_msg


# ----------------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------------

def use_b_if_not_a(a, b):
    if not pd.isnull(a):
        x = a
    else:
        x = b
    return x

def create_multiindex(df, split_char):
    cols = df.columns
    multi_cols = []
    for c in cols:
        multi_cols.append(tuple(c.split(split_char)))
    multi_index = pd.MultiIndex.from_tuples(multi_cols)
    df.columns = multi_index
    return df

def convert_to_multindex(df, delimiter = ': '):
    cols = list(df.columns)
    cols_with_delimiter = [c for c in cols if delimiter in c]
    df_mi = df[cols_with_delimiter].copy()
    df_mi.columns = [tuple(x) for x in df_mi.columns.str.split(delimiter)]
    df_mi.columns = pd.MultiIndex.from_tuples(df_mi.columns)
    return df_mi

def datatable_settings_multiindex(df, flatten_char = '_'):
    ''' Plotly dash datatables do not natively handle multiindex dataframes. This function takes a multiindex column set
    and generates a flattend column name list for the dataframe, while also structuring the table dictionary to represent the
    columns in their original multi-level format.

    Function returns the variables datatable_col_list, datatable_data for the columns and data parameters of
    the dash_table.DataTable'''
    datatable_col_list = []

    levels = df.columns.nlevels
    if levels == 1:
        for i in df.columns:
            datatable_col_list.append({"name": i, "id": i})
    else:
        columns_list = []
        for i in df.columns:
            col_id = flatten_char.join(i)
            datatable_col_list.append({"name": i, "id": col_id})
            columns_list.append(col_id)
        df.columns = columns_list

    dd = OrderedDict()
    datatable_data = df.to_dict('records', into=dd)
    #
    # datatable_data = df.to_dict('records')

    return datatable_col_list, datatable_data

# ----------------------------------------------------------------------------
# DATA DISPLAY DICTIONARIES & SCREENING DATA
# ----------------------------------------------------------------------------

def load_display_terms(ASSETS_PATH, display_terms_file):
    '''Load the data file that explains how to translate the data columns and controlled terms into the English language
    terms to be displayed to the user'''
    try:
        if ASSETS_PATH:
            display_terms = pd.read_csv(os.path.join(ASSETS_PATH, display_terms_file))
        else:
            display_terms = pd.read_csv(display_terms_file)

        # Get display terms dictionary for one-to-one records
        display_terms_uni = display_terms[display_terms.multi == 0]
        display_terms_dict = get_display_dictionary(display_terms_uni, 'api_field', 'api_value', 'display_text')

        # Get display terms dictionary for one-to-many records
        display_terms_multi = display_terms[display_terms.multi == 1]
        display_terms_dict_multi = get_display_dictionary(display_terms_multi, 'api_field', 'api_value', 'display_text')

        return display_terms, display_terms_dict, display_terms_dict_multi
    except Exception as e:
        traceback.print_exc()
        return None

def get_display_dictionary(display_terms, api_field, api_value, display_col):
    '''from a dataframe with the table display information, create a dictionary by field to match the database
    value to a value for use in the UI '''
    try:
        display_terms_list = display_terms[api_field].unique() # List of fields with matching display terms

        # Create a dictionary using the field as the key, and the dataframe to map database values to display text as the value
        display_terms_dict = {}
        for i in display_terms_list:
            term_df = display_terms[display_terms.api_field == i]
            term_df = term_df[[api_value,display_col]]
            term_df = term_df.rename(columns={api_value: i, display_col: i + '_display'})
            term_df = term_df.apply(pd.to_numeric, errors='ignore')
            display_terms_dict[i] = term_df
        return display_terms_dict

    except Exception as e:
        traceback.print_exc()
        return None

# ----------------------------------------------------------------------------
# DATA LOADING
# ----------------------------------------------------------------------------
def get_subjects_json(report, report_suffix,  file_url_root=None, mcc_list =[1,2], DATA_PATH = None):
    subjects_json = {}
    try:
        # Read files into json from API
        for mcc in mcc_list:
            json_url = '/'.join([file_url_root, report,report_suffix.replace('[mcc]',str(mcc))])
            r = requests.get(json_url)
            if r.status_code == 200:
            # TO DO: add an else statement to use local files if the request fails
                mcc_json = r.json()
                subjects_json[mcc] = mcc_json
        data_source = 'API'
        data_date = datetime.now().strftime('%m/%d/%Y')

        # for mcc in mcc_list:
        #     mcc_filename = ''.join(['subjects-',str(mcc),'-latest.json'])
        #     mcc_filepath = os.path.join(DATA_PATH, mcc_filename)
        #     with open(mcc_filepath, 'r') as f:
        #         subjects_json[mcc] = json.load(f)
        # data_source = 'local files'
        # data_date = '06/15/2022'
    except:
        #load local files if API failss
        for mcc in mcc_list:
            mcc_filename = ''.join(['subjects-',str(mcc),'-latest.json'])
            mcc_filepath = os.path.join(DATA_PATH, mcc_filename)
            with open(mcc_filepath, 'r') as f:
                subjects_json[mcc] = json.load(f)
        data_source = 'local files'
        data_date = '06/15/2022'

    return subjects_json, data_source, data_date


def combine_mcc_json(mcc_json):
    '''Convert MCC json subjects data into dataframe and combine'''
    df = pd.DataFrame()
    for mcc in mcc_json:
        mcc_data = pd.DataFrame.from_dict(mcc_json[mcc], orient='index').reset_index()
        mcc_data['mcc'] = mcc
        if df.empty:
            df = mcc_data
        else:
            df = pd.concat([df, mcc_data])

    return df

def add_screening_site(screening_sites, df, id_col):
    # Get dataframes
    ids = df.loc[:, [id_col]]

    # open sql connection to create new datarframe with record_id paired to screening site
    conn = sqlite3.connect(':memory:')
    ids.to_sql('ids', conn, index=False)
    screening_sites.to_sql('ss', conn, index=False)

    sql_qry = f'''
    select {id_col}, screening_site, site, surgery_type, record_id_start, record_id_end
    from ids
    join ss on
    ids.{id_col} between ss.record_id_start and ss.record_id_end
    '''
    sites = pd.read_sql_query(sql_qry, conn)
    conn.close()

    df = sites.merge(df, how='left', on=id_col)

    return df

# ----------------------------------------------------------------------------
# DATA CLEANING
# ----------------------------------------------------------------------------

def get_enrolled(subjects_json, screening_sites, display_terms_dict, display_terms_dict_multi):
    '''Take the raw subjects data frame and clean it up. Note that apis don't pass datetime columns well, so
    these should be converted to datetime by the receiver.'''
    try:
        # Combine jsons into single dataframe
        subjects_raw = combine_mcc_json(subjects_json)
        subjects_raw.reset_index(drop=True, inplace=True)


        # Select only consented patients (obtain_date not null) who have not dropped out (ewdateterm null) and needed columns
        enrolled_cols = ['index', 'main_record_id', 'obtain_date',
            'mcc', 'redcap_data_access_group','sp_data_site']
        enrolled = subjects_raw[(subjects_raw.obtain_date != 'N/A') & (subjects_raw.ewdateterm == 'N/A')][enrolled_cols].copy()
        # Rename 'index' to 'record_id'
        enrolled.rename(columns={"index": "record_id"}, inplace = True)

        # Convert all string 'N/A' values to nan values
        enrolled = enrolled.replace('N/A', np.nan)

        # Coerce numeric values to enable merge
        enrolled = enrolled.apply(pd.to_numeric, errors='ignore')

        # Merge columns on the display terms dictionary to convert from database terminology to user terminology
        for i in display_terms_dict.keys():
            if i in enrolled.columns: # Merge columns if the column exists in the dataframe
                display_terms = display_terms_dict[i]
                if enrolled[i].dtype == np.float64:
                    # for display columns where data is numeric, merge on display dictionary, treating cols as floats to handle nas
                    display_terms[i] = display_terms[i].astype('float64')
                enrolled = enrolled.merge(display_terms, how='left', on=i)



        # Add screening sites
        enrolled = add_screening_site(screening_sites, enrolled, 'record_id')

        # Convert datetime columns
        enrolled['obtain_date'] = enrolled['obtain_date'] .apply(pd.to_datetime, errors='coerce')

        # get treatment site column
        enrolled['treatment_site'] = enrolled.apply(lambda x: use_b_if_not_a(x['sp_data_site_display'], x['redcap_data_access_group_display']), axis=1)
        enrolled['treatment_site_type'] = enrolled['treatment_site'] + "/" + enrolled['surgery_type']

        # Modify columns
        enrolled['obtain_month'] = enrolled['obtain_date'].dt.to_period('M')
        enrolled['Site'] = enrolled['screening_site'] + ' (' + enrolled['surgery_type'] + ')'

        return enrolled

    except Exception as e:
        traceback.print_exc()
        return None

# ----------------------------------------------------------------------------
# Enrollment FUNCTIONS
# ----------------------------------------------------------------------------

def enrollment_rollup(enrollment_df, index_col, grouping_cols, count_col_name, cumsum=True, fill_na_value = 0):
    enrollment_count = enrollment_df.groupby([index_col] + grouping_cols).size().reset_index(name=count_col_name).fillna({count_col_name:fill_na_value})
    if cumsum:
        enrollment_count['Cumulative'] = enrollment_count.groupby(grouping_cols)[count_col_name].cumsum()

    return enrollment_count

def get_site_enrollments(enrollment_count, mcc):
    site_enrollments = enrollment_count[enrollment_count.mcc == mcc].copy()
    replace_string = 'MCC'+str(mcc)+': '
    site_enrollments['Site'] = site_enrollments['Site'].str.replace(replace_string,'')
    site_enrollments = pd.pivot(site_enrollments, index=['obtain_month'], columns = 'Site', values=['Monthly','Cumulative'])
    site_enrollments = site_enrollments.swaplevel(0,1, axis=1).sort_index(1).reindex(['Monthly','Cumulative'], level=1, axis=1).reset_index()
    site_enrollments['Month'] = site_enrollments['obtain_month'].dt.strftime("%B")
    site_enrollments['Year'] = site_enrollments['obtain_month'].dt.strftime("%Y")
    site_enrollments = site_enrollments.set_index(['Month','Year']).drop(columns='obtain_month')
    return site_enrollments

def get_enrollment_expectations():
    enrollment_expectations_dict = {'mcc': ['1','1','2','2'],
                                'surgery_type':['TKA','Thoracic','Thoracic','TKA'],
                                'start_month': ['02/22','06/22','02/22','06/22'],
                                'expected_cumulative_start':[280,10,70,10], 'expected_monthly':[30,10,30,10]}

    enrollment_expectations_df = pd.DataFrame.from_dict(enrollment_expectations_dict)
    enrollment_expectations_df['start_month'] =  pd.to_datetime(enrollment_expectations_df['start_month'], format='%m/%y').dt.to_period('M')

    enrollment_expectations_df['mcc'] = enrollment_expectations_df['mcc'].astype(int)

    return enrollment_expectations_df

def get_enrollment_expectations_monthly(enrollment_expectations_df):
    mcc_type_expectations = pd.DataFrame()
    for i in range(len(enrollment_expectations_df)):
        mcc = enrollment_expectations_df.iloc[i]['mcc']
        surgery_type = enrollment_expectations_df.iloc[i]['surgery_type']
        start_month = enrollment_expectations_df.iloc[i]['start_month']
        expected_start_count = enrollment_expectations_df.iloc[i]['expected_cumulative_start']
        expected_monthly = enrollment_expectations_df.iloc[i]['expected_monthly']

        months = pd.period_range(start_month,datetime.now(),freq='M').tolist()
        expected_monthly_series = len(months) * [expected_monthly]

        for index, month in enumerate(months):
            new_row = {'mcc':mcc,
                       'surgery_type': surgery_type,
                       'Month': month,
                       'Expected: Monthly': expected_monthly_series[index],
                       'Expected: Cumulative': expected_start_count+index*expected_monthly}
            mcc_type_expectations = mcc_type_expectations.append(new_row, ignore_index=True)

    return mcc_type_expectations

def rollup_enrollment_expectations(enrollment_df, enrollment_expectations_df, monthly_expectations):
    enrollment_df = enrollment_df.merge(enrollment_expectations_df[['mcc','surgery_type','start_month']], how='left', on=['mcc','surgery_type'])

    # Determine if values in early months or should be broken out
    enrollment_df['expected_month'] = np.where(enrollment_df['obtain_month'] <= enrollment_df['start_month'], enrollment_df['start_month'], enrollment_df['obtain_month'] )

    # Rolll up data by month
    ee_rollup = enrollment_rollup(enrollment_df, 'expected_month', ['mcc','surgery_type'], 'Monthly').sort_values(by='mcc')
    ee_rollup_rename_dict={
        'expected_month':'Month',
        'Monthly': 'Actual: Monthly',
        'Cumulative': 'Actual: Cumulative'
    }
    ee_rollup.rename(columns=ee_rollup_rename_dict,inplace=True)

    # Merge on Monthly expectations
    ee_rollup = ee_rollup.merge(monthly_expectations, how='left', on=['mcc','surgery_type','Month'])

    # Calculate percent peformance actual vs expectations
    ee_rollup['Percent: Monthly'] = (100 * ee_rollup['Actual: Monthly'] / ee_rollup['Expected: Monthly']).round(1).astype(str) + '%'
    ee_rollup['Percent: Cumulative'] = (100 * ee_rollup['Actual: Cumulative'] / ee_rollup['Expected: Cumulative']).round(1).astype(str) + '%'
    ee_rollup.loc[ee_rollup['Actual: Monthly'] == 0, 'Percent: Monthly'] = ''


    ee_rollup['Date: Year'] = ee_rollup['Month'].dt.strftime("%Y")
    ee_rollup['Date: Month'] = ee_rollup['Month'].dt.strftime("%B")

    return ee_rollup


def get_plot_date(enrollment_df, summary_rollup):
    cols = ['Month', 'mcc', 'surgery_type', 'Expected: Monthly', 'Expected: Cumulative']
    expected_data = summary_rollup[cols].copy()
    expected_data.columns = [s.replace('Expected: ','') for s in list(expected_data.columns)]
    expected_data['type'] = 'Expected'
    ec= enrollment_rollup(enrollment_df, 'obtain_month', ['mcc','surgery_type'], 'Monthly').rename(columns={'obtain_month':'Month'})
    ec['type'] = 'Actual'

    df = ec.append(expected_data, ignore_index=True )

    df['Month'] = df['Month'].apply(lambda x: x.to_timestamp())

    return df
