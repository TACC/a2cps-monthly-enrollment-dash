# ----------------------------------------------------------------------------
# PYTHON LIBRARIES
# ----------------------------------------------------------------------------
import traceback

# Dash Framework
import dash_bootstrap_components as dbc
from dash import Dash, callback, clientside_callback, html, dcc, dash_table as dt, Input, Output, State, MATCH, ALL
from dash.exceptions import PreventUpdate
import dash_daq as daq

from dash_extensions import Download
from dash_extensions.snippets import send_file

# import local modules
from config_settings import *
from data_processing import *
from styling import *

# for export
import io
import flask

# Plotly graphing
import plotly.graph_objects as go

# ----------------------------------------------------------------------------
# DEBUGGING
# ----------------------------------------------------------------------------


# ----------------------------------------------------------------------------
# APP Settings
# ----------------------------------------------------------------------------

external_stylesheets_list = [dbc.themes.SANDSTONE] #  set any external stylesheets

app = Dash(__name__,
                external_stylesheets=external_stylesheets_list,
                meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1'}],
                assets_folder=ASSETS_PATH,
                requests_pathname_prefix=REQUESTS_PATHNAME_PREFIX,
                suppress_callback_exceptions=True
                )


# ----------------------------------------------------------------------------
# POINTERS TO DATA FILES AND APIS
# ----------------------------------------------------------------------------
display_terms_file = 'A2CPS_display_terms.csv'

# Directions for locating file at TACC
file_url_root ='https://api.a2cps.org/files/v2/download/public/system/a2cps.storage.community/reports'
report = 'subjects'
report_suffix = report + '-[mcc]-latest.json'
mcc_list=[1,2]


# ----------------------------------------------------------------------------
# FUNCTIONS FOR DASH UI COMPONENTS
# ----------------------------------------------------------------------------
def build_datatable_multi(df, table_id, fill_width = False):
    table_columns, table_data = datatable_settings_multiindex(df)
    new_datatable = build_datatable(table_id, table_columns, table_data, fill_width)
    return new_datatable

def build_datatable(table_id, table_columns, table_data, fill_width = False):
    try:
        new_datatable =  dt.DataTable(
                id = table_id,
                columns=table_columns,
                data=table_data,
                css=[{'selector': '.row', 'rule': 'margin: 0; flex-wrap: nowrap'},
                     {'selector':'.export','rule':export_style }
                    # {'selector':'.export','rule':'position:absolute;right:25px;bottom:-35px;font-family:Arial, Helvetica, sans-serif,border-radius: .25re'}
                    ],
                # style_cell= {
                #     'text-align':'left',
                #     'vertical-align': 'top',
                #     'font-family':'sans-serif',
                #     'padding': '5px',
                #     'whiteSpace': 'normal',
                #     'height': 'auto',
                #     },
                # style_as_list_view=True,
                # style_header={
                #     'backgroundColor': 'grey',
                #     'whiteSpace': 'normal',
                #     'fontWeight': 'bold',
                #     'color': 'white',
                # },

                fill_width=fill_width,
                style_table={'overflowX': 'auto'},
                # export_format="csv",
                merge_duplicate_headers=True,
            )
        return new_datatable
    except Exception as e:
        traceback.print_exc()
        return None

# ----------------------------------------------------------------------------
# TABS
# ----------------------------------------------------------------------------


# ----------------------------------------------------------------------------
# DASH APP LAYOUT FUNCTION
# ----------------------------------------------------------------------------
def serve_layout():
    page_meta_dict, enrollment_dict = {'report_date_msg':''}, {}
    report_date = datetime.now()
    report_children = ['exception']

    # try:
    # get data for page
    # print('time parameters')
    today, start_report, end_report, report_date_msg, report_range_msg  = get_time_parameters(report_date)
    page_meta_dict['report_date_msg'] = report_date_msg
    page_meta_dict['report_range_msg'] = report_range_msg
    # print('get data inputs')

    # display_terms, display_terms_dict, display_terms_dict_multi, clean_weekly, consented, screening_data, clean_adverse, centers_df, r_status = get_data_for_page(ASSETS_PATH, display_terms_file, file_url_root, report, report_suffix, mcc_list)
    display_terms, display_terms_dict, display_terms_dict_multi = load_display_terms(ASSETS_PATH, 'A2CPS_display_terms.csv')
    screening_sites = pd.read_csv(os.path.join(ASSETS_PATH, 'screening_sites.csv'))

    # Run Data Calls
    # print(os.path.join(DATA_PATH,'test_file.txt'))
    # try:
    subjects_json, data_source, data_date = get_subjects_json(report, report_suffix,file_url_root, mcc_list =[1,2],  DATA_PATH = DATA_PATH)
    page_meta_dict['data_source'] = data_source
    page_meta_dict['data_date'] = data_date
    # except:
    #     subjects_json = get_subjects_json(report, report_suffix,file_url_root, mcc_list =[1,2], source='local', DATA_PATH = DATA_PATH)
    #     page_meta_dict['data_source'] = 'loca data files'
    #     page_meta_dict['data_date'] = '06/15/2022'

    print(page_meta_dict['data_source'])
    print(page_meta_dict['data_date'])

    if subjects_json:
    ### DATA PROCESSING
        enrolled = get_enrolled(subjects_json, screening_sites, display_terms_dict, display_terms_dict_multi)
        enrollment_count = enrollment_rollup(enrolled, 'obtain_month', ['mcc','screening_site','surgery_type','Site'], 'Monthly')
        mcc1_enrollments = get_site_enrollments(enrollment_count, 1).reset_index()
        mcc2_enrollments = get_site_enrollments(enrollment_count, 2).reset_index()
        enrollment_expectations_df = get_enrollment_expectations()
        monthly_expectations = get_enrollment_expectations_monthly(enrollment_expectations_df)
        summary_rollup = rollup_enrollment_expectations(enrolled, enrollment_expectations_df, monthly_expectations)
        summary_options_list = [(x, y) for x in summary_rollup.mcc.unique() for y in summary_rollup.surgery_type.unique()]
        tab_summary_content_children = []
        for tup in summary_options_list:
            tup_summary = summary_rollup[(summary_rollup.mcc == tup[0]) & (summary_rollup.surgery_type == tup[1])]
            if len(tup_summary) > 0:
                table_cols = ['Date: Year', 'Date: Month', 'Actual: Monthly', 'Actual: Cumulative',
                                'Expected: Monthly', 'Expected: Cumulative', 'Percent: Monthly','Percent: Cumulative']
                tup_stup_table_df = convert_to_multindex(tup_summary[table_cols])
                table_id = 'table_mcc'+str(tup[0])+'_'+tup[1]
                tup_table = build_datatable_multi(tup_stup_table_df, table_id)
            else:
                tup_message = 'There is currently no data for ' + tup[1] + ' surgeries at MCC' + str(tup[0])
                tup_table = html.Div(tup_message)
            tup_section = html.Div([
                html.H2('MCC' + str(tup[0]) + ': ' + tup[1]),
                html.Div(tup_table)
            ], style={'margin-bottom':'20px'})
            tab_summary_content_children.append(tup_section)

    ### BUILD PAGE COMPONENTS
        data_source = 'Data Source: ' + page_meta_dict['data_source']
        data_date = 'Data Date: ' + page_meta_dict['data_date']

        tab_enrollments = html.Div([
            html.H2('MCC 1'),
            build_datatable_multi(mcc1_enrollments, 'mcc1_datatable'),
            html.H2('MCC 2'),
            build_datatable_multi(mcc2_enrollments, 'mcc2_datatable')
            ])

        tab_summary_content = html.Div(
            tab_summary_content_children
        )

        tabs = html.Div([
                    dcc.Tabs(id='tabs_tables', children=[
                        dcc.Tab(label='Site Enrollments', id='tab_1', children=[
                            html.Div([tab_enrollments], id='section_1'),
                        ]),
                        dcc.Tab(label="Site / Surgery Summary", id='tab_3', children=[
                            html.Div([tab_summary_content], id='section_3'),
                        ]),

                    ]),
                    ])
    else:
        data_source = 'unavailable'
        data_date = 'unavailable'
        tabs = 'Data unavailable'

    page_layout = html.Div([
        dcc.Loading(
            id="loading-2",
            children=[
                html.Div(
        [
            dbc.Row([
                dbc.Col([
                    html.H1('Enrollment Report', style={'textAlign': 'center'})
                ], width=12),
            ]),

            dbc.Row([
                dbc.Col([
                    html.P(data_source),
                ], width=6),
                dbc.Col([
                    html.P(data_date),
                ], width=6, style={'text-align': 'right'}),
            ]),

            dbc.Row([
                dbc.Col([
                    tabs,
                ])
                ]),

        ]
    ,id='report_content'
    , style =CONTENT_STYLE
    )
            ],
            type="circle",
        )
    ], style=TACC_IFRAME_SIZE
    )
    return page_layout

app.layout = serve_layout

# ----------------------------------------------------------------------------
# DATA CALLBACKS
# ----------------------------------------------------------------------------


# ----------------------------------------------------------------------------
# RUN APPLICATION
# ----------------------------------------------------------------------------

if __name__ == '__main__':
    app.run_server()
    # app.run_server(debug=True, port=8020)dev
else:
    server = app.server
