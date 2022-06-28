# ----------------------------------------------------------------------------
# PYTHON LIBRARIES
# ----------------------------------------------------------------------------
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
    subjects_json = get_subjects_json(report, report_suffix,file_url_root, mcc_list =[1,2], source='url', DATA_PATH = DATA_PATH)
    enrolled = get_enrolled(subjects_json, screening_sites, display_terms_dict, display_terms_dict_multi)


    enrollment_count = enrollment_rollup(enrolled, 'obtain_month', ['mcc','screening_site','surgery_type','Site'], 'Monthly')
    #
    mcc1_enrollments = get_site_enrollments(enrollment_count, 1).reset_index()
    # print(mcc1_enrollments)
    mcc2_enrollments = get_site_enrollments(enrollment_count, 2).reset_index()
    # print(mcc2_enrollments)

    enrollment_expectations_df = get_enrollment_expectations()
    # print(enrollment_expectations_df)
    monthly_expectations = get_enrollment_expectations_monthly(enrollment_expectations_df)
    # print(monthly_expectations)
    summary_rollup = rollup_enrollment_expectations(enrolled, enrollment_expectations_df, monthly_expectations)
    print(summary_rollup)
    summary_options_list = [(x, y) for x in summary_rollup.mcc.unique() for y in summary_rollup.surgery_type.unique()]
    tab_summary_content_children = []
    for tup in summary_options_list:
        tup_summary = summary_rollup[(summary_rollup.mcc == tup[0]) & (summary_rollup.surgery_type == tup[1])]
        if len(tup_summary) > 0:
            table_cols = ['Date: Year', 'Date: Month', 'Actual: Monthly', 'Actual: Cumulative',
                            'Expected: Monthly', 'Expected: Cumulative', 'Percent: Monthly','Percent: Cumulative']
            tup_stup_table_df = convert_to_multindex(tup_summary[table_cols])
            table_id = 'table_mcc'+str(tup[0])+'_'+tup[1]
            # tup_table = html.Div(json.dump(tup_stup_table_df.to_dict('records')))
            tup_table = build_datatable_multi(tup_stup_table_df, table_id)
        else:
            tup_message = 'There is currently no data for ' + tup[1] + ' surgeries at MCC' + str(tup[0])
            tup_table = html.Div(tup_message)

        print(tup_summary)
        print(tup[1])
        tup_section = html.Div([
            html.H2('MCC' + str(tup[0]) + ': ' + tup[1]),
            html.Div(tup_table)
        ], style={'margin-bottom':'20px'})
        tab_summary_content_children.append(tup_section)
    # summary_sites = list(summary_rollup.Site.unique())
    # datatables = {}
    # for site in summary_sites:
    #     subset_df = summary_rollup[summary_rollup.Site==site].drop(['Site'],axis=1)
    #     subset_df_mi = convert_to_multindex(subset_df)
    #     table_columns, table_data = datatable_settings_multiindex(subset_df_mi)
    #
    #     site_dict = {
    #         'table_id': 'table_' + site.replace(" ", "_"),
    #         'table_columns': table_columns,
    #         'table_data':table_data
    #     }
    #     datatables[site] = site_dict


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

    page_layout = html.Div([
        # dcc.Store(id='store_meta', data = page_meta_dict),
        # dcc.Store(id='store_enrollment', data = enrollment_dict),

        html.Div(
            [
                tabs,
                # html.Row([
                #     html.Div(json.dumps(datatables[site])) for site in summary_sites
                #     ]),


                # html.Row([
                #     html.Col([
                #         html.Div(build_datatable(convert_to_multindex(summary_rollup), 'summary_multi')),
                #     ],width=6),
                #     html.Col([],width=6)
                # ])
                # html.Div(build_datatable(summary_rollup, 'summary_rollup', multi=False)),
                # html.Div([
                #     build_datatable(summary_rollup, 'summary_rollup', multi=False)
                #     # build_datatable_multi(summary_rollup[summary_rollup.Site==site], ('summary_table_' + site)) for site in list(summary_rollup.Site.unique())
                # ])
            ]
        ,id='report_content'
        , style =CONTENT_STYLE
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

    app.run_server(debug=True)
else:
    server = app.server
