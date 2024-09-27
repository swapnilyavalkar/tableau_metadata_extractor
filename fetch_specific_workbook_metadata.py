import pandas as pd
from tableau_api_lib import TableauServerConnection

SERVER_URL = 'https://abc.com/'
TOKEN_NAME = 'PAT-NAME'
TOKEN_VALUE = 'Secret'
SITE_NAME = 'site-name'
conn = ''

df_workbook_details = pd.DataFrame()

query = open('workbooks_metadata_query.graphql', 'r').read()
workbook_name = ''
sheet_name = ''


def TSsignIn():
    global conn, df_workbook_details, workbook_name, sheet_name
    ts_config = {
        'test_env': {
            'server': SERVER_URL,
            'api_version': '3.10',
            'personal_access_token_name': TOKEN_NAME,
            'personal_access_token_secret': TOKEN_VALUE,
            'site_name': SITE_NAME,
            'site_url': SITE_NAME
        }
    }
    conn = TableauServerConnection(ts_config, env='test_env', ssl_verify=False)
    conn.sign_in()
    response = conn.metadata_graphql_query(query=query)
    for workbooksItem in response.json()['data']['workbooks']:
        workbook_name = workbooksItem['name']
        nodes = workbooksItem['sheetsConnection']['nodes']
        for sheetsDetails in nodes:
            for datasourceFields in sheetsDetails['datasourceFields']:
                if datasourceFields['__typename'] == 'CalculatedField':
                    if datasourceFields['referencedByCalculations']:
                        for referencedByCalculations in datasourceFields['referencedByCalculations']:
                            if referencedByCalculations['referencedByFields']:
                                for referencedByFields in referencedByCalculations['referencedByFields']:
                                    for fields in referencedByFields['fields']:
                                        if referencedByFields['__typename'] == 'CalculatedField':
                                            if fields['__typename'] == 'CalculatedField':
                                                df_workbook_details = df_workbook_details.append(
                                                    {'WORKBOOK_NAME': workbooksItem['name'],
                                                     'WORKBOOK_PROJECT_NAME': workbooksItem['projectName'],
                                                     'WORKBOOK_OWNER_NAME': workbooksItem['owner']['username'],
                                                     'SHEET_NAME': sheetsDetails['name'],
                                                     'FIELD_NAME': datasourceFields['name'],
                                                     'FIELD_TYPE': datasourceFields['__typename'],
                                                     'FORMULA': datasourceFields['formula'],
                                                     'FOLDER_NAME': datasourceFields['folderName'],
                                                     'REFERENCED_NAME': referencedByCalculations['name'],
                                                     'RBF_FIELD_TYPE': referencedByFields['__typename'],
                                                     'RBF_FIELD_FORMULA': referencedByFields['formula'],
                                                     'FIELDS_NAME': fields['name'],
                                                     'FIELDS_TYPE': fields['__typename'],
                                                     'FIELDS_FORMULA': fields['formula'],
                                                     'DATASOURCE_NAME': datasourceFields['datasource']['name']
                                                     },
                                                    ignore_index=True)
                                            else:
                                                df_workbook_details = df_workbook_details.append(
                                                    {'WORKBOOK_NAME': workbooksItem['name'],
                                                     'WORKBOOK_PROJECT_NAME': workbooksItem['projectName'],
                                                     'WORKBOOK_OWNER_NAME': workbooksItem['owner']['username'],
                                                     'SHEET_NAME': sheetsDetails['name'],
                                                     'FIELD_NAME': datasourceFields['name'],
                                                     'FIELD_TYPE': datasourceFields['__typename'],
                                                     'FORMULA': datasourceFields['formula'],
                                                     'FOLDER_NAME': datasourceFields['folderName'],
                                                     'REFERENCED_NAME': referencedByCalculations['name'],
                                                     'RBF_FIELD_TYPE': referencedByFields['__typename'],
                                                     'RBF_FIELD_FORMULA': referencedByFields['formula'],
                                                     'FIELDS_NAME': fields['name'],
                                                     'FIELDS_TYPE': fields['__typename'],
                                                     'FIELDS_FORMULA': '',
                                                     'DATASOURCE_NAME': datasourceFields['datasource']['name']
                                                     },
                                                    ignore_index=True)
                                        else:
                                            df_workbook_details = df_workbook_details.append(
                                                {'WORKBOOK_NAME': workbooksItem['name'],
                                                 'WORKBOOK_PROJECT_NAME': workbooksItem['projectName'],
                                                 'WORKBOOK_OWNER_NAME': workbooksItem['owner']['username'],
                                                 'SHEET_NAME': sheetsDetails['name'],
                                                 'FIELD_NAME': datasourceFields['name'],
                                                 'FIELD_TYPE': datasourceFields['__typename'],
                                                 'FORMULA': datasourceFields['formula'],
                                                 'FOLDER_NAME': datasourceFields['folderName'],
                                                 'REFERENCED_NAME': referencedByCalculations['name'],
                                                 'RBF_FIELD_TYPE': referencedByFields['__typename'],
                                                 'RBF_FIELD_FORMULA': '',
                                                 'FIELDS_NAME': '',
                                                 'FIELDS_TYPE': '',
                                                 'FIELDS_FORMULA': '',
                                                 'DATASOURCE_NAME': datasourceFields['datasource']['name']
                                                 },
                                                ignore_index=True)
                            else:
                                df_workbook_details = df_workbook_details.append(
                                    {'WORKBOOK_NAME': workbooksItem['name'],
                                     'WORKBOOK_PROJECT_NAME': workbooksItem['projectName'],
                                     'WORKBOOK_OWNER_NAME': workbooksItem['owner']['username'],
                                     'SHEET_NAME': sheetsDetails['name'],
                                     'FIELD_NAME': datasourceFields['name'],
                                     'FIELD_TYPE': datasourceFields['__typename'],
                                     'FORMULA': datasourceFields['formula'],
                                     'FOLDER_NAME': datasourceFields['folderName'],
                                     'REFERENCED_NAME': referencedByCalculations['name'],
                                     'RBF_FIELD_TYPE': '',
                                     'RBF_FIELD_FORMULA': '',
                                     'FIELDS_NAME': '',
                                     'FIELDS_TYPE': '',
                                     'FIELDS_FORMULA': '',
                                     'DATASOURCE_NAME': datasourceFields['datasource']['name']
                                     },
                                    ignore_index=True)

                    else:
                        df_workbook_details = df_workbook_details.append(
                            {'WORKBOOK_NAME': workbooksItem['name'],
                             'WORKBOOK_PROJECT_NAME': workbooksItem['projectName'],
                             'WORKBOOK_OWNER_NAME': workbooksItem['owner']['username'],
                             'SHEET_NAME': sheetsDetails['name'],
                             'FIELD_NAME': datasourceFields['name'],
                             'FIELD_TYPE': datasourceFields['__typename'],
                             'FORMULA': datasourceFields['formula'],
                             'FOLDER_NAME': datasourceFields['folderName'],
                             'REFERENCED_NAME': '',
                             'RBF_FIELD_TYPE': '',
                             'RBF_FIELD_FORMULA': '',
                             'FIELDS_NAME': '',
                             'FIELDS_TYPE': '',
                             'FIELDS_FORMULA': '',
                             'DATASOURCE_NAME': datasourceFields['datasource']['name']
                             },
                            ignore_index=True)
                else:
                    if datasourceFields['referencedByCalculations']:
                        for referencedByCalculations in datasourceFields['referencedByCalculations']:
                            if referencedByCalculations['referencedByFields']:
                                for referencedByFields in referencedByCalculations['referencedByFields']:
                                    for fields in referencedByFields['fields']:
                                        if referencedByFields['__typename'] == 'CalculatedField':
                                            if fields['__typename'] == 'CalculatedField':
                                                df_workbook_details = df_workbook_details.append(
                                                    {'WORKBOOK_NAME': workbooksItem['name'],
                                                     'WORKBOOK_PROJECT_NAME': workbooksItem['projectName'],
                                                     'WORKBOOK_OWNER_NAME': workbooksItem['owner']['username'],
                                                     'SHEET_NAME': sheetsDetails['name'],
                                                     'FIELD_NAME': datasourceFields['name'],
                                                     'FIELD_TYPE': datasourceFields['__typename'],
                                                     'FORMULA': '',
                                                     'FOLDER_NAME': datasourceFields['folderName'],
                                                     'REFERENCED_NAME': referencedByCalculations['name'],
                                                     'RBF_FIELD_TYPE': referencedByFields['__typename'],
                                                     'RBF_FIELD_FORMULA': referencedByFields['formula'],
                                                     'FIELDS_NAME': fields['name'],
                                                     'FIELDS_TYPE': fields['__typename'],
                                                     'FIELDS_FORMULA': fields['formula'],
                                                     'DATASOURCE_NAME': datasourceFields['datasource']['name']
                                                     },
                                                    ignore_index=True)
                                            else:
                                                df_workbook_details = df_workbook_details.append(
                                                    {'WORKBOOK_NAME': workbooksItem['name'],
                                                     'WORKBOOK_PROJECT_NAME': workbooksItem['projectName'],
                                                     'WORKBOOK_OWNER_NAME': workbooksItem['owner']['username'],
                                                     'SHEET_NAME': sheetsDetails['name'],
                                                     'FIELD_NAME': datasourceFields['name'],
                                                     'FIELD_TYPE': datasourceFields['__typename'],
                                                     'FORMULA': '',
                                                     'FOLDER_NAME': datasourceFields['folderName'],
                                                     'REFERENCED_NAME': referencedByCalculations['name'],
                                                     'RBF_FIELD_TYPE': referencedByFields['__typename'],
                                                     'RBF_FIELD_FORMULA': referencedByFields['formula'],
                                                     'FIELDS_NAME': fields['name'],
                                                     'FIELDS_TYPE': fields['__typename'],
                                                     'FIELDS_FORMULA': '',
                                                     'DATASOURCE_NAME': datasourceFields['datasource']['name']
                                                     },
                                                    ignore_index=True)
                                        else:
                                            df_workbook_details = df_workbook_details.append(
                                                {'WORKBOOK_NAME': workbooksItem['name'],
                                                 'WORKBOOK_PROJECT_NAME': workbooksItem['projectName'],
                                                 'WORKBOOK_OWNER_NAME': workbooksItem['owner']['username'],
                                                 'SHEET_NAME': sheetsDetails['name'],
                                                 'FIELD_NAME': datasourceFields['name'],
                                                 'FIELD_TYPE': datasourceFields['__typename'],
                                                 'FORMULA': '',
                                                 'FOLDER_NAME': datasourceFields['folderName'],
                                                 'REFERENCED_NAME': referencedByCalculations['name'],
                                                 'RBF_FIELD_TYPE': referencedByFields['__typename'],
                                                 'RBF_FIELD_FORMULA': '',
                                                 'FIELDS_NAME': '',
                                                 'FIELDS_TYPE': '',
                                                 'FIELDS_FORMULA': '',
                                                 'DATASOURCE_NAME': datasourceFields['datasource']['name']
                                                 },
                                                ignore_index=True)
                            else:
                                df_workbook_details = df_workbook_details.append(
                                    {'WORKBOOK_NAME': workbooksItem['name'],
                                     'WORKBOOK_PROJECT_NAME': workbooksItem['projectName'],
                                     'WORKBOOK_OWNER_NAME': workbooksItem['owner']['username'],
                                     'SHEET_NAME': sheetsDetails['name'],
                                     'FIELD_NAME': datasourceFields['name'],
                                     'FIELD_TYPE': datasourceFields['__typename'],
                                     'FORMULA': '',
                                     'FOLDER_NAME': datasourceFields['folderName'],
                                     'REFERENCED_NAME': referencedByCalculations['name'],
                                     'RBF_FIELD_TYPE': '',
                                     'RBF_FIELD_FORMULA': '',
                                     'FIELDS_NAME': '',
                                     'FIELDS_TYPE': '',
                                     'FIELDS_FORMULA': '',
                                     'DATASOURCE_NAME': datasourceFields['datasource']['name']
                                     },
                                    ignore_index=True)
                    else:
                        df_workbook_details = df_workbook_details.append(
                            {'WORKBOOK_NAME': workbooksItem['name'],
                             'WORKBOOK_PROJECT_NAME': workbooksItem['projectName'],
                             'WORKBOOK_OWNER_NAME': workbooksItem['owner']['username'],
                             'SHEET_NAME': sheetsDetails['name'],
                             'FIELD_NAME': datasourceFields['name'],
                             'FIELD_TYPE': datasourceFields['__typename'],
                             'FORMULA': '',
                             'FOLDER_NAME': datasourceFields['folderName'],
                             'REFERENCED_NAME': '',
                             'RBF_FIELD_TYPE': '',
                             'RBF_FIELD_FORMULA': '',
                             'FIELDS_NAME': '',
                             'FIELDS_TYPE': '',
                             'FIELDS_FORMULA': '',
                             'DATASOURCE_NAME': datasourceFields['datasource']['name']
                             },
                            ignore_index=True)

    file_name = workbook_name + ".xlsx"
    conn.sign_out()
    df_workbook_details.to_excel(file_name, index=False, freeze_panes=[1, 1])


TSsignIn()