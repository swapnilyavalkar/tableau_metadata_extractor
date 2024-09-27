# without assiciated field names in data source filters.
import pandas as pd
from tableau_api_lib import TableauServerConnection

SERVER_URL = 'https://abc.com/'
TOKEN_NAME = 'PAT-NAME'
TOKEN_VALUE = 'Secret'
SITE_NAME = 'site-name'
conn = ''

df_datasource_details = pd.DataFrame()
df_datasourceFilters = pd.DataFrame()
df_upstreamTables = pd.DataFrame()

data_source_name = ''


def TSsignIn():
    global conn
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


def GetDataSourceMetadata(batch_size=100):
    global conn, df_datasource_details, df_datasourceFilters, df_upstreamTables, data_source_name
    all_calculated_fields = []
    has_next = True
    start = 0
    while has_next is True:
        query = """
            {
              publishedDatasourcesConnection(first: %s, offset: %s) {
                nodes {
                  projectName
                  name
                  owner {
                    username
                  }
                  fields {
                    name
                    __typename
                    ... on ColumnField {
                      dataType
                      role
                    }
                    ... on CalculatedField {
                      formula
                      dataType
                      role
                    }
                    isHidden
                    folderName
                  }
                  datasourceFilters {
                    field {
                      name
                    }
                  }
                  upstreamTables {
                    name
                    schema
                    fullName
                    connectionType
                    database {
                      name
                      __typename
                    }
                  }
                }
                pageInfo {
                  hasNextPage
                  endCursor
                }
              }
            }
            """ % (batch_size, start)

        response = conn.metadata_graphql_query(query=query)
        #all_calculated_fields.extend(response.json()['data']['publishedDatasourcesConnection']['nodes'])
        if response.json()['data']['publishedDatasourcesConnection']['nodes']:
            for dataSourceItem in response.json()['data']['publishedDatasourcesConnection']['nodes']:
                if dataSourceItem['fields']:
                    for fieldItem in dataSourceItem['fields']:
                        if fieldItem['__typename'] == 'CalculatedField':
                            if 'dataType' in fieldItem and 'role' in fieldItem:
                                data_source_name = dataSourceItem['name']
                                df_datasource_details = df_datasource_details.append(
                                    {
                                        'DATASOURCE_NAME': dataSourceItem['name'],
                                        'DATASOURCE_PROJECT_NAME': dataSourceItem['projectName'],
                                        'DATASOURCE_OWNER_NAME': dataSourceItem['owner']['username'],
                                        'FIELD_NAME': fieldItem['name'],
                                        'FIELD_TYPE': fieldItem['__typename'],
                                        'FORMULA': fieldItem['formula'],
                                        'FIELD_DATA_TYPE': fieldItem['dataType'],
                                        'FIELD_ROLE': fieldItem['role'],
                                        'FIELD_IS_HIDDEN': fieldItem['isHidden'],
                                        'FOLDER_NAME': fieldItem['folderName'],

                                    },
                                    ignore_index=True)
                            else:
                                data_source_name = dataSourceItem['name']
                                df_datasource_details = df_datasource_details.append(
                                    {
                                        'DATASOURCE_NAME': dataSourceItem['name'],
                                        'DATASOURCE_PROJECT_NAME': dataSourceItem['projectName'],
                                        'DATASOURCE_OWNER_NAME': dataSourceItem['owner']['username'],
                                        'FIELD_NAME': fieldItem['name'],
                                        'FIELD_TYPE': fieldItem['__typename'],
                                        'FORMULA': fieldItem['formula'],
                                        'FIELD_DATA_TYPE': '',
                                        'FIELD_ROLE': '',
                                        'FIELD_IS_HIDDEN': fieldItem['isHidden'],
                                        'FOLDER_NAME': fieldItem['folderName'],

                                    },
                                    ignore_index=True)
                        else:
                            if 'dataType' in fieldItem and 'role' in fieldItem:
                                data_source_name = dataSourceItem['name']
                                df_datasource_details = df_datasource_details.append(
                                    {
                                        'DATASOURCE_NAME': dataSourceItem['name'],
                                        'DATASOURCE_PROJECT_NAME': dataSourceItem['projectName'],
                                        'DATASOURCE_OWNER_NAME': dataSourceItem['owner']['username'],
                                        'FIELD_NAME': fieldItem['name'],
                                        'FIELD_TYPE': fieldItem['__typename'],
                                        'FORMULA': '',
                                        'FIELD_DATA_TYPE': fieldItem['dataType'],
                                        'FIELD_ROLE': fieldItem['role'],
                                        'FIELD_IS_HIDDEN': fieldItem['isHidden'],
                                        'FOLDER_NAME': fieldItem['folderName'],

                                    },
                                    ignore_index=True)
                            else:
                                data_source_name = dataSourceItem['name']
                                df_datasource_details = df_datasource_details.append(
                                    {
                                        'DATASOURCE_NAME': dataSourceItem['name'],
                                        'DATASOURCE_PROJECT_NAME': dataSourceItem['projectName'],
                                        'DATASOURCE_OWNER_NAME': dataSourceItem['owner']['username'],
                                        'FIELD_NAME': fieldItem['name'],
                                        'FIELD_TYPE': fieldItem['__typename'],
                                        'FORMULA': '',
                                        'FIELD_DATA_TYPE': '',
                                        'FIELD_ROLE': '',
                                        'FIELD_IS_HIDDEN': fieldItem['isHidden'],
                                        'FOLDER_NAME': fieldItem['folderName'],

                                    },
                                    ignore_index=True)
                if dataSourceItem['datasourceFilters']:
                    for datasourceFilter in dataSourceItem['datasourceFilters']:
                        df_datasourceFilters = df_datasourceFilters.append(
                            {
                                'DATA_SOURCE_NAME': dataSourceItem['name'],
                                'DS_FILTER_NAME': datasourceFilter['field']['name'],
                            },
                            ignore_index=True)
                else:
                    df_datasourceFilters = df_datasourceFilters.append(
                        {
                            'DATA_SOURCE_NAME': dataSourceItem['name'],
                            'FIELD_NAME': '',
                        },
                        ignore_index=True)
                if dataSourceItem['upstreamTables']:
                    for upstreamTable in dataSourceItem['upstreamTables']:
                        df_upstreamTables = df_upstreamTables.append(
                            {
                                'DATA_SOURCE_NAME': dataSourceItem['name'],
                                'TABLE_NAME': upstreamTable['name'],
                                'SCHEMA': upstreamTable['schema'],
                                'TABLE_FULLNAME': upstreamTable['fullName'],
                                'CONNECTION_TYPE': upstreamTable['connectionType'],
                                'DB_NAME': upstreamTable['database']['name'],
                                'DB_TYPE': upstreamTable['database']['__typename']
                            },
                            ignore_index=True)
                else:
                    df_upstreamTables = df_upstreamTables.append(
                        {
                            'DATA_SOURCE_NAME': dataSourceItem['name'],
                            'TABLE_NAME': '',
                            'SCHEMA': '',
                            'TABLE_FULLNAME': '',
                            'CONNECTION_TYPE': '',
                            'DB_NAME': '',
                            'DB_TYPE': ''
                        },
                        ignore_index=True)
        start = start + batch_size
        if response.json()['data']['publishedDatasourcesConnection']['pageInfo']['hasNextPage'] == False:
            has_next = False
    conn.sign_out()
    df_datasource_details['FIELD_DATA_TYPE'] = df_datasource_details['FIELD_DATA_TYPE'].str.replace('REAL', 'FLOAT')
    df_datasource_details['FIELD_IS_HIDDEN'] = df_datasource_details['FIELD_IS_HIDDEN'].replace(0, 'FALSE')
    df_datasource_details['FIELD_IS_HIDDEN'] = df_datasource_details['FIELD_IS_HIDDEN'].replace(1, 'TRUE')

    #file_name = data_source_name + '.xlsx'
    file_name = 'DataSourceMetadata.xlsx'

    with pd.ExcelWriter(file_name) as writer:
        df_datasource_details.to_excel(writer, sheet_name="Field Details", index=False, freeze_panes=[1, 1])
        df_datasourceFilters.to_excel(writer, sheet_name="DataSource Filters", index=False, freeze_panes=[1, 1])
        df_upstreamTables.to_excel(writer, sheet_name="Table Details", index=False, freeze_panes=[1, 1])

TSsignIn()
GetDataSourceMetadata(batch_size=100)