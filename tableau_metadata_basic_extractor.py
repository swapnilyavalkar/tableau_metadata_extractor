import pandas as pd
from pandas import json_normalize
from tableau_api_lib import TableauServerConnection

server = "https://abc.com" # mention your tableau server URL here.
pat_name = "pat-name" # mention your pat name here.
pat_secret = "secret" # mention your secret here.
site = "" # mention your site name here.

# Configuration for the Tableau Server
tableau_server_config = {
    'my_env': {
        'server': server,  # replace with your own server
        'api_version': '3.21',  # replace with your REST API version
        'personal_access_token_name': pat_name,
        'personal_access_token_secret': pat_secret,
        'site_name': site,  # Default site, will change dynamically
        'site_url': site  # Default site URL, will change dynamically
    }
}

# GraphQL query to fetch detailed workbook information
query_workbooks_tables = """
{
  workbooks {
    name
    id
    projectName
    owner {
      username
    }
    updatedAt
    views {
      name
    }
    upstreamTables {
      name
      database {
        name
        connectionType
      }
    }
    upstreamDatasources {
      name
      hasExtracts
      containsUnsupportedCustomSql
    }
  }
}
"""

# GraphQL query to fetch detailed data source information
query_datasources_tables = """
{
  publishedDatasources {
    name
    id
    projectName
    owner {
      username
    }
    updatedAt
    hasExtracts
    containsUnsupportedCustomSql
    upstreamTables {
      name
      database {
        name
        connectionType
      }
    }
  }
}
"""


# Function to fetch metadata from Tableau Server
def get_metadata_json(conn, query, content_type):
    results = conn.metadata_graphql_query(query)

    # Check the raw response
    #print(f"Response for {content_type} query: {results.text}")

    # Check if the response has data and handle errors
    try:
        results_json = results.json().get('data', {}).get(content_type, None)
        if results_json is None:
            raise ValueError(f"No data found in the response for {content_type}.")
        return results_json
    except Exception as e:
        print(f"Error fetching {content_type} data: {e}")
        raise


# Function to process workbook metadata, splitting views into individual rows, and adding workbook URL
def get_workbook_metadata(json_data, site_name, server_url):
    records = []
    for workbook in json_data:
        workbook_name = workbook['name']
        project_name = workbook['projectName']
        workbook_owner = workbook['owner']['username']
        last_modified = workbook['updatedAt']
        workbook_url = f"{server_url}/#/site/{site_name}/workbooks/{workbook['id']}"  # Construct workbook URL
        workbook_hyperlink = f'=HYPERLINK("{workbook_url}", "Click Here")'  # Excel hyperlink formula
        for view in workbook['views']:  # Separate row for each view
            view_name = view['name']
            # Details for data sources and database connection
            for datasource in workbook['upstreamDatasources']:
                datasource_name = datasource['name']
                connection_type = "Published" if datasource['hasExtracts'] else "Live"
                custom_sql = "Yes" if datasource['containsUnsupportedCustomSql'] else "No"
                for table in workbook['upstreamTables']:
                    table_name = table['name']
                    db_name = table['database']['name']
                    db_connection_type = table['database']['connectionType']
                    records.append({
                        'site_name': site_name,
                        #'workbook_url': workbook_hyperlink,
                        'workbook_name': workbook_name,
                        'workbook_owner': workbook_owner,
                        'last_modified': last_modified,
                        'view_name': view_name,
                        'project_name': project_name,
                        'table_name': table_name,
                        'datasource_name': datasource_name,
                        'is_published': connection_type,
                        'is_custom_sql': custom_sql,
                        'db_name': db_name,
                        'db_connectionType': db_connection_type
                    })
    return pd.DataFrame(records)


# Function to process data source metadata, splitting views into individual rows, and adding data source URL
def get_datasource_metadata(json_data, site_name, server_url):
    records = []
    for datasource in json_data:
        datasource_name = datasource['name']
        project_name = datasource['projectName']
        datasource_owner = datasource['owner']['username']
        last_modified = datasource['updatedAt']
        has_extracts = "Yes" if datasource['hasExtracts'] else "No"
        custom_sql = "Yes" if datasource['containsUnsupportedCustomSql'] else "No"
        datasource_url = f"{server_url}/#/site/{site_name}/datasources/{datasource['id']}"  # Construct datasource URL
        datasource_hyperlink = f'=HYPERLINK("{datasource_url}", "Click Here")'  # Excel hyperlink formula
        for table in datasource['upstreamTables']:
            table_name = table['name']
            db_name = table['database']['name']
            db_connection_type = table['database']['connectionType']
            records.append({
                'site_name': site_name,
                #'datasource_url': datasource_hyperlink,
                'datasource_name': datasource_name,
                'datasource_owner': datasource_owner,
                'last_modified': last_modified,
                'table_name': table_name,
                'is_custom_sql': custom_sql,
                'db_name': db_name,
                'db_connectionType': db_connection_type,
                'has_extracts': has_extracts
            })
    return pd.DataFrame(records)


def main():
    # Establish connection
    conn = TableauServerConnection(tableau_server_config, 'my_env')
    conn.sign_in()

    server_url = tableau_server_config['my_env']['server']
    site_name = tableau_server_config['my_env']['site_url']

    # Query metadata for workbook and data source tables
    print(f"Querying data from site '{site_name}'...")

    try:
        # Fetch workbook data with detailed logging
        wb_query_results_json = get_metadata_json(conn, query_workbooks_tables, 'workbooks')
        workbook_metadata_df = get_workbook_metadata(wb_query_results_json, site_name, server_url)
        workbook_metadata_df.to_excel(f'{site_name}_workbook_metadata_detailed.xlsx', index=False)
        print(f"Workbook metadata saved to {site_name}_workbook_metadata_detailed.xlsx")

        # Fetch data source data
        ds_query_results_json = get_metadata_json(conn, query_datasources_tables, 'publishedDatasources')
        datasource_metadata_df = get_datasource_metadata(ds_query_results_json, site_name, server_url)
        datasource_metadata_df.to_excel(f'{site_name}_datasource_metadata_detailed.xlsx', index=False)
        print(f"Data source metadata saved to {site_name}_datasource_metadata_detailed.xlsx")

    except Exception as e:
        print(f"An error occurred: {e}")

    conn.sign_out()


if __name__ == "__main__":
    main()
