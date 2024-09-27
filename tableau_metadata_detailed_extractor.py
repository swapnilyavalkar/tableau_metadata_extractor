import pandas as pd
from pandas import json_normalize
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_sites_dataframe

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


# GraphQL query to fetch workbook details including tables, views, owner, and URLs
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
  }
}
"""

# GraphQL query to fetch data source details including tables, owner, and URLs
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
    upstreamTables {
      name
      database {
        name
        connectionType
      }
    }
    downstreamWorkbooks {
      name
      views {
        name
      }
    }
  }
}
"""


# Function to fetch metadata from Tableau Server
def get_metadata_json(conn, query, content_type):
    results = conn.metadata_graphql_query(query)
    results_json = results.json()['data'][content_type]
    return results_json


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
            for table in workbook['upstreamTables']:
                table_name = table['name']
                db_name = table['database']['name']
                db_connection_type = table['database']['connectionType']
                records.append({
                    'site_name': site_name,
                    #'workbook_url': workbook_hyperlink,  # Use Excel hyperlink formula
                    'workbook_name': workbook_name,
                    'workbook_owner': workbook_owner,
                    'last_modified': last_modified,
                    'view_name': view_name,
                    'project_name': project_name,
                    'table_name': table_name,
                    'db_name': db_name,
                    'database.connectionType': db_connection_type
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
        has_extracts = datasource['hasExtracts']
        datasource_url = f"{server_url}/#/site/{site_name}/datasources/{datasource['id']}"  # Construct datasource URL
        datasource_hyperlink = f'=HYPERLINK("{datasource_url}", "Click Here")'  # Excel hyperlink formula
        for table in datasource['upstreamTables']:
            table_name = table['name']
            db_name = table['database']['name']
            db_connection_type = table['database']['connectionType']
            # Downstream workbooks using this data source
            for workbook in datasource['downstreamWorkbooks']:
                workbook_name = workbook['name']
                for view in workbook['views']:  # Separate row for each view
                    view_name = view['name']
                    records.append({
                        'site_name': site_name,
                        #'datasource_url': datasource_hyperlink,  # Use Excel hyperlink formula
                        'datasource_name': datasource_name,
                        'datasource_owner': datasource_owner,
                        'workbook_name': workbook_name,  # Workbook using this data source
                        'view_name': view_name,
                        'project_name': project_name,
                        'table_name': table_name,
                        'db_name': db_name,
                        'database.connectionType': db_connection_type,
                        'last_modified': last_modified,
                        'has_extracts': has_extracts
                    })
    return pd.DataFrame(records)


def process_site_data(conn, site_name, site_id, server_url):
    # Switch to the current site
    conn.switch_site(site_id)

    # Query metadata for workbook and data source tables
    print(f"Querying data from site '{site_name}'...")
    wb_query_results_json = get_metadata_json(conn, query_workbooks_tables, 'workbooks')
    ds_query_results_json = get_metadata_json(conn, query_datasources_tables, 'publishedDatasources')

    # Process workbooks metadata and create Excel file for the current site
    workbook_metadata_df = get_workbook_metadata(wb_query_results_json, site_name, server_url)
    workbook_metadata_df.to_excel(f'{site_name}_workbook_metadata.xlsx', index=False)
    print(f"Workbook metadata saved for site '{site_name}'")

    # Process data sources metadata and create Excel file for the current site
    datasource_metadata_df = get_datasource_metadata(ds_query_results_json, site_name, server_url)
    datasource_metadata_df.to_excel(f'{site_name}_datasource_metadata.xlsx', index=False)
    print(f"Data source metadata saved for site '{site_name}'")


def main():
    # Establish connection
    conn = TableauServerConnection(tableau_server_config, 'my_env')
    conn.sign_in()

    server_url = tableau_server_config['my_env']['server']

    # Get a list of all sites on the Tableau Server
    sites_df = get_sites_dataframe(conn)

    # Iterate through all sites and gather data for each site
    for index, site in sites_df.iterrows():
        site_name = site['name']
        site_id = site['contentUrl']
        process_site_data(conn, site_name, site_id, server_url)

    conn.sign_out()


if __name__ == "__main__":
    main()