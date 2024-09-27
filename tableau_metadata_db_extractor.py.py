# Use this code to get the details workbooks and data sources metadata
# which are using HANA database from all sites. Update the filter filter: {connectionType: "saphana" for any other specific database type.

import pandas as pd
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_sites_dataframe
import time
import logging

# Configure logging
logging.basicConfig(filename='logs.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

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

# GraphQL query to fetch workbook details including pagination
query_workbooks_tables = """
{
  workbooksConnection(first: 100, after: {cursor}) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
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
      upstreamTables(filter: {connectionType: "saphana"}) {
        name
        database {
          name
          connectionType
        }
      }
    }
  }
}
"""

# GraphQL query to fetch data source details including pagination
query_datasources_tables = """
{
  publishedDatasourcesConnection(first: 100, after: {cursor}) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      name
      id
      projectName
      owner {
        username
      }
      updatedAt
      hasExtracts
      upstreamTables(filter: {connectionType: "saphana"}) {
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
}
"""

# Function to fetch metadata with pagination and error handling
def get_metadata_json(conn, query, content_type, handle_pagination=True, retries=3):
    all_records = []
    cursor = None  # Initial cursor set to None for the first request
    has_next_page = True

    while has_next_page:
        try:
            # Replace {cursor} in the query with the actual cursor value, if present
            if cursor:
                paginated_query = query.replace("{cursor}", f'"{cursor}"')  # Use cursor for subsequent pages
            else:
                # Remove 'after' clause if cursor is None for the first request
                paginated_query = query.replace('after: {cursor}', '')  # First request without 'after'

            # Log the full query being sent
            logging.info(f"Executing query:\n{paginated_query}")

            # Execute the GraphQL query
            results = conn.metadata_graphql_query(paginated_query)

            # Check if response exists and is not empty
            if results is None or results.text.strip() == '':
                logging.error("Error: Empty response from server.")
                return None

            # Log status code and headers for further diagnosis
            logging.info(f"Status code: {results.status_code}")
            logging.info(f"Response headers: {results.headers}")

            # Attempt to parse the JSON response
            try:
                results_json = results.json()
                logging.info(f"Response Data: {results_json}")
                print(f"Response Data: {results_json}")
            except ValueError as e:
                logging.error(f"Error: Failed to parse JSON. Response text: {results.text}")
                return None

            # Ensure the expected data exists in the response
            if 'data' not in results_json or content_type not in results_json['data']:
                logging.error(f"Error: The expected data '{content_type}' was not found in the response.")
                return None

            # Extract the actual data (nodes)
            records = results_json['data'][content_type]['nodes']
            all_records.extend(records)  # Append the records from this page

            # Handle pagination using pageInfo (only if pagination is enabled)
            if handle_pagination:
                if 'pageInfo' in results_json['data'][content_type]:
                    page_info = results_json['data'][content_type]['pageInfo']
                    has_next_page = page_info['hasNextPage']
                    cursor = page_info['endCursor']  # Set cursor for the next page
                else:
                    # If pageInfo is not in the response, stop pagination
                    has_next_page = False
            else:
                # If pagination is not enabled, stop after the first request
                has_next_page = False

        except Exception as e:
            logging.error(f"Error while executing GraphQL query: {e}")
            retries -= 1
            if retries > 0:
                logging.info(f"Retrying... {retries} attempts left")
                time.sleep(2)  # Add a small delay before retrying
                continue  # Retry the request
            else:
                return None

    return all_records


# Function to process workbook metadata, filtering for SAP HANA database
def get_workbook_metadata(json_data, site_name, server_url):
    records = []
    for workbook in json_data:
        # Handle all possible missing fields with default values
        workbook_name = workbook.get('name', 'Unknown')
        project_name = workbook.get('projectName', 'Unknown')
        workbook_owner = workbook['owner']['username'] if workbook.get('owner') and workbook['owner'].get(
            'username') else 'Unknown'
        last_modified = workbook.get('updatedAt', 'Unknown')

        # Construct workbook URL and hyperlink with fallback
        workbook_id = workbook.get('id', 'Unknown')
        workbook_url = f"{server_url}/#/site/{site_name}/workbooks/{workbook_id}"
        workbook_hyperlink = f'=HYPERLINK("{workbook_url}", "Click Here")'

        views = workbook.get('views', [])
        for view in views:  # Separate row for each view
            view_name = view.get('name', 'Unknown')

            # Handle missing or empty upstreamTables
            upstream_tables = workbook.get('upstreamTables', [])
            for table in upstream_tables:
                table_name = table.get('name', 'Unknown')

                # Handle missing database field and nested attributes
                db_info = table.get('database', {})
                db_name = db_info.get('name', 'Unknown')
                db_connection_type = db_info.get('connectionType', 'Unknown')

                # Filter for SAP HANA databases (saphana)
                if db_connection_type == 'saphana':
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
                        'db_connectionType': db_connection_type
                    })
    return pd.DataFrame(records)


# Function to process data source metadata, filtering for SAP HANA database
def get_datasource_metadata(json_data, site_name, server_url):
    records = []
    for datasource in json_data:
        # Handle all possible missing fields with default values
        datasource_name = datasource.get('name', 'Unknown')
        project_name = datasource.get('projectName', 'Unknown')
        datasource_owner = datasource['owner']['username'] if datasource.get('owner') and datasource['owner'].get(
            'username') else 'Unknown'
        last_modified = datasource.get('updatedAt', 'Unknown')
        has_extracts = datasource.get('hasExtracts', False)

        # Construct datasource URL and hyperlink with fallback
        datasource_id = datasource.get('id', 'Unknown')
        datasource_url = f"{server_url}/#/site/{site_name}/datasources/{datasource_id}"
        datasource_hyperlink = f'=HYPERLINK("{datasource_url}", "Click Here")'

        # Handle missing or empty upstreamTables
        upstream_tables = datasource.get('upstreamTables', [])
        for table in upstream_tables:
            table_name = table.get('name', 'Unknown')

            # Handle missing database field and nested attributes
            db_info = table.get('database', {})
            db_name = db_info.get('name', 'Unknown')
            db_connection_type = db_info.get('connectionType', 'Unknown')

            # Filter for SAP HANA databases (saphana)
            if db_connection_type == 'saphana':
                # Handle missing downstreamWorkbooks
                downstream_workbooks = datasource.get('downstreamWorkbooks', [])
                for workbook in downstream_workbooks:
                    workbook_name = workbook.get('name', 'Unknown')

                    # Handle missing views in downstream workbooks
                    views = workbook.get('views', [])
                    for view in views:
                        view_name = view.get('name', 'Unknown')
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
                            'db_connectionType': db_connection_type,
                            'last_modified': last_modified,
                            'has_extracts': has_extracts
                        })
    return pd.DataFrame(records)


# Function to process site data
def process_site_data(conn, site_name, site_id, server_url, sites_without_saphana):
    # Switch to the current site
    conn.switch_site(site_id)

    # Query paginated workbook data
    logging.info(f"Querying data from site '{site_name}'...")

    wb_query_results_json = get_metadata_json(conn, query_workbooks_tables, 'workbooksConnection')
    if wb_query_results_json is None:
        logging.info(f"Skipping site '{site_name}' due to error in fetching workbook data.")
        return

    # Process workbooks metadata
    workbook_metadata_df = get_workbook_metadata(wb_query_results_json, site_name, server_url)

    # Write workbook metadata to Excel if SAP HANA data is found
    if not workbook_metadata_df.empty:
        workbook_filename = f'{site_name}_workbook_metadata.xlsx'
        workbook_metadata_df.to_excel(workbook_filename, index=False)
        logging.info(f"Workbook metadata with SAP HANA details saved for site '{site_name}' to '{workbook_filename}'")
    else:
        sites_without_saphana.append(site_name)
        logging.info(f"No SAP HANA details found in workbooks for site '{site_name}'.")

    # Query paginated data source data
    ds_query_results_json = get_metadata_json(conn, query_datasources_tables, 'publishedDatasourcesConnection')
    if ds_query_results_json is None:
        logging.info(f"Skipping site '{site_name}' due to error in fetching datasource data.")
        return

    # Process data sources metadata
    datasource_metadata_df = get_datasource_metadata(ds_query_results_json, site_name, server_url)
    # Write datasource metadata to Excel if SAP HANA data is found
    if not datasource_metadata_df.empty:
        datasource_filename = f'{site_name}_datasource_metadata.xlsx'
        datasource_metadata_df.to_excel(datasource_filename, index=False)
        logging.info(
            f"Data source metadata with SAP HANA details saved for site '{site_name}' to '{datasource_filename}'")
    else:
        sites_without_saphana.append(site_name)
        logging.info(f"No SAP HANA details found in data sources for site '{site_name}'.")

def main():
    # List to track sites without SAP HANA data
    sites_without_saphana = []

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
        process_site_data(conn, site_name, site_id, server_url, sites_without_saphana)

    conn.sign_out()

    # After processing all sites, print the sites without SAP HANA details
    if sites_without_saphana:
        logging.info("Sites where SAP HANA type was not found:")
        for site in sites_without_saphana:
            logging.info(f"- {site}")

        # Optional: write to a file
        with open("sites_without_saphana.txt", "w") as file:
            for site in sites_without_saphana:
                file.write(f"{site}\n")
    else:
        logging.info("All sites had SAP HANA data.")

if __name__ == "__main__":
    main()
