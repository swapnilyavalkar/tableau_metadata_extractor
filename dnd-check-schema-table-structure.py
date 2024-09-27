import json
from tableau_api_lib import TableauServerConnection

# Tableau server connection config
tableau_server_config = {
    'my_env': {
        'server': 'https://10ax.online.tableau.com',
        'api_version': '3.8',
        'personal_access_token_name': '<PAT NAME>',
        'personal_access_token_secret': '<PAT SECRET>',
        'site_name': 'abc',
        'site_url': 'abc'
    }
}

# GraphQL query to retrieve the schema of the PublishedDatasource object
query_schema_published_datasources = """
{
  __type(name: "PublishedDatasource") {
    name
    fields {
      name
      type {
        name
      }
    }
  }
}
"""


# Fetch metadata from Tableau Server
def get_published_datasource_fields(conn):
    results = conn.metadata_graphql_query(query=query_schema_published_datasources)
    schema_data = results.json()['data']['__type']['fields']
    return schema_data


def main():
    # Establish connection
    conn = TableauServerConnection(tableau_server_config, 'my_env')
    conn.sign_in()

    # Query and print the available fields for the PublishedDatasource object
    fields = get_published_datasource_fields(conn)
    print(json.dumps(fields, indent=2))  # Pretty print the available fields

    conn.sign_out()


if __name__ == "__main__":
    main()
