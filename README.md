# Tableau Metadata Extractor

This repository contains a set of Python scripts designed to extract metadata from Tableau workbooks and data sources. The scripts use the Tableau GraphQL API to retrieve detailed information from Tableau Server, including workbooks, data sources, tables, and other related metadata.

### Files:
1. **Code 1**: `tableau_metadata_db_extractor.py`
2. **Code 2**: `tableau_metadata_basic_extractor.py`
3. **Code 3**: `tableau_metadata_detailed_extractor.py`

## Features

- Fetch metadata for Tableau workbooks and data sources from multiple sites.
- Filter results for SAP HANA or any other database by adjusting the connection type.
- Handle pagination and large datasets.
- Output metadata to Excel files for easy review and analysis.

## Prerequisites

- A Personal Access Token (PAT) for authentication with Tableau Server.
- Python 3.x installed with required dependencies.

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/tableau_metadata_extractor.git
   cd tableau_metadata_extractor
   ```

2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Update the `tableau_server_config` in each script with your Tableau Server details, including:
   - `server`: Tableau Server URL.
   - `personal_access_token_name`: Your Personal Access Token name.
   - `personal_access_token_secret`: Your Personal Access Token secret.
   - `site_name`: The name of the Tableau site to extract data from.

## Scripts Overview

### 1. `tableau_metadata_db_extractor.py`
This script is used to extract metadata specifically related to SAP HANA databases. It queries all sites on your Tableau Server and filters the upstream tables for those using the "saphana" connection type. The metadata, including workbook names, project names, owners, and view details, is saved in Excel files. You can update the filter to use this script for any other database types. 

**Usage:**
```bash
python tableau_metadata_db_extractor.py
```

### 2. `tableau_metadata_basic_extractor.py`
This script retrieves metadata for all Tableau workbooks and data sources across sites without focusing on a specific database. It provides details such as workbook names, views, and data source information, and saves the data to Excel files.

**Usage:**
```bash
python tableau_metadata_basic_extractor.py
```

### 3. `tableau_metadata_detailed_extractor.py`
This script extracts detailed metadata for both workbooks and data sources, including information about upstream data sources, whether a data source uses extracts, and whether it contains custom SQL. The extracted metadata is saved to detailed Excel files.

**Usage:**
```bash
python tableau_metadata_detailed_extractor.py
```

## Output

Each script will output Excel files containing metadata related to the workbooks and data sources. The files will be named according to the Tableau site being queried (e.g., `site_workbook_metadata.xlsx`, `site_datasource_metadata.xlsx`).

## Notes

- Ensure that your Tableau Personal Access Token (PAT) has the necessary permissions to access all sites.
- You can adjust the database connection type filter in the scripts if you are interested in extracting metadata for other databases besides SAP HANA.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

---

## Contributing

Contributions to this project are always welcome! Whether it's reporting bugs, suggesting new features, or submitting improvements, your input is valuable.

If you would like to contribute, feel free to fork the repository, make your changes, and submit a pull request. We appreciate all contributions and will review them as quickly as possible.

Thank you for your interest in improving this project!