# PostgreSQL to Excel Report Generator

This Python script connects to a PostgreSQL database and generates an Excel report with detailed information about tables, schemas, and geometry types.

## Features
- Extracts data about schemas and tables in a PostgreSQL database.
- Generates a report in Excel format with customizable formatting.
- Highlights rows based on specific conditions.

## Overview
- Creates an Excel report from a PostgreSQL database with table composition and geometry types.
- Allows specifying schemas (`schm_list`) and geometry field names to check (`geom_fieldnames`).
- Generates a report including the following fields:
  - #, schema_name, schema_description, class_name, class_description, geom_fieldname, geom_type, primary_key, unique_key, foreign_key, records_number, columns_number.
- Includes an additional sheet that lists classes with their attributes.
- Custom formatting and highlighting:
  - Rows are highlighted in red if 'records_number' contains specific error messages: "Table Not Found", "Access Denied/Not a Table", or "Transaction Error".
  - Rows are highlighted in green in the "classes attributes" sheet if the table contains a geometry column.

## Prerequisites
- Python 3.x
- PostgreSQL database
- Required 'psycopg2' and 'openpyxl' Python libraries (see `requirements.txt`)

## Installation
Clone the repository and install dependencies:
```bash
git clone https://github.com/yourusername/your-repo-name.git
cd your-repo-name
pip install -r requirements.txt
