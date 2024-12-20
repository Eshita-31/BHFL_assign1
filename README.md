# BHFL A1 FastAPI CRUD Application

This project is a FastAPI-based Python application that performs CRUD (Create, Read, Update, Delete) operations on data from Excel sheets. It uses pandas for data handling, pydantic for data validation, and loguru for logging. The goal is to learn EDA, data cleaning, REST API endpoints, and exporting processed data back to Excel.

## Features
- Load `Accounts`, `Policies`, and `Claims` from Excel.
- Perform CRUD operations on customers, policies, and claims.
- Track changes in a `history_df` DataFrame.
- Export updated data and history to an Excel file with multiple sheets.
- Validations and error handling with proper HTTP status codes.
- Logging using loguru.

## Prerequisites
- Python 3.x installed.
- `pip install -r requirements.txt` (if you have a `requirements.txt` file).
  If not, install individually:  
  `pip install fastapi uvicorn loguru pandas openpyxl pydantic`

## Running the Application
1. Start the server:
   ```bash
   python main.py

2. Open your browser at http://127.0.0.1:8000/ to see the root message.
3. Explore the interactive docs at http://127.0.0.1:8000/docs for available endpoints.

## Using the API (via Postman or similar tool)
Get Customer: GET http://127.0.0.1:8000/customers/{account_id}
Create Customer: POST http://127.0.0.1:8000/customers/
Update Customer: PUT http://127.0.0.1:8000/customers/{account_id}
Delete Customer: DELETE http://127.0.0.1:8000/customers/{account_id}
(Similar endpoints exist for /policies and /claims.)

## Exporting Data
After performing some operations, call:
  ```bash 
GET http://127.0.0.1:8000/export/







