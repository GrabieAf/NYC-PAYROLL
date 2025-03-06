#Import necessary libraries
import pandas as pd
import os
import io
from azure.storage.blob import BlobServiceClient, BlobClient
from dotenv import load_dotenv

#Data Extraction Layer
nyc2020_df = pd.read_csv(r"datasets\nycpayroll_2020.csv")
nyc2021_df = pd.read_csv(r"datasets\nycpayroll_2021.csv")

#Checking id the columns mismatch and merging both into a dataset
set_2020 = set(nyc2020_df.columns)
set_2021 = set(nyc2021_df.columns)

if set_2020 != set_2021:
    print("Columns do not match! Adjust before merging.")
    print("Extra columns in 2020:", set_2020 - set_2021)
    print("Extra columns in 2021:", set_2021 - set_2020)
else:
    print("Columns match, ready to merge.")

# Merge datasets
Mergedpayroll_df = pd.concat([nyc2020_df, nyc2021_df], ignore_index=True)
print(f"Combined dataset has {Mergedpayroll_df.shape[0]} rows and {Mergedpayroll_df.shape[1]} columns.")

# Ensure both dataframes have the same columns
all_columns = set(nyc2020_df.columns).union(set(nyc2021_df.columns))

# Adding missing columns to each dataframe with NaN values
for col in all_columns:
    if col not in nyc2020_df.columns:
        nyc2020_df[col] = None  # Add missing column in 2020
    if col not in nyc2021_df.columns:
        nyc2021_df[col] = None  # Add missing column in 2021

# Reorder columns to match before merging
nyc2020_df = nyc2020_df[sorted(all_columns)]
nyc2021_df = nyc2021_df[sorted(all_columns)]

# Merge datasets
Mergedpayroll_df = pd.concat([nyc2020_df, nyc2021_df], ignore_index=True)

# Confirm the merge
print(f"✅ Merged dataset has {Mergedpayroll_df.shape[0]} rows and {Mergedpayroll_df.shape[1]} columns.")

#Filling two columns 'AgencyID' and 'AgencyCode' having  null values for a clean dataset
Mergedpayroll_df[['AgencyID', 'AgencyCode']] = Mergedpayroll_df[['AgencyID', 'AgencyCode']].fillna(0.0)

#Convert the column ti datatime datatype
Mergedpayroll_df['AgencyStartDate'] = pd.to_datetime(Mergedpayroll_df['AgencyStartDate'])

#Agency table
agency = Mergedpayroll_df[['AgencyID','AgencyName','AgencyCode','AgencyStartDate']].copy().drop_duplicates().reset_index(drop=True)

#employee table
employee = Mergedpayroll_df[['EmployeeID','LastName', 'FirstName','TitleCode','TitleDescription']].copy().drop_duplicates().reset_index(drop=True)

#facts table
facts_table = Mergedpayroll_df[['EmployeeID','AgencyID','FiscalYear','BaseSalary','RegularHours', 'RegularGrossPaid', 'OTHours',
                                'TotalOTPaid', 'TotalOtherPay','PayBasis','PayrollNumber','WorkLocationBorough','LeaveStatusasofJune30']].copy().drop_duplicates().reset_index(drop=True)


#Converting to csv temporarily
agency.to_csv(r'datasets/agency.csv', index=False)
employee.to_csv(r'datasets/employee.csv', index=False)
facts_table.to_csv(r'datasets/facts_table.csv', index=False)

print('Files have been loaded temporarily into local machine')


#Data Loading
#Azure Blob Connection
load_dotenv()
connect_str = os.getenv('CONNECT_STR')
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container_name = os.getenv('CONTAINER_NAME')
container_client = blob_service_client.get_container_client(container_name)


#create a function to load data into Azure blog storage as a parquet file

def upload_df_to_blob_as_parquet(df, container_client,blob_name):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(buffer, blob_type="BlockBlob", overwrite=True)
    print (f'{blob_name} uploaded to Blob storage successfully')

#Loading files in parquet format to azure cloud storage
upload_df_to_blob_as_parquet(agency, container_client, 'rawdataset/agency.parquet')
upload_df_to_blob_as_parquet(employee, container_client, 'rawdataset/employee.parquet')
upload_df_to_blob_as_parquet(facts_table, container_client, 'rawdataset/facts_table.parquet')
