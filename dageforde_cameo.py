# Develop a pipeline that cleans up the dataset listed below and "deliver it" to a target schema. For purposes of the exercise, the target schema can simply be separate csv files.

#Dataset: Dataset: https://data.cityofchicago.org/Health-Human-Services/Food-Inspections/4ijn-s7e5/data

# Minimum Requirements
# Use a language of your choice to develop a script that reads the dataset and pushes an output to separate csv files.
# Create 2 output tables:
# Table A includes a list of all inspections
# Table B includes a list of all violations and comments associated with each inspection

import pandas as pd
import numpy as np
from sodapy import Socrata

#I'll call this API specifying inspections with IDs >= 0, assuming the numbering system begins with 0. There are currently just under 200,000 observations dating back to January 1, 2010. I'll set the limit to 1,000,000. Assuming inspections continue at the same rate, this limit allows me to collect all observations for another 4 years. 
client = Socrata('data.cityofchicago.org', None)
res = client.get('4ijn-s7e5', where='inspection_id>=0', limit=1000000)
df = pd.DataFrame.from_records(res)

#Considering the specificity & gravity of the data here, I don't want to impute missing data; nor do I want to drop observations. I will chalk this up to human error and replace NaNs with 'No Entry'
df.fillna('No Entry', inplace=True)

#return a dataframe that includes 
table_a = df[['dba_name', 'inspection_id', 'inspection_date', 'inspection_type']]
table_a.to_csv('table_a.csv', index=False)

#
vio_df = df.violations.apply(lambda x: pd.Series(str(x).split('|')))
vio_df.fillna('No Entry', inplace=True)
inspection = df[['dba_name', 'inspection_id',]]
table_b = pd.concat([inspection, vio_df], axis=1)
table_b.to_csv('table_b.csv', index=False)