import pandas as pd

# Read the second sheet of the Excel file
df = pd.read_excel('efotw-2024-master-index-data-for-researchers-iso.xlsx', sheet_name=1)

# Round all numeric columns to 2 decimal places
df = df.round(2)

# Export to CSV
df.to_csv('areas.csv', index=False)