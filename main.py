import pandas as pd
import numpy as np

def transform_csv(input_file, output_file):
    # Read the main data, skipping header rows
    df = pd.read_csv(input_file, skiprows=4)
    
    # Create dataframes - one for each metric
    dataframes = []
    
    # Base columns to copy for all dataframes
    base_columns = [
        'Year', 
        'ISO Code 2', 
        'ISO Code 3', 
        'Countries', 
        ' Economic Freedom Summary Index',
        'Rank', 
        'Quartile'
    ]
    
    # Create the different metric dataframes
    metrics = [
        ('1E', 'State ownership of Assets', 'State ownership of Assets'),
        ('1B', 'Transfers and subsidies', 'Transfers and subsidies'),
        ('1C', 'Government investment', 'Government investment'),
        ('5Cvi', 'Tax compliance', 'Tax compliance'),
        ('1D', 'Top marginal income tax rate', 'S'),
        ('Area1', 'Size of Government', 'V'),
        ('N', 'Economic Freedom Summary Index', ' Economic Freedom Summary Index'),
        ('N', 'Rank', 'Rank'),
        ('N', 'Quartile', 'Quartile')
    ]
    
    for research_id, research_name, value_column in metrics:
        # Create a new dataframe with base columns
        temp_df = df[base_columns].copy()
        
        # Clean Year column
        temp_df['Year'] = pd.to_numeric(temp_df['Year'], errors='coerce')
        temp_df = temp_df.dropna(subset=['Year'])
        
        # Add research columns
        temp_df['Research ID'] = research_id
        temp_df['Research'] = research_name
        # Handle both named columns and letter-based columns
        if len(value_column) == 1:  # If it's a single letter column reference
            temp_df['Index - Continuous -F'] = df.iloc[:, ord(value_column) - ord('A')]
        else:
            temp_df['Index - Continuous -F'] = df[value_column]
        
        # Add to list of dataframes
        dataframes.append(temp_df)
    
    # Combine all dataframes
    new_df = pd.concat(dataframes, ignore_index=True)
    
    # Clean up the combined dataframe
    new_df = new_df.dropna(subset=['Year', 'Index - Continuous -F'])
    new_df['Year'] = new_df['Year'].astype(int)
    
    # Reorder columns to match desired output
    column_order = [
        'Year',
        'ISO Code 2',
        'ISO Code 3',
        'Countries',
        ' Economic Freedom Summary Index',
        'Rank',
        'Quartile',
        'Research ID',
        'Research',
        'Index - Continuous -F'
    ]
    new_df = new_df[column_order]
    
    # Sort by Year, Country, and Research ID
    new_df = new_df.sort_values(['Year', 'Countries', 'Research ID'])
    
    # Save to new CSV file
    new_df.to_csv(output_file, index=False)
    
    return new_df

# Usage
input_file = 'csvsample.csv'
output_file = 'transformed_data.csv'
result_df = transform_csv(input_file, output_file)

# save the first 1000 rows of the transformed data
result_df.head(1000).to_csv('transformed_data_1000.csv', index=False)

print("\nTransformed data:")
print(result_df.head())
