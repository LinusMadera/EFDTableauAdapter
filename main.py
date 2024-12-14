import pandas as pd
import numpy as np
import pycountry
import country_converter as coco
from typing import Tuple
import pycountry_convert as pc

# Load both CSV files, skipping initial empty rows
df_areas = pd.read_csv('areas.csv')
df_metrics = pd.read_csv('csvsample.csv', skiprows=4)  # Skip the first 4 rows to get to the actual headers

# Clean up the areas dataframe column names
df_areas.columns = df_areas.columns.str.strip()

# Ensure we have matching Year and Country columns for merging
df_metrics['Year'] = pd.to_numeric(df_metrics['Year'], errors='coerce')
df_areas['Year'] = pd.to_numeric(df_areas['Year'], errors='coerce')

# Merge the dataframes on Year and Country
# We'll use the ISO_Code for matching since it's more reliable than country names
df_combined = pd.merge(
    df_metrics,
    df_areas[['ISO_Code', 'Year', 'Area 1', 'Area 2', 'Area 3', 'Area 4', 'Area 5']],
    left_on=['ISO Code 3', 'Year'],
    right_on=['ISO_Code', 'Year'],
    how='left'
)

# Now df_combined has both the detailed metrics and all Area values from areas.csv
# We can use df_combined['Area 1'] through df_combined['Area 5'] instead of calculating them from components

def get_country_metadata(iso3_code: str) -> Tuple[str, str, str]:
    """
    Gets region and subregion data using pycountry_convert.
    Returns (region, subregion, state) in Portuguese
    """
    try:
        # Convert ISO3 to ISO2 (pycountry_convert uses ISO2)
        country_2_code = pc.country_alpha3_to_country_alpha2(iso3_code)
        
        # Get continent code and convert to continent name
        continent_code = pc.country_alpha2_to_continent_code(country_2_code)
        region_raw = pc.convert_continent_code_to_continent_name(continent_code)
        
        # Get UN region (we'll use this as subregion)
        subregion = pc.country_alpha2_to_country_region(country_2_code)
        
        # Portuguese translations for continents
        region_translations = {
            'Europe': 'Europa',
            'Asia': 'Ásia',
            'Africa': 'África',
            'North America': 'América do Norte',
            'South America': 'América do Sul',
            'Oceania': 'Oceania',
            'Antarctica': 'Antártida'
        }
        
        return (
            region_translations.get(region_raw, region_raw),
            subregion,
            'Estado'  # Default state value
        )
    except:
        return ('N/A', 'N/A', 'N/A')

def transform_csv(input_file, output_file):
    # Read the main data
    df = df_combined
    
    df['Subregião / Subregion'] = df['World Bank Region']
    
    # Now add other base columns
    df['Language1'] = 'English'
    df['Regiao / Region'] = df['ISO Code 3'].apply(lambda x: get_country_metadata(x)[0])
    df['State'] = df['ISO Code 3'].apply(lambda x: get_country_metadata(x)[2])
    
    # Base columns that will be used for each research metric
    base_columns = [
        'Year',
        'Language1',
        'Regiao / Region',
        'Subregião / Subregion',  # Now this will already have the correct values
        'Countries',
        'State',
        ' Economic Freedom Summary Index',
        'Rank', 
        'Quartile'
    ]
    
    # Create the different metric dataframes
    # Format: (Research ID, Research Name, column_index or name)
    metrics = [
        ('1A', 'Government consumption', 'Government consumption'),
        ('1E', 'State ownership of Assets', 'State ownership of Assets'),
        ('1B', 'Transfers and subsidies', 'Transfers and subsidies'),
        ('1C', 'Government investment', 'Government investment'),
        ('1D', 'Top marginal income tax rate', 'Top marginal income tax rate'),
        ('1Di', 'Top marginal income tax rate', 'Top marginal income tax rate'),
        ('1Dii', 'Top marginal income and payroll tax rate', 'Top marginal income and payroll tax rate'),
        ('2A', 'Judicial independence', 'Judicial independence'),
        ('2B', 'Impartial courts', 'Impartial courts'),
        ('2C', 'Protection of property rights', 'Protection of property rights'),
        ('2D', 'Military interference in rule of law and politics', 'Military interference in rule of law and politics'),
        ('2E', 'Integrity of the legal system', 'Integrity of the legal system'),
        ('2F', 'Legal enforcement of contracts', 'Legal enforcement of contracts'),
        ('2G', 'Regulatory restrictions on the sale of real property', 'Regulatory restrictions on the sale of real property'),
        ('2H', 'Reliability of police', 'Reliability of police'),
        ('3A', 'Money growth', 'Money growth'),
        ('3B', 'Standard deviation of inflation', 'Standard deviation of inflation'),
        ('3C', 'Inflation: Most recent year', 'Inflation: Most recent year'),
        ('3D', 'Freedom to own foreign currency bank accounts', 'Freedom to own foreign currency bank accounts'),
        ('4A', 'Tariffs', 'Tariffs'),
        ('4Ai', 'Revenue from trade taxes of trade sector', 'Revenue from trade taxes (% of trade sector)'),
        ('4Aii', 'Mean tariff rate', 'Mean tariff rate'),
        ('4Aiii', 'Standard deviation of tariff rates', 'Standard deviation of tariff rates'),
        ('4B', 'Regulatory trade barriers', 'Regulatory trade barriers'),
        ('4Bi', 'Non-tariff trade barriers', 'Non-tariff trade barriers'),
        ('4Bii', 'Compliance costs of importing and exporting', 'Compliance costs of importing and exporting'),
        ('4C', 'Black market exchange rates', 'Black market exchange rates'),
        ('4D', 'Financial openness', 'Financial openness'),
        ('4Dii', 'Capital controls', 'Capital controls'),
        ('4Diii', 'Freedom of foreigners to visit', 'Freedom of foreigners to visit'),
        ('5A', 'Credit market regulations', 'Credit market regulations'),
        ('5Ai', 'Ownership of banks', 'Ownership of banks'),
        ('5Aii', 'Private sector credit', 'Private sector credit'),
        ('5B', 'Labor market regulations', 'Labor market regulations'),
        ('5Bi', 'Hiring regulations and minimum wage', 'Hiring regulations and minimum wage'),
        ('5Bii', 'Hiring and firing regulations', 'Hiring and firing regulations'),
        ('5Biii', 'Centralized collective bargaining', 'Centralized collective bargaining'),
        ('5Biv', 'Hours Regulations', 'Hours Regulations'),
        ('5Bv', 'Mandated cost of worker dismissal', 'Mandated cost of worker dismissal'),
        ('5Bvi', 'Conscription', 'Conscription'),
        ('5C', 'Business regulations', 'Business regulations'),
        ('5Cvi', 'Tax compliance', 'Tax compliance'),
        ('Area1', 'Size of Government', 'Area 1'),
        ('Area2', 'Legal System And Property Rights', 'Area 2'),
        ('Area3', 'Sound Money', 'Area 3'),
        ('Area4', 'Freedom to trade internationally', 'Area 4'),
        ('Area5', 'Regulation', 'Area 5'),
        ('5Civ', 'Tax compliance', 74),  # Using column index (76-1 because 0-based)
        ('N', 'Economic Freedom Summary Index', ' Economic Freedom Summary Index'),
        ('N', 'Rank', 'Rank'),
        ('N', 'Quartile', 'Quartile')
    ]
    
    # Create dataframes - one for each metric
    dataframes = []
    
    for research_id, research_name, value_column in metrics:
        # Create a new dataframe with base columns
        temp_df = df[base_columns].copy()
        
        # Clean Year column
        temp_df['Year'] = pd.to_numeric(temp_df['Year'], errors='coerce')
        temp_df = temp_df.dropna(subset=['Year'])
        
        # Add research columns
        temp_df['Research ID'] = research_id
        temp_df['Research'] = research_name
        
        # Handle different types of column references
        if isinstance(value_column, int):  # If it's a column index
            temp_df['Index - Continuous'] = df.iloc[:, value_column]
        elif len(value_column) == 1:  # If it's a single letter column reference
            temp_df['Index - Continuous'] = df.iloc[:, ord(value_column) - ord('A')]
        else:  # If it's a column name
            temp_df['Index - Continuous'] = df[value_column]
        
        # Add to list of dataframes
        dataframes.append(temp_df)
    
    # Combine all dataframes
    new_df = pd.concat(dataframes, ignore_index=True)
    
    # Clean up the combined dataframe
    new_df = new_df.dropna(subset=['Year', 'Index - Continuous'])
    new_df['Year'] = new_df['Year'].astype(int)
    
    # First rename the columns
    new_df = new_df.rename(columns={
        'Year': 'Ano/Year',
        'Countries': 'País / Country',
        'Index - Continuous': 'indexValue - Continuous',
        'Research ID': 'Research Code',
        'Regiao / Region': 'Região / Region',
    })
    
    
    # Add all the other columns
    new_df['Area'] = 'N/A'
    new_df['Quartiles - Eco Free'] = 'N/A'
    new_df['Rank - World'] = 'N/A'
    new_df['Quartile'] = 'N/A'
    new_df['Rank'] = 'N/A'
    new_df['Quartil / Quartile'] = 'N/A'
    new_df['Área / Area'] = 'N/A'
    
    # Add the discrete index column (truncated to 2 decimals)
    new_df['Indice / Index - Discrete'] = new_df['indexValue - Continuous'].round(2)
    
    # Then update the final column order
    column_order = [
        'Language1',
        'Ano/Year',
        'Região / Region',
        'Subregião / Subregion',
        'País / Country',
        'State',
        'Area',
        'Research Code',
        'Research',
        'Indice / Index - Discrete',
        'Quartiles - Eco Free',
        'Rank - World',
        'Quartile',
        'Rank',
        'Quartil / Quartile',
        'Área / Area',
        'indexValue - Continuous'
    ]
    
    # Now reorder
    new_df = new_df[column_order]
    
    # Sort using the new column names
    new_df = new_df.sort_values(['Ano/Year', 'País / Country', 'Research Code'])
    
    # Save to new CSV file
    new_df.to_csv(output_file, index=False)
    
    return new_df

# Usage
input_file = 'csvsample.csv'
output_file = 'transformed_data.csv'
result_df = transform_csv(input_file, output_file)

# Define function to determine area based on Research Code
def get_area(research_code):
    if pd.isna(research_code):
        return 'Economic Freedom Summary Index'
    
    research_code = str(research_code)  # Convert to string to ensure string methods work
    
    # Look for any occurrence of these numbers in the code
    area_mapping = {
        '1': 'Size of Government',
        '2': 'Legal System & Property Rights',
        '3': 'Sound Money',
        '4': 'Freedom to trade internationally',
        '5': 'Regulation'
    }
    
    # Check for any of the numbers anywhere in the code
    for number, area in area_mapping.items():
        if number in research_code:
            return area
    
    return 'Economic Freedom Summary Index'

# Apply the area rules
result_df['Area'] = result_df['Research Code'].apply(get_area)

# Save the final transformed data
# result_df.to_csv('transformed_data_with_areas.csv', index=False)

# save the first 1000 rows of the transformed data
result_df.head(1000).to_csv('transformed_data_Leite.csv', index=False)

# Print verification info
print("\nTransformed data:")
print(result_df.head())
print("\nUnique areas in the dataset:")
print(result_df['Area'].unique())
print("\nSample of the Research Code and Area mapping:")
print(result_df[['Research Code', 'Area']].head(10))
