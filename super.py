import pandas as pd
import numpy as np
import pycountry
import country_converter as coco
from typing import Tuple
import pycountry_convert as pc
import tkinter as tk
from tkinter import messagebox

def transform_csv(input_file, output_file):
    # Load both CSV files, skipping initial empty rows, and round all floats
    df_areas = pd.read_csv('areas.csv').round(2)
    df_metrics = pd.read_csv('csvsample.csv', skiprows=4).round(2)
    
    # Clean up the areas dataframe column names
    df_areas.columns = df_areas.columns.str.strip()

    df_metrics['Year'] = pd.to_numeric(df_metrics['Year'], errors='coerce')
    df_areas['Year'] = pd.to_numeric(df_areas['Year'], errors='coerce')

    # Merge the dataframes on Year and Country
    df_combined = pd.merge(
        df_metrics,
        df_areas[['ISO_Code', 'Year', 'Area 1', 'Area 2', 'Area 3', 'Area 4', 'Area 5']],
        left_on=['ISO Code 3', 'Year'],
        right_on=['ISO_Code', 'Year'],
        how='left'
    )
        
    def get_regiao_from_subregion(subregion, country):
        """
        Maps World Bank regions to Portuguese continental regions
        """
        if pd.isna(subregion):
            return 'N/A'
        
        subregion = str(subregion).lower()
        country = str(country).lower()
        
        regiao = 'N/A'
        
        # Check main regions first (more common cases)
        if 'east asia' in subregion or 'pacific' in subregion or 'south asia' in subregion:
            regiao = 'Ásia'
        elif 'europe' in subregion:
            regiao = 'Europa'
        elif 'latin america' in subregion or 'north america' in subregion or 'caribbean' in subregion:
            regiao = 'América'
        elif 'sub-saharan africa' in subregion or 'middle east' in subregion or 'north africa' in subregion:
            regiao = 'África'
        
        # Check Oceania last (less common case)
        oceania_countries = {
            'australia', 'new zealand', 'fiji', 'papua new guinea', 
            'solomon islands', 'vanuatu', 'new caledonia', 'french polynesia',
            'samoa', 'tonga', 'micronesia', 'kiribati', 'palau', 'marshall islands',
            'nauru', 'tuvalu', 'cook islands', 'niue', 'tokelau', 'wallis and futuna',
            'american samoa', 'guam', 'northern mariana islands'
        }
        
        if any(c in country for c in oceania_countries):
            regiao = 'Oceania'
        
        return regiao

    df = df_combined
    df['Subregião / Subregion'] = df['World Bank Region']
    df['Quartile'] = df['Quartile']
    df['Quartil - Eco Free'] = df['World Bank Current Income Classification, 1990-Present']
    df['Rank - World'] = df['Rank']
    df['Rank'] = df['Rank']
    df['Regiao / Region'] = df.apply(lambda x: get_regiao_from_subregion(x['Subregião / Subregion'], x['Countries']), axis=1)
    df['Language1'] = 'English'
    df['State'] = np.nan
    
    # Base columns that will be used for each research metric
    base_columns = [
        'Year',
        'Language1',
        'Regiao / Region',
        'Subregião / Subregion',
        'Countries',
        'State',
        ' Economic Freedom Summary Index',
        'Rank', 
        'Quartile',
        'Quartil - Eco Free',
        'Rank - World'
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
    
    new_df['Quartiles'] = df['World Bank Current Income Classification, 1990-Present']

    new_df['Area'] = 'N/A'
    new_df['Quartil / Quartile'] = 'N/A'
    new_df['Área / Area'] = 'N/A'
    
    new_df['indexValue - Continuous'] = new_df['indexValue - Continuous'].round(2)
    new_df['Indice / Index - Discrete'] = new_df['indexValue - Continuous']
    
    # Update the final column order
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
        'Quartil - Eco Free',
        'Rank - World',
        'Quartile',
        'Rank',
        'Quartil / Quartile',
        'Área / Area',
        'indexValue - Continuous'
    ]
    
    # Reorder
    new_df = new_df[column_order]
    
    # Sort using the new column names
    new_df = new_df.sort_values(['Ano/Year', 'País / Country', 'Research Code'])
    
    # Save to new CSV file
    new_df.to_csv(output_file, index=False)
    
    return new_df

# Define function to determine area based on Research Code
def get_area(research_code):
    if pd.isna(research_code):
        return 'Economic Freedom Summary Index'
    
    research_code = str(research_code)
    
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

class TransformationGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Data Transformation Tool")
        
        # Set window size and position it in the center
        window_width = 500
        window_height = 500
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()
        center_x = int(screen_width/2 - window_width/2)
        center_y = int(screen_height/2 - window_height/2)
        
        self.root.geometry(f'{window_width}x{window_height}+{center_x}+{center_y}')
        
        # Create main frame
        main_frame = tk.Frame(root, padx=20, pady=20)
        main_frame.pack(expand=True)
        
        step1_label = tk.Label(main_frame,
                             text="Passo 1: Abra o site economic freedom e baixe o xlsx",
                             font=("Arial", 10),
                             wraplength=400)
        step1_label.pack(pady=10)
        
        step2_label = tk.Label(main_frame,
                             text="Passo 2: Selecione o arquivo no selecionador abaixo",
                             font=("Arial", 10),
                             wraplength=400)
        step2_label.pack(pady=5)
        
        # File selection frame
        file_frame = tk.Frame(main_frame)
        file_frame.pack(pady=5)
        
        self.file_entry = tk.Entry(file_frame, width=50)
        self.file_entry.pack(side=tk.LEFT, padx=5)
        
        browse_button = tk.Button(file_frame, 
                                text="Procurar", 
                                command=self.select_file,
                                font=("Arial", 10))
        browse_button.pack(side=tk.LEFT)
        
        step3_label = tk.Label(main_frame,
                             text="Passo 3: Clique no botão para converter",
                             font=("Arial", 10),
                             wraplength=400)
        step3_label.pack(pady=5)
        
        self.convert_button = tk.Button(main_frame,
                                      text="Converter para CSV",
                                      command=self.convert_to_csv,
                                      font=("Arial", 10),
                                      width=20,
                                      state='disabled')
        self.convert_button.pack(pady=5)
        
        step4_label = tk.Label(main_frame,
                             text="Passo 4: Clique no botão para finalizar",
                             font=("Arial", 10),
                             wraplength=400)
        step4_label.pack(pady=5)
        
        self.transform_button = tk.Button(main_frame,
                                        text="Finalizar Transformação",
                                        command=self.transform_data,
                                        font=("Arial", 10),
                                        width=20,
                                        state='disabled')
        self.transform_button.pack(pady=5)
        
        step5_label = tk.Label(main_frame,
                             text="Passo 5: Abra o tableau e importe o arquivo 'Sheet1' como fonte de dados",
                             font=("Arial", 10),
                             wraplength=400)
        step5_label.pack(pady=10)
        
        self.selected_file_path = None
        
    def select_file(self):
        from tkinter import filedialog
        filetypes = [
            ('Excel files', '*.xlsx'),
            ('Excel files', '*.xls'),
            ('All files', '*.*')
        ]
        filename = filedialog.askopenfilename(
            title='Select Excel File',
            filetypes=filetypes
        )
        if filename:
            self.selected_file_path = filename
            self.file_entry.delete(0, tk.END)
            self.file_entry.insert(0, filename)
            self.convert_button.config(state='normal')
            
    def convert_to_csv(self):
        try:
            self.convert_button.config(state='disabled', text="Convertendo...")
            self.root.update()
            
            # Read Excel files with specific float format
            df_first = pd.read_excel(self.selected_file_path, sheet_name=0)
            df_second = pd.read_excel(self.selected_file_path, sheet_name=1)
            
            # Convert to string first to preserve decimal places
            for col in df_first.select_dtypes(include=['float64']).columns:
                df_first[col] = df_first[col].apply(lambda x: f'{x:.2f}' if pd.notnull(x) else x)
            
            for col in df_second.select_dtypes(include=['float64']).columns:
                df_second[col] = df_second[col].apply(lambda x: f'{x:.2f}' if pd.notnull(x) else x)
            
            df_first.to_csv('csvsample.csv', index=False, float_format='%.2f')
            df_second.to_csv('areas.csv', index=False, float_format='%.2f')
            
            messagebox.showinfo("Sucesso", 
                              "Arquivos convertidos com sucesso!")
            
            # Enable transform button after successful conversion
            self.transform_button.config(state='normal')
            
        except Exception as e:
            messagebox.showerror("Erro", f"Ocorreu um erro durante a conversão:\n{str(e)}")
            
        finally:
            self.convert_button.config(state='normal', text="Converter para CSV")
    
    def transform_data(self):
        try:
            self.transform_button.config(state='disabled', text="Processando...")
            self.root.update()
            
            result_df = transform_csv('csvsample.csv', 'transformed_data.csv')
            
            result_df['Area'] = result_df['Research Code'].apply(get_area)
            
            result_df.to_excel('Sheet1.xlsx', index=False)
            
            # Delete auxiliary files
            import os
            if os.path.exists('csvsample.csv'):
                os.remove('csvsample.csv')
            if os.path.exists('areas.csv'):
                os.remove('areas.csv')
            if os.path.exists('transformed_data.csv'):
                os.remove('transformed_data.csv')
            
            messagebox.showinfo("Sucesso", 
                              "Transformação concluída!\n\n"
                              "Arquivo criado:\n"
                              "- Sheet1.xlsx")
            
        except Exception as e:
            messagebox.showerror("Erro", f"Ocorreu um erro:\n{str(e)}")
        
        finally:
            self.transform_button.config(state='normal', text="Finalizar Transformação")

def main():
    root = tk.Tk()
    app = TransformationGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
