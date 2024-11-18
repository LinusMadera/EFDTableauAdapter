import pandas as pd
import pyodbc

def create_connection(host, user, password, database="master"):  # Default to master database
    try:
        # Create a connection string for SQL Server that works on Fedora
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={host},1433;"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            "TrustServerCertificate=yes;"
        )
        connection = pyodbc.connect(conn_str)
        
        # If connecting to master and sqlserver database is needed, create it
        if database == "master":
            cursor = connection.cursor()
            # Check if database exists
            cursor.execute("SELECT name FROM sys.databases WHERE name = 'sqlserver'")
            if not cursor.fetchone():
                # Set autocommit to True for CREATE DATABASE
                connection.autocommit = True
                cursor.execute("CREATE DATABASE sqlserver")
                connection.autocommit = False
                
                # Create the necessary tables
                connection.close()
                connection = create_connection(host, user, password, "sqlserver")
                cursor = connection.cursor()
                
                # Create tables
                cursor.execute("""
                    CREATE TABLE Language (
                        Id INT PRIMARY KEY,
                        language NVARCHAR(100)
                    )
                """)
                cursor.execute("""
                    CREATE TABLE LanguageC (
                        Id INT PRIMARY KEY,
                        language NVARCHAR(100)
                    )
                """)
                cursor.execute("""
                    CREATE TABLE LanguageR (
                        Id INT PRIMARY KEY,
                        language NVARCHAR(100)
                    )
                """)
                cursor.execute("""
                    CREATE TABLE LanguageSt (
                        Id INT PRIMARY KEY,
                        language NVARCHAR(100)
                    )
                """)
                cursor.execute("""
                    CREATE TABLE LanguageSub (
                        Id INT PRIMARY KEY,
                        language NVARCHAR(100)
                    )
                """)
                cursor.execute("""
                    CREATE TABLE Area (
                        Id INT PRIMARY KEY,
                        area NVARCHAR(100)
                    )
                """)
                cursor.execute("""
                    CREATE TABLE Year (
                        Id INT PRIMARY KEY,
                        year INT
                    )
                """)
                cursor.execute("""
                    CREATE TABLE Region (
                        Id INT PRIMARY KEY,
                        region NVARCHAR(100)
                    )
                """)
                cursor.execute("""
                    CREATE TABLE LanguageRRegion (
                        Id INT PRIMARY KEY,
                        LanguageRId INT,
                        RegionId INT,
                        RegionTranslated NVARCHAR(200)
                    )
                """)
                cursor.execute("""
                    CREATE TABLE Country (
                        Id INT PRIMARY KEY,
                        ISOCode NVARCHAR(10),
                        country NVARCHAR(100)
                    )
                """)
                cursor.execute("""
                    CREATE TABLE LanguageCCountry (
                        Id INT PRIMARY KEY,
                        LanguageCId INT,
                        CountryId INT,
                        CountryTranslated NVARCHAR(200)
                    )
                """)
                cursor.execute("""
                    CREATE TABLE Research (
                        Id INT PRIMARY KEY,
                        AreaId INT,
                        researchCode NVARCHAR(20),
                        research NVARCHAR(200)
                    )
                """)
                cursor.execute("""
                    CREATE TABLE ResearchLanguage (
                        Id INT PRIMARY KEY,
                        ResearchId INT,
                        LanguageId INT,
                        ResearchTranslated NVARCHAR(300)
                    )
                """)
                cursor.execute("""
                    CREATE TABLE YearIsoResearch (
                        Id INT PRIMARY KEY,
                        ResearchId INT,
                        indexValue FLOAT,
                        YearStateId INT
                    )
                """)
                connection.commit()
                cursor.close()
            
            # Reconnect to the sqlserver database
            connection.close()
            return create_connection(host, user, password, "sqlserver")
            
        return connection
    except pyodbc.Error as e:
        print(f"Error: {e}")
        return None

def insert_data_from_excel(file_path, connection):
    if not connection:
        return False
    
    try:
        df = pd.read_excel(file_path)
        cursor = connection.cursor()

        # Insert languages (Portuguese as default)
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM Language WHERE Id = 1)
            INSERT INTO Language (Id, language) VALUES (1, 'Portuguese')
        """)
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM LanguageC WHERE Id = 1)
            INSERT INTO LanguageC (Id, language) VALUES (1, 'Portuguese')
        """)
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM LanguageR WHERE Id = 1)
            INSERT INTO LanguageR (Id, language) VALUES (1, 'Portuguese')
        """)
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM LanguageSt WHERE Id = 1)
            INSERT INTO LanguageSt (Id, language) VALUES (1, 'Portuguese')
        """)
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM LanguageSub WHERE Id = 1)
            INSERT INTO LanguageSub (Id, language) VALUES (1, 'Portuguese')
        """)

        # Insert Areas
        areas = ['Area 1', 'Area 2', 'Area 3', 'Area 4', 'Area 5']
        for i, area in enumerate(areas, 1):
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM Area WHERE Id = ?)
                INSERT INTO Area (Id, area) VALUES (?, ?)
            """, (i, i, area))

        # Insert Years
        years = df['Year'].unique()
        for i, year in enumerate(years, 1):
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM Year WHERE Id = ?)
                INSERT INTO Year (Id, year) VALUES (?, ?)
            """, (i, i, int(year)))

        # Insert Regions and Countries
        regions = df['World Bank Region'].unique()
        for i, region in enumerate(regions, 1):
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM Region WHERE Id = ?)
                INSERT INTO Region (Id, region) VALUES (?, ?)
            """, (i, i, region))
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM LanguageRRegion WHERE Id = ?)
                INSERT INTO LanguageRRegion (Id, LanguageRId, RegionId, RegionTranslated)
                VALUES (?, ?, ?, ?)
            """, (i, i, 1, i, f"{region} (PT)"))

        # Insert Countries
        for i, row in df.iterrows():
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM Country WHERE Id = ?)
                INSERT INTO Country (Id, ISOCode, country)
                VALUES (?, ?, ?)
            """, (i+1, i+1, row['ISO Code 2'], row['Countries']))
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM LanguageCCountry WHERE Id = ?)
                INSERT INTO LanguageCCountry (Id, LanguageCId, CountryId, CountryTranslated)
                VALUES (?, ?, ?, ?)
            """, (i+1, i+1, 1, i+1, f"{row['Countries']} (PT)"))

        # Insert Research data
        research_columns = [
            'Economic Freedom Summary Index', 'Government consumption', 'Transfers and subsidies',
            'Government investment', 'Top marginal income tax rate', 'Top marginal income and payroll tax rate'
        ]

        for i, col in enumerate(research_columns, 1):
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM Research WHERE Id = ?)
                INSERT INTO Research (Id, AreaId, researchCode, research)
                VALUES (?, ?, ?, ?)
            """, (i, i, 1, f"R{i}", col))
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM ResearchLanguage WHERE Id = ?)
                INSERT INTO ResearchLanguage (Id, ResearchId, LanguageId, ResearchTranslated)
                VALUES (?, ?, ?, ?)
            """, (i, i, i, 1, f"{col} (PT)"))

        # Insert data values
        for i, row in df.iterrows():
            for j, col in enumerate(research_columns, 1):
                if pd.notna(row[col]):
                    cursor.execute("""
                        IF NOT EXISTS (SELECT 1 FROM YearIsoResearch WHERE Id = ?)
                        INSERT INTO YearIsoResearch (Id, ResearchId, indexValue, YearStateId)
                        VALUES (?, ?, ?, ?)
                    """, (i*100+j, i*100+j, j, float(row[col]), i+1))

        connection.commit()
        return True

    except pyodbc.Error as e:
        print(f"Error: {e}")
        connection.rollback()
        return False
    finally:
        if connection:
            cursor.close()
