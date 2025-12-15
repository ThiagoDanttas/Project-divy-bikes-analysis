import pandas as pd
import os
import glob

# Caminhos dos arquivos
path_folder = r"C:\Users\***\Bikes\Files"
path_files_csv = os.path.join(path_folder, "*.csv")

# Define as colunas necessárias
columns = ['rideable_type', 'started_at', 'ended_at',
           'start_station_name', 'end_station_name', 'member_casual']

# Filtra arquivos do tipo csv
files = glob.glob(path_files_csv)

# Inserindo cada file em uma lista
dfs = []
for file in files:
    df = pd.read_csv(file, usecols=columns)
    dfs.append(df)
    print(f"Dfs inseridos: {len(dfs)}")

# Consolida tudo em único dataframe
df_final = pd.concat(dfs, ignore_index=True)

# Salva como parquet
df_final.to_parquet(os.path.join(path_folder, "consolidado.parquet"), engine="pyarrow")


