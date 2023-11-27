import requests
import zipfile
import pandas as pd

base_url = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"

dates = pd.date_range(start="2021-11-01", end="2023-10-31", freq="M")

#criar pasta zip
import os
if not os.path.exists('zip'):
    os.makedirs('zip')

#criar pasta data
if not os.path.exists('data'):
    os.makedirs('data')


for date in dates:
    file_name = f"inf_diario_fi_{date.year}{str(date.month).rjust(2, '0')}.zip"
    url = f"{base_url}{file_name}"
    download = requests.get(url)

    with open(f"zip/{file_name}", "wb") as cvm_file:
        cvm_file.write(download.content)

    zip_file = zipfile.ZipFile(f"zip/{file_name}")
    zip_file.extract(zip_file.namelist()[0], path=f"data/")

    print(f"File: {file_name} downloaded sucessfully!")

registration_url = "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"
download = requests.get(registration_url)
file_name = "cad_fi.csv"


with open(f"data/{file_name}", "wb") as cvm_file:
     cvm_file.write(download.content)

print(f"File: {file_name} downloaded sucessfully!")
