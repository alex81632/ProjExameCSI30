import pandas as pd
import sqlalchemy

# Establish a connection to your PostgreSQL database
engine = sqlalchemy.create_engine('postgresql://postgres:admin@localhost:5432/postgres')
conn = engine.connect()

columns = ["CNPJ_FUNDO", "DENOM_SOCIAL", "TP_FUNDO", "SIT"]
registration_file = "data/cad_fi.csv"
registration_df = pd.read_csv(registration_file, sep=";", encoding = "ISO-8859-1", usecols=columns)
registration_df = registration_df.drop_duplicates(subset=["CNPJ_FUNDO"])

if registration_df is None:
    exit(1)

regist_up = registration_df.copy()
regist_up.rename(columns={"TP_FUNDO": "tp_fundo", "CNPJ_FUNDO": "cnpj_fundo", "DENOM_SOCIAL": "denom_social", "SIT": "sit"}, inplace=True)
resultado = regist_up.to_sql('info_cadastro', con=conn, if_exists='append', index=False)
print(registration_df)

target_final = pd.DataFrame(columns=["cnpj_fundo", "dt_comptc", "vl_cota", "vl_total", "vl_patrim_liq", "nr_cotst", "captc_dia", "resg_dia"])

dates = pd.date_range(start="2021-11-01", end="2023-10-31", freq="M")
for i in range(len(dates)):
    date = dates[i]
    file_name = f"data/inf_diario_fi_{date.year}{str(date.month).rjust(2, '0')}.csv"
    df = pd.read_csv(file_name, sep=";", encoding="ISO-8859-1")

    merged_df = pd.merge(df, registration_df, how="inner", left_on="CNPJ_FUNDO", right_on="CNPJ_FUNDO")
    target = merged_df[["CNPJ_FUNDO","DT_COMPTC", "VL_QUOTA", "VL_TOTAL", "VL_PATRIM_LIQ", "NR_COTST", "CAPTC_DIA", "RESG_DIA"]] 
    target.rename(columns={"CNPJ_FUNDO": "cnpj_fundo", "DT_COMPTC": "dt_comptc", "VL_QUOTA": "vl_cota", "VL_TOTAL": "vl_total", "VL_PATRIM_LIQ": "vl_patrim_liq", "NR_COTST": "nr_cotst", "CAPTC_DIA": "captc_dia", "RESG_DIA": "resg_dia"}, inplace=True)

    # concatena os dataframes
    target_final = pd.concat([target_final, target])

target_final.reset_index(drop=True, inplace=True)

# cria tabela tempo
lista = target_final["dt_comptc"].unique()
lista_df = pd.DataFrame(lista, columns=["dt_comptc"])
lista_df.reset_index(inplace=True)
lista_df.rename(columns={"index": "id"}, inplace=True)
target_final = pd.merge(target_final, lista_df, how="inner", left_on="dt_comptc", right_on="dt_comptc")
target_final.rename(columns={"id": "tempo_id"}, inplace=True)
target_final.drop(columns=["dt_comptc"], inplace=True)

# dropar os que tem vl_cota = 0
target_final = target_final[target_final["vl_cota"] != 0]

lista_df.rename(columns={"id": "tempo_id", "dt_comptc": "dt_comptc"}, inplace=True)

lista_df.to_sql('tempo', con=conn, if_exists='append', index=False)

target_final.to_sql('alvo', con=conn, if_exists='append', index=False)
