import pandas as pd
import sgs
import sqlalchemy
import numpy as np
from multiprocessing.pool import Pool
from statsmodels.tsa.holtwinters import SimpleExpSmoothing, Holt

import warnings
warnings.filterwarnings('ignore')

# Get data from SGS
sgg = {'CDI':12}

df = pd.DataFrame()
for j in sgg:
    t = sgs.time_serie(sgg[j], start="30/10/2023", end="20/11/2023")/100 + 1
    t.name = 'diff_log'
    t.index.name = 'DATE'

acumulado = 1
for i in range(len(t)):
    acumulado = acumulado * t[i]

# Establish a connection to your PostgreSQL database
engine = sqlalchemy.create_engine('postgresql://postgres:admin@localhost:5432/postgres')
conn = engine.connect()

query = """
select * from tempo
"""

tempo = pd.read_sql(query, conn)

# select tabela alvo
query = """
select * from info_cadastro
where sit <> 'CANCELADA' and cnpj_fundo in (select cnpj_fundo from alvo where vl_patrim_liq>100000000)
"""
df = pd.read_sql(query, conn)

# forecasting 14 days
days = pd.DataFrame()
days['day'] = [28,29,30,31,1,2,3,4,5,6,7,8,9,10]
days['month'] = [10,10,10,10,11,11,11,11,11,11,11,11,11,11]
days['year'] = [2023]*14

query = """
select vl_cota, tempo_id, cnpj_fundo from alvo
where vl_patrim_liq>100000000 and cnpj_fundo not in (select cnpj_fundo from info_cadastro where sit = 'CANCELADA')
"""

df2 = pd.read_sql(query, conn)

df2 = df2.merge(tempo, left_on='tempo_id', right_on='tempo_id')
df2.drop(columns=['tempo_id'], inplace=True)
df2['log_cota'] = np.log(df2['vl_cota'])
df2 = df2.dropna()

lista = range(len(df))
print(len(lista))

def GET(i):

    print(i)

    cnpj = df['cnpj_fundo'][i]

    df3 = df2[df2['cnpj_fundo'] == cnpj]

    dict = {'cnpj_fundo': cnpj, 'forecast': 0, 'porc_cdi': 0, 'apr': 0, 'mse': 0, 'mae': 0}

    if len(df3) < 2:
        return dict

    df3['day'] = df3['dt_comptc'].dt.day
    df3['month'] = df3['dt_comptc'].dt.month
    df3['year'] = df3['dt_comptc'].dt.year
    df3['log_cota'] = np.log(df3['vl_cota'])
    df3['diff_log'] = df3['log_cota'].diff()

    y_train = df3['diff_log']

    model = SimpleExpSmoothing(np.asarray(y_train[1:]))

    try:
        fit2 = model.fit(smoothing_level=.2)
    except:
        return dict
    
    fitted = fit2.fittedvalues

    mse = np.mean((fitted - y_train[1:])**2)
    mae = np.mean(np.abs(fitted - y_train[1:]))
    
    pred2 = fit2.forecast(14)
    soma = pred2.sum()
    result = np.exp(soma)
    apr = 'BOM' if result > acumulado else 'RUIM'

    dict = {'cnpj_fundo': cnpj, 'forecast': result, 'porc_cdi': result/acumulado, 'apr': apr, 'mse': mse, 'mae': mae}

    return dict


if __name__ == '__main__':
    
    with Pool(processes=2) as pool:
        DATA_T = pool.map(GET, lista)

    DATA_T = [t for t in DATA_T if t != None]

    dados = pd.DataFrame(DATA_T)
    dados = dados[dados['apr'] != 0]

    # sort by porc_cdi
    dados = dados.sort_values(by=['porc_cdi'], ascending=False)

    dados.to_csv('dados.csv', index=False)