import requests
import pandas as pd
import sqlite3
import json
import os
from tqdm import tqdm
from datetime import datetime


#Baixa base da ANEEL via API e consolida bases no banco

def API_ANEEL():

    """ GERAÇÃO - https://dadosabertos.aneel.gov.br/organization/agencia-nacional-de-energia-eletrica?groups=geracao """

    #API_GERAÇÃO
    url = 'https://dadosabertos.aneel.gov.br/api/3/action/datastore_search'
    GER_id_bases = { 'GER_SIGA_ANEEL': '11ec447d-698d-4ab8-977f-b424d5deee6a', #Sistema de Informações de Geração da ANEEL
                 'GER_Acrescimo_Anual_PotIns': '3b3eb013-8831-4ca9-a582-01010ff4e0fb',
                 'GER_Capacidade_Instalada_UF': '6fbee0f8-2617-4879-a69a-6b7892f12dad',
                 'GER_Tipo_empreendimento_OP': 'e61fd029-5e78-43be-bed4-873b7b11f04c',
                 'GER_RALIE_Usina': '4a615df8-4c25-48fa-bbea-873a36a79518', #Relatório de Acompanhamento da Expansão da Oferta de Geração de Energia Elétrica
                 'GER_RALIE_Usina': '8a794ee4-3d41-4ce7-a80a-ae7702a16b7b', #Relatório de Acompanhamento da Expansão da Oferta de Geração de Energia Elétrica
                 'GER_RALIE_Usina': '3b3eb013-8831-4ca9-a582-01010ff4e0fb', #Relatório de Acompanhamento da Expansão da Oferta de Geração de Energia Elétrica

    }

    """ DISTRIBUIÇÃO - https://dadosabertos.aneel.gov.br/dataset/?groups=distribuicao """

    #API_DISTRIBUIÇAO
    url = 'https://dadosabertos.aneel.gov.br/api/3/action/datastore_search'

    DIS_id_bases = { 'DIS_ID_cont_2020_2029': '4493985c-baea-429c-9df5-3030422c71d7', #indicadores-continuidade-coletivos-2020-2029
                 'DIS_ID_cont_2010_2019': 'b4dbdc46-a8a3-4e5d-9565-9f23b1156496', #indicadores-continuidade-coletivos-2010-2019
                 'DIS_ID_cont_2000_2009': '33818f8c-bb8f-4a81-b89b-cacbd0a79e75', #indicadores-continuidade-coletivos-2000-2009
                 'DIS_ID_cont_comp_2020_2029': '364d945e-a18b-4111-ab1b-73aa0f7b06b1', #indicadores-continuidade-coletivos-compensacao-2020-2029
                 'DIS_ID_cont_comp_2010_2019': '8cdad784-07c1-4a67-a285-2f2adce5df70', #indicadores-continuidade-coletivos-compensacao-2010-2019
                 'DIS_ID_cont_col_limit': 'fd69e1dd-fd66-4269-b60c-cc0b7eb221b4', #indicadores-continuidade-coletivos-limite
                 'DIS_ID_cont_col_atributos': '3c780aca-38cf-406d-9d45-f07a9216eef2', #indicadores-continuidade-coletivos-atributos
                 'DIS_ID_indqual_dominio': '17fc99b7-e707-4ec4-9553-a43d7a41f7a6', #dominio-indicadores-indqual
                 'DIS_Tarifa_distribuidoras': 'fcf2906c-7c32-4b9b-a637-054e7a5234f4', #Tarifas de aplicação das distribuidoras de energia elétrica
                 'DIS_Int_energy_2017': '246e926b-a686-42fc-b55f-32e4046834de', #interrupcoes-energia-eletrica-2017
                 'DIS_Int_energy_2018': '8fcce0f2-4ea2-42ea-b7ac-00fd8feab03c', #interrupcoes-energia-eletrica-2018
                 'DIS_Int_energy_2019': '965d2abb-91fe-4fab-b463-8c84f2e02188', #interrupcoes-energia-eletrica-2019
                 'DIS_Int_energy_2020': '58201617-8364-4e7f-975d-21f8b8c7436f', #interrupcoes-energia-eletrica-2020
                 'DIS_Int_energy_2021': '42d778de-4a10-4b54-a00a-87c8ff35db6f', #interrupcoes-energia-eletrica-2021
                 'DIS_Int_energy_2022': '7d081751-3f4c-4ede-96b5-1bf2e46b61ca', #interrupcoes-energia-eletrica-2022
                 'DIS_Int_emer_dist_energy_2017': '845b550c-e454-4cac-a8e7-ab7f1732fa6d', #ocorrencias-emergenciais-rede-distribuicao-2017
                 'DIS_Int_emer_dist_energy_2018': '5a7f1a13-2ec1-406c-b542-3bcba96c8a9e', #ocorrencias-emergenciais-rede-distribuicao-2018
                 'DIS_Int_emer_dist_energy_2019': '770611ff-cec4-4b18-82e0-b06375a462f0', #ocorrencias-emergenciais-rede-distribuicao-2019
                 'DIS_Int_emer_dist_energy_2020': 'f41804fe-b3c3-4f39-b777-c48c602affb1', #ocorrencias-emergenciais-rede-distribuicao-2020
                 'DIS_Int_emer_dist_energy_2021': 'ca188f2f-2384-4690-af3a-048f44575c74', #ocorrencias-emergenciais-rede-distribuicao-2021
                 'DIS_Int_emer_dist_energy_2022': '4ecf2f60-65de-4874-b293-330927720ccf', #ocorrencias-emergenciais-rede-distribuicao-2022
                 'DIS_ID_conf_niv_tensao': 'b6755d51-f537-4e0f-8fd8-d2cead66178a', #indicadores-conformidade-nivel-tensao
                 'DIS_ID_comp_niv_tensao': '476d0490-a225-4de7-89a8-bb7a189f0868', #indicadores-compensacao-nivel-tensao
                 'DIS_ID_municipios': '3f841488-80a8-42f2-a6ca-e0c593b228de', #indqual-municipio
                 'DIS_Indice_satisf_ANELL': '7abd3a47-1e1a-4a33-933b-48d658a6f912', #indice-aneel-satisfacao-consumidor
    }

    # ANEEL_BASES = {}
    # ANEEL_BASES.update(GER_id_bases)
    # ANEEL_BASES.update(DIS_id_bases)

    # for name, id in tqdm(ANEEL_BASES.items(),leave=False,unit='Bases'):
        
    #     try:
    #         params = { 'resource_id':id,'limit':999999999999999999999999999999}
    #         response = requests.get(url,params=params)
    #         response = response.json()
    #         response = response['result']['records']
    #         df = pd.DataFrame(response)

    #         conn = sqlite3.connect('ANEEL_GER_DIST.db')
    #         df.to_sql(name,conn,if_exists='replace',index=False)
    #         print(f'Base {name} atualizada com sucesso.')
    #     except:
    #         print(f'Não foi possível atualizar a base: {name}')
        
    # print('\n\nBanco de dados atualizado!!!')

    params = { 'resource_id':'246e926b-a686-42fc-b55f-32e4046834de','limit':999999999999999999999999999999}
    response = requests.get(url,params=params)
    response = response.json()
    response = response['result']['records']
    df = pd.DataFrame(response)
    os.chdir(r'C:\Users\marco\Desktop')
    df.to_csv('Testando_tamanho_do_arquivo.csv',sep =';',index=False)

    return

def UP_CSV_TO_DB():

    print(f'Iniciando a subida das bases para o banco de dados...({datetime.now()})\n\n')
    os.chdir(r"C:\Users\marco\Desktop\TCC\BASES_COMPLETAS_DISTRIBUIÇÃO")
    files = list(os.listdir())
    conn = sqlite3.connect(r"C:\Users\marco\Desktop\TCC\DIST_DB_ANEEL.db")

    for file in tqdm(files,leave=False):
        os.chdir(r"C:\Users\marco\Desktop\TCC\BASES_COMPLETAS_DISTRIBUIÇÃO")
        df = pd.read_csv(file,sep=';',encoding = 'ansi',low_memory=False)
        df.to_sql('DIST_'+str(file)[0:-4],conn,if_exists='replace',index=False)
        print(f'\n{str(file)} ----- OK')
    
    conn.close()
    
    print(f'\nBanco de dados atualizado!({datetime.now()})\n\n')

    return

#UP_CSV_TO_DB()


def UP_CSV_TO_DB_2():

    conn = sqlite3.connect(r"C:\Users\marco\Desktop\TCC\DIST_DB_ANEEL.db")

    file = r"C:\Users\marco\Desktop\TCC\in_put\indice-aneel-satisfacao-consumidor (1).csv"
    df = pd.read_csv(file,sep=';',encoding = 'ansi',low_memory=False)
    df.to_sql('DIST_IND_SATISF_CONSU_ANEEL',conn,if_exists='replace',index=False)
    print(f'\n{str(file)} ----- OK')
    
    conn.close()
    
    print(f'\nBanco de dados atualizado!({datetime.now()})\n\n')

    return

UP_CSV_TO_DB_2()