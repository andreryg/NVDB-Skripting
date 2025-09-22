# -*- coding: utf-8 -*-
"""
Created on Mon Sep 15 14:35:58 2025

@author: andryg
"""

import requests
import time
import pandas as pd

def hent_vegobjekttype(nvdbid, query, miljø, rådata=False):
    match miljø:
        case 'prod':
            base_url = "https://nvdbapiles.atlas.vegvesen.no/"
        case 'test':
            base_url = "https://nvdbapiles.test.atlas.vegvesen.no/"
        case 'stm':
            base_url = "https://nvdbapiles.utv.atlas.vegvesen.no/"
        case 'utv':
            base_url = "https://nvdbapiles.utv.atlas.vegvesen.no/"
        case _:
            return pd.DataFrame()
            
    antall_hentet = 0
    nvdb_url = f"{base_url}vegobjekter/{nvdbid}?{query}"
    df_list = []
    while True:
        t = requests.get(nvdb_url, headers={"X-Client": "Andryg python"})
        if t.status_code == 200:
            t = t.json()
            antall_hentet += t.get('metadata').get('returnert')
            df_list += [pd.json_normalize(t['objekter'])]
            ny_url = t.get('metadata').get('neste').get('href')
            if ny_url == nvdb_url:
                break
            else:
                nvdb_url = ny_url
            print(antall_hentet)
        else:
            print(t.text)
            print("Error, prøver på nytt om 5 sekunder")
            time.sleep(5)
    if df_list:
        df = pd.concat(df_list, ignore_index=True)
    else:
        df = pd.DataFrame()
        return df
    if rådata:
        return df
    
    columns = [column for column in df.columns.tolist()]
    column_categories = list(set([column.split('.')[0] for column in columns]))
    print(columns)
    print(column_categories)
    

def hent_vegnett(fylke_id, vref, detaljnivå, typeveg, sideanlegg, veglenketype):
    url = f"https://nvdbapiles.atlas.vegvesen.no/vegnett/api/v4/veglenkesekvenser/segmentert?fylke={fylke_id}&vegsystemreferanse={vref}&detaljniva={detaljnivå}&typeveg={typeveg}&sideanlegg={sideanlegg}&veglenketype={veglenketype}"
    df_list = []
    antall = 0
    while True:
        r = requests.get(url, headers={"X-Client": "Andryg python"})
        if r.status_code == 200:
            r = r.json()
            if len(r['objekter']) == 0:
                break
            df_list += [pd.json_normalize(r['objekter'])]
            url = r['metadata']['neste']['href']
            antall += r['metadata']['returnert']
            print(f"Antall: {antall}")
        else:
            print(r.text)
            print("Error, prøver på nytt om 5 sekunder")
            time.sleep(5)
    if df_list:
        df = pd.concat(df_list, ignore_index=True)

        df = df[df['vegsystemreferanse.vegsystem.fase'] == 'V']
        df = df[df['vegsystemreferanse.strekning.trafikantgruppe'] != 'G']
        df = df[['veglenkesekvensid', 'lengde']]
        df = df.groupby('veglenkesekvensid')['lengde'].sum()

        return df
    else:
        return pd.DataFrame()
    
if __name__ == "__main__":
    df = hent_vegobjekttype(273, "inkluder=alle", "prod")
    