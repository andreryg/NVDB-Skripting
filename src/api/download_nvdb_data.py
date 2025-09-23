# -*- coding: utf-8 -*-
"""
Created on Mon Sep 15 14:35:58 2025

@author: andryg
"""

import requests
import time
import pandas as pd
from functools import wraps

def api_caller(api_url):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            MAX_RETRIES = 3
            retries = 0
            while retries < MAX_RETRIES:
                response = requests.get(api_url, headers={"X-Client": "Andryg python"})
                if response.status_code == 200:
                    data = response.json()
                    return func(data)
                else:
                    print(response.text)
                    print("Error, retrying in 5 seconds")
                    retries += 1
                    time.sleep(5)
            print("Max retries reached. Exiting.")
            return None
        return wrapper
    return decorator

class FeatureTypeDownloader:
    def __init__(self, feature_type_id: int, environment: str = "prod", raw_data: bool = False, **api_query_parameters: str):
        self.feature_type_id = feature_type_id
        match environment:
            case 'prod':
                self.base_url = "https://nvdbapiles.atlas.vegvesen.no/"
            case 'test':
                self.base_url = "https://nvdbapiles.test.atlas.vegvesen.no/"
            case 'stm':
                self.base_url = "https://nvdbapiles.utv.atlas.vegvesen.no/"
            case 'utv':
                self.base_url = "https://nvdbapiles.utv.atlas.vegvesen.no/"
            case _:
                print("Invalid environment. Choose from 'prod', 'test', 'stm', or 'utv'. Defaulting to 'prod'.")
                self.base_url = "https://nvdbapiles.atlas.vegvesen.no/"
        self.objects = pd.DataFrame()
        self.raw_data = raw_data
        self.api_query_parameters = api_query_parameters

    def build_api_url(self) -> str:
        query_string = "&".join([f"{key}={value}" for key, value in self.api_query_parameters.items()])
        return f"{self.base_url}vegobjekter/{self.feature_type_id}?{query_string}"
    
    def get_attributes_from_data_catalogue(self) -> None:
        data_catalogue_url = f"{self.base_url}datakatalog/api/v1/vegobjekttyper/{self.feature_type_id}?inkluder=egenskapstyper"

        @api_caller(api_url=data_catalogue_url)
        def fetch_attributes(data=None) -> list:
            if not data:
                return []
            attributes = [str(attr['id'])+'.'+attr['navn'] for attr in data.get('egenskapstyper', []) if attr.get('id') < 100000]
            return attributes
        self.attributes = fetch_attributes()
    
    def get_relationships_from_data_catalogue(self) -> None:
        data_catalogue_url = f"{self.base_url}datakatalog/api/v1/vegobjekttyper/{self.feature_type_id}?inkluder=relasjonstyper"

        @api_caller(api_url=data_catalogue_url)
        def fetch_relationships(data=None) -> tuple[list, list]:
            if not data:
                return [], []
            parents = [str(parent['innhold']['type']['id'])+'.'+parent['innhold']['type']['navn'] for parent in data.get('relasjonstyper', []).get('foreldre', [])]
            children = [str(child['innhold']['type']['id'])+'.'+child['innhold']['type']['navn'] for child in data.get('relasjonstyper', []).get('barn', [])]
            return parents, children
        self.parents, self.children = fetch_relationships()
    
    def download(self) -> bool:
        api_url = self.build_api_url()
        total_fetched = 0
        df_list = []
        
        while True:
            response = requests.get(api_url, headers={"X-Client": "Andryg python"})
            if response.status_code == 200:
                data = response.json()
                total_fetched += data.get('metadata', {}).get('returnert', 0)
                df_list.append(pd.json_normalize(data.get('objekter', [])))
                next_url = data.get('metadata', {}).get('neste', {}).get('href')
                if next_url == api_url or not next_url:
                    break
                api_url = next_url
                print(f"Total fetched: {total_fetched}")
            else:
                print(response.text)
                print("Error, retrying in 5 seconds")
                time.sleep(5)
        
        if df_list:
            self.objects = pd.concat(df_list, ignore_index=True)
            return True
        else:
            return False


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
    instance = FeatureTypeDownloader(feature_type_id=210, miljø='prod', raw_data=False, inkluder='egenskaper')
    instance.get_relationships_from_data_catalogue()
    print(instance.children)

    