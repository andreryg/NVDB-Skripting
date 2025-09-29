# -*- coding: utf-8 -*-

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

    def populate_columns(self, attributes = True, relationships = True, road_reference = True) -> None:
        def populate_attributes():
            if not hasattr(self, 'attributes'):
                self.get_attributes_from_data_catalogue()
            for attr in self.attributes:
                if attr not in self.objects.columns:
                    attr_id = attr.split('.')[0]
                    self.objects[attr] = self.objects['egenskaper'].apply(lambda attributes: next((attribute.get('verdi') for attribute in attributes if str(attribute.get('id')) == attr_id), None) if isinstance(attributes, list) else None)
        
        def populate_relationships():
            if not hasattr(self, 'parents') and not hasattr(self, 'children'):
                self.get_relationships_from_data_catalogue()
            for rel in self.parents:
                if rel not in self.objects.columns and 'relasjoner.foreldre' in self.objects.columns:
                    parent_id = rel.split('.')[0]
                    self.objects['Forelder_'+rel] = self.objects['relasjoner.foreldre'].apply(lambda parents: next((parent.get('vegobjekter') for parent in parents if str(parent.get('type').get('id')) == parent_id), None) if isinstance(parents, list) else None)
            for rel in self.children:
                if rel not in self.objects.columns and 'relasjoner.barn' in self.objects.columns:
                    child_id = rel.split('.')[0]
                    self.objects['Barn_'+rel] = self.objects['relasjoner.barn'].apply(lambda children: next((child.get('vegobjekter') for child in children if str(child.get('type').get('id')) == child_id), None) if isinstance(children, list) else None)

        if attributes:
            populate_attributes()
        if relationships:
            populate_relationships()

    def download(self) -> bool:
        api_url = self.build_api_url()
        total_fetched = 0
        df_list = []

        def fetch_objects(new_url=None):
            @api_caller(api_url=new_url)
            def fetcher(data=None) -> dict|None:
                return data
            return fetcher()
            
        while True:
            data = fetch_objects(api_url)
            if not data or data.get('metadata', {}).get('returnert', 0) == 0:
                break
            total_fetched += data.get('metadata', {}).get('returnert', 0)
            print(f"Total fetched: {total_fetched}")
            
            next_url = data.get('metadata', {}).get('neste', {}).get('href')
            if next_url == api_url or not next_url:
                break
            df_list.append(pd.json_normalize(data.get('objekter', [])))
            api_url = next_url
            
        if df_list:
            self.objects = pd.concat(df_list, ignore_index=True)
            return True
        else:
            return False
        
    def export(self, file_name: str, file_type: str = "csv") -> None:
        match file_type.lower():
            case 'csv':
                self.objects.to_csv(file_name+'.csv', index=False, sep=';', encoding='utf-8-sig')
            case 'excel' | 'xlsx':
                self.objects.to_excel(file_name+'.xlsx', index=False)
            case 'txt':
                self.objects.to_csv(file_name+'.txt', index=False, sep=';', encoding='utf-8-sig')
            case _:
                print("Unsupported file type. Supported types are: csv, excel/xlsx, json. Defaulting to csv.")
                self.objects.to_csv(file_name+'.csv', index=False, sep=';', encoding='utf-8-sig')

class RoadNetworkDownloader:
    def __init__(self, environment: str = "prod", **api_query_parameters: str):
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

        self.road_segments = pd.DataFrame()
        self.api_query_parameters = api_query_parameters

    def build_api_url(self) -> str:
        query_string = "&".join([f"{key}={value}" for key, value in self.api_query_parameters.items()])
        return f"{self.base_url}vegnett/api/v4/veglenkesekvenser/segmentert?{query_string}"
    
    def download(self) -> bool:
        api_url = self.build_api_url()
        total_fetched = 0
        df_list = []

        def fetch_segments(new_url=None):
            @api_caller(api_url=new_url)
            def fetcher(data=None) -> dict|None:
                return data
            return fetcher()
            
        while True:
            data = fetch_segments(api_url)
            if not data or data.get('metadata', {}).get('returnert', 0) == 0:
                break
            total_fetched += data.get('metadata', {}).get('returnert', 0)
            print(f"Total fetched: {total_fetched}")
            
            next_url = data.get('metadata', {}).get('neste', {}).get('href')
            if next_url == api_url or not next_url:
                break
            df_list.append(pd.json_normalize(data.get('objekter', [])))
            api_url = next_url
            
        if df_list:
            self.road_segments = pd.concat(df_list, ignore_index=True)
            self.road_segments = self.road_segments[self.road_segments['vegsystemreferanse.vegsystem.nummer'] != 99999] #Used for internal testing
            self.road_segments = self.road_segments[self.road_segments['vegsystemreferanse.vegsystem.fase'] == 'V'] #Only drivable roads
            return True
        else:
            return False
        
    def export(self, file_name: str, file_type: str = "csv") -> None:
        match file_type.lower():
            case 'csv':
                self.road_segments.to_csv(file_name+'.csv', index=False, sep=';', encoding='utf-8-sig')
            case 'excel' | 'xlsx': #Deprecated, use csv or txt instead
                self.road_segments.to_excel(file_name+'.xlsx', index=False)
            case 'txt':
                self.road_segments.to_csv(file_name+'.txt', index=False, sep=';', encoding='utf-8-sig')
            case _:
                print("Unsupported file type. Supported types are: csv, excel/xlsx, json. Defaulting to csv.")
                self.road_segments.to_csv(file_name+'.csv', index=False, sep=';', encoding='utf-8-sig')
    
if __name__ == "__main__":
    instance = FeatureTypeDownloader(feature_type_id=210, environment='prod', raw_data=False, inkluder='metadata,egenskaper,relasjoner')
    instance.download()
    instance.populate_columns(attributes=False, relationships=False, road_reference=False)
    instance.export(file_name='vegobjekter_210_raw', file_type='csv')
    #instance.get_relationships_from_data_catalogue()
    #

    # instance.download()
    # instance.export(file_name='vegobjekter_210', file_type='csv')
    # Basic examples for RoadNetworkDownloader:

    # The class takes environment as an argument, which should be 'prod'
    # Additionally it takes any of the parameters listed here: https://nvdbapiles.atlas.vegvesen.no/swagger-ui/index.html?urls.primaryName=Vegnett#/Vegnett/getVeglenkesegmenter
    # To download data for a specific date, use 'tidspunkt' parameter in the format 'YYYY-MM-DD'.
    
    # Downloads road network for whole Norway in 2000-01-01 and exports to CSV.
    #date = '2000-01-01'
    #instance = RoadNetworkDownloader(environment='prod', tidspunkt=date)
    #instance.download()
    #instance.export(file_name=f'Road_network_{date}', file_type='csv')

    # Downloads road network for Trøndelag county in 2000-01-01 and exports to Excel. You can also have multiple counties: fylke='50,34'
    #instance = RoadNetworkDownloader(environment='prod', tidspunkt='2000-01-01', fylke='50')
    #instance.download()
    #instance.export(file_name='Road_network_50_2000-01-01', file_type='csv')

    # Most useful parameters is probably:
    # fylke (county), one or more county ids, 
    # kommune (municipality), one or more municipality ids,
    # vegsystemfereranse (road system reference), road category and number, e.g. 'EV6', 'RV3', 'FV65' etc, but can also be just the road category: 'E', 'R' or 'E,R,F'.