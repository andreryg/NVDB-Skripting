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

def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Function '{func.__name__}' executed in {end_time - start_time:.2f} seconds")
        return result
    return wrapper

class FeatureTypeDownloader:
    def __init__(self, feature_type_id: int, environment: str = "prod", **api_query_parameters: str) -> None:
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
        self.api_query_parameters : dict = api_query_parameters

    def build_api_url(self) -> str:
        query_string : str = "&".join([f"{key}={value}" for key, value in self.api_query_parameters.items()])
        return f"{self.base_url}vegobjekter/{self.feature_type_id}?{query_string}"
    
    def get_attributes_from_data_catalogue(self) -> None:
        data_catalogue_url : str = f"{self.base_url}datakatalog/api/v1/vegobjekttyper/{self.feature_type_id}?inkluder=egenskapstyper"

        @api_caller(api_url=data_catalogue_url)
        def fetch_attributes(data=None) -> list:
            if not data:
                return []
            attributes : list = [str(attr['id'])+'.'+attr['navn'] for attr in data.get('egenskapstyper', []) if attr.get('id') < 100000]
            return attributes
        self.attributes = fetch_attributes()
    
    def get_relationships_from_data_catalogue(self) -> None:
        data_catalogue_url : str = f"{self.base_url}datakatalog/api/v1/vegobjekttyper/{self.feature_type_id}?inkluder=relasjonstyper"

        @api_caller(api_url=data_catalogue_url)
        def fetch_relationships(data=None) -> tuple[list, list]:
            if not data:
                return [], []
            parents : list = [str(parent['innhold']['type']['id'])+'.'+parent['innhold']['type']['navn'] for parent in data.get('relasjonstyper', []).get('foreldre', [])]
            children : list = [str(child['innhold']['type']['id'])+'.'+child['innhold']['type']['navn'] for child in data.get('relasjonstyper', []).get('barn', [])]
            return parents, children
        self.parents, self.children = fetch_relationships()

    @timing_decorator
    def populate_columns(self, attributes = True, geometry_attribute_quality_parameters = True, relationships = True, road_reference = True, geometry = True) -> None:
        def populate_attributes() -> list[str]:
            if not hasattr(self, 'attributes'):
                self.get_attributes_from_data_catalogue()
            old_columns : list = self.objects.columns.tolist()
            for attr in self.attributes:
                if attr not in self.objects.columns:
                    attr_id : str = attr.split('.')[0]
                    self.objects['ET_' + attr] = self.objects['egenskaper'].apply(lambda attributes: next((attribute.get('verdi') for attribute in attributes if str(attribute.get('id')) == attr_id), None) if isinstance(attributes, list) else None)
                    if "Geometri" in attr and geometry_attribute_quality_parameters:
                        for quality_param in ['Målemetode', 'Datafangtsmetode', 'Nøyaktighet', 'Synbarhet', 'MålemetodeHøyde', 'DatafangstmetodeHøyde', 'NøyaktighetHøyde']:
                            self.objects['ET_' + attr + '.' + quality_param] = self.objects['egenskaper'].apply(lambda attributes: next((attribute.get('kvalitet', {}).get(quality_param[0].lower() + quality_param[1:], None) for attribute in attributes if str(attribute.get('id')) == attr_id), None) if isinstance(attributes, list) else None)
                        self.objects['ET_' + attr + '.Datafangstdato'] = self.objects['egenskaper'].apply(lambda attributes: next((attribute.get('datafangstdato', None) for attribute in attributes if str(attribute.get('id')) == attr_id), None) if isinstance(attributes, list) else None)
                        self.objects['ET_' + attr + '.Høydereferanse'] = self.objects['egenskaper'].apply(lambda attributes: next((attribute.get('høydereferanse', None) for attribute in attributes if str(attribute.get('id')) == attr_id), None) if isinstance(attributes, list) else None)
            attribute_columns = [col for col in self.objects.columns if col not in old_columns]
            return attribute_columns
        
        def populate_relationships() -> list[str]:
            if not hasattr(self, 'parents') and not hasattr(self, 'children'):
                self.get_relationships_from_data_catalogue()
            old_columns : list[str] = self.objects.columns.tolist()
            for rel in self.parents:
                if rel not in self.objects.columns and 'relasjoner.foreldre' in self.objects.columns:
                    parent_id : str = rel.split('.')[0]
                    self.objects['Forelder_'+rel] = self.objects['relasjoner.foreldre'].apply(lambda parents: next((parent.get('vegobjekter') for parent in parents if str(parent.get('type').get('id')) == parent_id), None) if isinstance(parents, list) else None)
            for rel in self.children:
                if rel not in self.objects.columns and 'relasjoner.barn' in self.objects.columns:
                    child_id : str = rel.split('.')[0]
                    self.objects['Barn_'+rel] = self.objects['relasjoner.barn'].apply(lambda children: next((child.get('vegobjekter') for child in children if str(child.get('type').get('id')) == child_id), None) if isinstance(children, list) else None)
            relationship_columns = [col for col in self.objects.columns if col not in old_columns]
            return relationship_columns

        def populate_road_reference() -> None:
            if 'lokasjon.kontraktsområder' in self.objects.columns:
                self.objects['lokasjon.kontraktsområder'] = self.objects['lokasjon.kontraktsområder'].apply(lambda kontraktsområder: [kontraktsområde.get('navn', None) for kontraktsområde in kontraktsområder if isinstance(kontraktsområde, dict)] if isinstance(kontraktsområder, list) else None)

            if 'lokasjon.vegforvaltere' in self.objects.columns:
                self.objects['lokasjon.vegforvaltere'] = self.objects['lokasjon.vegforvaltere'].apply(lambda vegforvaltere: [vegforvalter.get('vegforvalter', None) for vegforvalter in vegforvaltere if isinstance(vegforvalter, dict)] if isinstance(vegforvaltere, list) else None)

            if 'lokasjon.adresser' in self.objects.columns:
                self.objects['Adressekoder'] = self.objects['lokasjon.adresser'].apply(lambda adresser: [adresse.get('adressekode', None) for adresse in adresser if isinstance(adresse, dict)] if isinstance(adresser, list) else None)
                self.objects['lokasjon.adresser'] = self.objects['lokasjon.adresser'].apply(lambda adresser: [adresse.get('navn', None) for adresse in adresser if isinstance(adresse, dict)] if isinstance(adresser, list) else None)

            if 'lokasjon.vegsystemreferanser' in self.objects.columns:
                self.objects['Vegkategorier'] = self.objects['lokasjon.vegsystemreferanser'].apply(lambda vegsystemreferanser: list(set([vegsystemreferanse.get('vegsystem', {}).get('vegkategori', None) for vegsystemreferanse in vegsystemreferanser if isinstance(vegsystemreferanse, dict)])) if isinstance(vegsystemreferanser, list) else None)
                self.objects['Vegfaser'] = self.objects['lokasjon.vegsystemreferanser'].apply(lambda vegsystemreferanser: list(set([vegsystemreferanse.get('vegsystem', {}).get('fase', None) for vegsystemreferanse in vegsystemreferanser if isinstance(vegsystemreferanse, dict)])) if isinstance(vegsystemreferanser, list) else None)
                self.objects['Vegnumre'] = self.objects['lokasjon.vegsystemreferanser'].apply(lambda vegsystemreferanser: list(set([vegsystemreferanse.get('vegsystem', {}).get('nummer', None) for vegsystemreferanse in vegsystemreferanser if isinstance(vegsystemreferanse, dict)])) if isinstance(vegsystemreferanser, list) else None)
                self.objects['Strekning'] = self.objects['lokasjon.vegsystemreferanser'].apply(lambda vegsystemreferanser: False if vegsystemreferanser != vegsystemreferanser else (True if any(isinstance(vegsystemreferanse, dict) and 'strekning' in vegsystemreferanse for vegsystemreferanse in vegsystemreferanser) else False))
                self.objects['Kryssystem'] = self.objects['lokasjon.vegsystemreferanser'].apply(lambda vegsystemreferanser: False if vegsystemreferanser != vegsystemreferanser else (True if any(isinstance(vegsystemreferanse, dict) and 'kryssystem' in vegsystemreferanse for vegsystemreferanse in vegsystemreferanser) else False))
                self.objects['Sideanlegg'] = self.objects['lokasjon.vegsystemreferanser'].apply(lambda vegsystemreferanser: False if vegsystemreferanser != vegsystemreferanser else (True if any(isinstance(vegsystemreferanse, dict) and 'sideanlegg' in vegsystemreferanse for vegsystemreferanse in vegsystemreferanser) else False))
                self.objects['Vegsystemreferanseretning'] = self.objects['lokasjon.vegsystemreferanser'].apply(lambda vegsystemreferanser: list(set([vegsystemreferanse.get('metrertLokasjon', {}).get('retning', None) for vegsystemreferanse in vegsystemreferanser if isinstance(vegsystemreferanse, dict)])) if isinstance(vegsystemreferanser, list) else None)
                self.objects['Sideposisjoner'] = self.objects['lokasjon.vegsystemreferanser'].apply(lambda vegsystemreferanser: list(set([vegsystemreferanse.get('metrertLokasjon', {}).get('sideposisjon', None) for vegsystemreferanse in vegsystemreferanser if isinstance(vegsystemreferanse, dict)])) if isinstance(vegsystemreferanser, list) else None)
                self.objects['lokasjon.vegsystemreferanser'] = self.objects['lokasjon.vegsystemreferanser'].apply(lambda vegsystemreferanser: [vegsystemreferanse.get('kortform', None) for vegsystemreferanse in vegsystemreferanser if isinstance(vegsystemreferanse, dict)] if isinstance(vegsystemreferanser, list) else None)

            if 'lokasjon.stedfestinger' in self.objects.columns:
                self.objects['Stedfestingstyper'] = self.objects['lokasjon.stedfestinger'].apply(lambda stedfestinger: list(set([stedfesting.get('type', None) for stedfesting in stedfestinger if isinstance(stedfesting, dict)])) if isinstance(stedfestinger, list) else None)
                self.objects['lokasjon.stedfestinger'] = self.objects['lokasjon.stedfestinger'].apply(lambda stedfestinger: [stedfesting.get('kortform', None) for stedfesting in stedfestinger if isinstance(stedfesting, dict)] if isinstance(stedfestinger, list) else None)

            if 'lokasjon.riksvegruter' in self.objects.columns:
                self.objects['lokasjon.riksvegruter'] = self.objects['lokasjon.riksvegruter'].apply(lambda riksvegruter: [riksvegrute.get('riksvegrute', None) for riksvegrute in riksvegruter if isinstance(riksvegrute, dict)] if isinstance(riksvegruter, list) else None)

        def rename_columns() -> None:
            column_name_mapping : dict = {
                'id': 'nvdbId',
                'metadata.type.id': 'VT_ID',
                'metadata.type.navn': 'VT_Navn',
                'metadata.versjon': 'Versjon',
                'metadata.startdato': 'Startdato',
                'metadata.sluttdato': 'Sluttdato',
                'metadata.sist_modifisert': 'Sist_modifisert',
                'lokasjon.kommuner': 'Kommuner',
                'lokasjon.fylker': 'Fylker',
                'lokasjon.geometri.wkt': 'Lokasjonsgeometri',
                'lokasjon.kontraktsområder': 'Kontraktsområder',
                'lokasjon.vegforvaltere': 'Vegforvaltere',
                'lokasjon.adresser': 'Adresser',
                'lokasjon.vegsystemreferanser': 'Vegsystemreferanser',
                'lokasjon.stedfestinger': 'Stedfestinger',
                'lokasjon.lengde': 'Stedfestingslengde',
                'lokasjon.riksvegruter': 'Riksvegruter',
                'geometri.wkt': 'Geometri',
                'geometri.lengde': 'Geometrilengde',
                'geometri.areal': 'Geometriareal',
                'geometri.srid': 'Geometri_SRID',
                'geometri.egengeometri': 'Har_egengeometri',
            }
            filtered_columns : dict = {col: new_col for col, new_col in column_name_mapping.items() if col in self.objects.columns}
            self.objects.rename(columns=filtered_columns, inplace=True)

        ac, rc, rrc, gc = [], [], [], []
        if attributes:
            ac = populate_attributes()
        if relationships:
            rc = populate_relationships()
        if road_reference:
            populate_road_reference()
            rrc = ['Kommuner', 'Fylker', 'Vegforvaltere', 'Kontraktsområder', 'Adresser', 'Adressekoder', 
                   'Riksvegruter', 'Vegkategorier', 'Vegfaser', 'Vegnumre', 'Vegsystemreferanser', 'Vegsystemreferanseretning', 'Sideposisjoner', 'Strekning', 'Kryssystem', 'Sideanlegg', 'Stedfestinger', 
                   'Stedfestingstyper', 'Stedfestingslengde', 'Lokasjonsgeometri']
        if geometry:
            gc = ['Geometri', 'Geometrilengde', 'Geometriareal', 'Geometri_SRID', 'Har_egengeometri']
        rename_columns()

        columns : list[str] = [col_name for col_name in ['nvdbId', 'VT_ID', 'VT_Navn', 'Versjon', 'Startdato', 'Sluttdato', 'Sist_modifisert'] + ac + rc + rrc + gc if col_name in self.objects.columns]
        self.objects : pd.DataFrame = self.objects[columns]

    def download(self) -> bool:
        api_url = self.build_api_url()
        total_fetched = 0
        df_list = []

        def fetch_objects(new_url=None) -> dict|None:
            @api_caller(api_url=new_url)
            def fetcher(data=None) -> dict|None:
                return data
            return fetcher()
            
        while True:
            data : dict|None = fetch_objects(api_url)
            if not data or data.get('metadata', {}).get('returnert', 0) == 0:
                break
            total_fetched += data.get('metadata', {}).get('returnert', 0)
            print(f"Total fetched: {total_fetched}")
            
            next_url : str = data.get('metadata', {}).get('neste', {}).get('href', "")
            if next_url == api_url or not next_url:
                break
            df_list.append(pd.json_normalize(data.get('objekter', [])))
            api_url: str = next_url
            
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
    instance = FeatureTypeDownloader(feature_type_id=591, environment='prod', inkluder='metadata,egenskaper', alle_versjoner="true")
    #print(instance.build_api_url())
    instance.download()
    instance.populate_columns(attributes=True, geometry_attribute_quality_parameters=False, relationships=False, road_reference=False, geometry=False)
    instance.export(file_name='vegobjekter_591_raw', file_type='excel')
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