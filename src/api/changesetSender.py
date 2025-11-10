import requests
from bs4 import BeautifulSoup
import json
import uuid

def authenticate(username : str, password : str, miljø : str, x_client : str) -> str:
    match miljø:
        case 'test':
            base_url = "https://nvdbapiskriv.test.atlas.vegvesen.no/"
        case 'stm':
            base_url = "https://nvdbapiskriv-stm.utv.atlas.vegvesen.no/"
        case 'utv':
            base_url = "https://nvdbapiskriv.utv.atlas.vegvesen.no/"
        case _:
            base_url = "https://nvdbapiskriv.atlas.vegvesen.no/"

    url = f"{base_url}rest/v1/oidc/authenticate"
    payload = {
        "username": username,
        "password": password,
        "realm": "EMPLOYEE"
        }
    headers = {
        "Content-Type": "application/json",
        "X-Client": x_client
        }
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'xml')
        id_token = soup.find('idToken')
        if id_token:
            return id_token.text
    return ""

class Changeset:
    def __init__(self, path : str, miljø : str, id_token : str, x_client : str, dryrun : bool):
        self.X_request_ID = str(uuid.uuid4())
        match miljø:
            case 'test':
                self.base_url = "https://nvdbapiskriv.test.atlas.vegvesen.no/"
            case 'stm':
                self.base_url = "https://nvdbapiskriv-stm.utv.atlas.vegvesen.no/"
            case 'utv':
                self.base_url = "https://nvdbapiskriv.utv.atlas.vegvesen.no/"
            case _:
                self.base_url = "https://nvdbapiskriv.atlas.vegvesen.no/"
        with open(path, 'r') as file:
            self.endringssett = json.load(file)
        self.id_token = id_token
        self.x_client = x_client

        if dryrun:
            self.dryrun = "JA"
        else:
            self.dryrun = "NEI"
    
    def validate(self) -> bool:
        url = f"{self.base_url}rest/v3/endringssett/validator"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.id_token}",
            "X-Client": self.x_client,
            "X-Request-ID": self.X_request_ID
            }
        payload = self.endringssett
        
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            response = response.json()
            fremdrift = response.get("fremdrift")
            avvist_årsak = response.get("avvistårsak", "Ingen avvist årsak")

            if fremdrift == "UTFØRT":
                return True
            print(avvist_årsak)
        return False
    
    def register(self) -> bool:
        url = f"{self.base_url}rest/v3/endringssett"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.id_token}",
            "X-Client": self.x_client,
            "X-Request-ID": self.X_request_ID,
            "X-NVDB-DryRun-NoLocking": self.dryrun
            }
        payload = self.endringssett
        
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 201:
            response = response.json()
            self.start_behandling_url = next((i.get("src") for i in response if i.get("rel") == "start"), False)
            return True
        return False
    
    def start(self) -> bool:
        url = self.start_behandling_url
        if not url:
            return False
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.id_token}",
            "X-Client": self.x_client,
            "X-Request-ID": self.X_request_ID
        }
        response = requests.post(url, headers=headers) # type: ignore
        if response.status_code == 202:
            return True
        return False
