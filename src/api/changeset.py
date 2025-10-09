import requests
import json

def get_current_data_catalogue_version() -> str:
    r = requests.get("https://nvdbapiles.atlas.vegvesen.no/datakatalog/api/v1/versjon")
    if r.status_code == 200:
        r = r.json()
        return r.get("versjon", "2.41")
    return "2.41"

def ensure_unique_objects(func):
    def wrapper(self, *args, **kwargs):
        nytt_obj = func(self, *args, **kwargs)
        in_list = any(obj.get("nvdbId") == nytt_obj.get("nvdbId") for obj in self.objects)
        if not in_list:
            self.objects.append(nytt_obj)
        else:
            raise Exception(f"Found a duplicate of {nytt_obj.get("nvdbId")}-{nytt_obj.get("versjon")} in the changeset.")
    return wrapper

def ensure_unique_objectversion(func):
    def wrapper(self, *args, **kwargs):
        nytt_obj = func(self, *args, **kwargs)
        in_list = any(obj.get("nvdbId") == nytt_obj.get("nvdbId") and obj.get("versjon") == nytt_obj.get("versjon") for obj in self.objects)
        if not in_list:
            self.objects.append(nytt_obj)
    return wrapper

class Changeset:
    def __init__(self, typeId : int):
        self.data_catalogue_version = get_current_data_catalogue_version()
        self.typeId = typeId
        self.objects : list[dict] = []

    def save_json(self, path : str) -> bool:
        if ".json" not in path:
            path = path + ".json"
        changeset = {
            self.__class__.__name__ [0].lower() + self.__class__.__name__[1:]: {
                "vegobjekter" : self.objects
            },
            "datakatalogversjon" : self.data_catalogue_version
        }
        try:
            with open(path, "w") as fp:
                json.dump(changeset, fp, indent=4, ensure_ascii=False)
            return True
        except Exception:
            return False
        
class Registrer(Changeset):
    def __init__(self, typeId : int):
        super().__init__(typeId)

class Oppdater(Changeset):
    def __init__(self, typeId : int):
        super().__init__(typeId)

    @ensure_unique_objectversion
    def add_object(self, nvdbId : int, versjon : int, stedfesting : dict|bool = False, egenskaper : list|bool = False):
        pass

class Lukk(Changeset):
    def __init__(self, typeId : int):
        super().__init__(typeId)

    @ensure_unique_objects
    def add_object(self, nvdbId : int, versjon : int, kaskade : bool, lukkedato : str) -> dict:
        return {
            "lukkedato" : lukkedato,
            "kaskadelukking" : "JA" if kaskade else "NEI",
            "typeId" : self.typeId,
            "nvdbId" : nvdbId,
            "versjon" : versjon
        }

class Gjenopprett(Changeset):
    def __init__(self, typeId : int):
        super().__init__(typeId)

    @ensure_unique_objects
    def add_object(self, nvdbId : int, versjon : int, kaskade : bool) -> dict:
        return {
            "kaskadegjenoppretting" : "JA" if kaskade else "NEI",
            "typeId" : self.typeId,
            "nvdbId" : nvdbId,
            "versjon" : versjon
        }

class Korriger(Changeset):
    def __init__(self, typeId : int):
        super().__init__(typeId)

class Fjern(Changeset):
    def __init__(self, typeId : int):
        super().__init__(typeId)

    @ensure_unique_objectversion
    def add_object(self, nvdbId : int, versjon : int, kaskade : bool) -> dict:
        return {
            "kaskadefjerning" : "JA" if kaskade else "NEI",
            "typeId" : self.typeId,
            "nvdbId" : nvdbId,
            "versjon" : versjon
        }

class DelvisOppdater(Oppdater):
    pass

class DelvisKorriger(Korriger):
    pass

if __name__ == "__main__":
    test = Lukk(50)
    test.add_object(12342, 1, True, "2020-10-10")
    #print(test.objects)
    test.add_object(12342, 2, False, "2020-10-10")
    #print(test.objects)
    test.save_json("test")