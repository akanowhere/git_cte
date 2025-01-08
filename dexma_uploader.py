
'''
HÁ UMA LIMITAÇÃO DE REQUISIÇÕES DIÁRIAS NA PLATAFORMA DO DEXMA. DESSA
MANEIRA, É PRECISO ARMAZENAR E ATUALIZAR A DATA DA ÚLTIMA MEDIÇÃO LOCALMENTE
(AQUI EU FAÇO POR JSON EM UMA PASTA) - https://developers.dexma.com/#rate-limiting
'''

'''
IMPORTAÇÃO DE LIBS
'''

# Comuns
import requests as req
import datetime
import time
import os
import shutil
import json
import urllib3
import sys

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Threads
import threading
from concurrent.futures import ThreadPoolExecutor

# Customizadas
from src import methods as mt
from src import messagelog as ml
from src import saveTemp
from src import sf as sf
'''
VARIÁVEIS GLOBAIS
'''

# Variáveis de controle
tempo_entre_chamadas = 600  # Tempo em segundos
dexma_url = "https://is3.dexcell.com/readings?source_key=Testing_123&x-dexcell-source-token=e06ca4e1fb863c26e7a4"
dexma_token = "05b4880757ff1cb56d38"
PATH_NAME = ""
APAGAR_ARQUIVOS_TEMPORARIOS = False

# Controle de Threads
lock = threading.Lock()
#METER_WORKERS = 10
METER_WORKERS = 30

# Listas de controle
water_meter_names = ["Hid_", "_HREU", "_HPOT"]
predios_omnieletronica = ["Edifício Castelo"]
# predios_outro_fornecedor = []

# APIs
dexma_url = "https://is3.dexcell.com/readings?source_key=Testing_123&x-dexcell-source-token=e06ca4e1fb863c26e7a4"

'''
FUNÇÕES
'''


def locations():
    # Retrieve Location
    dexma_locations = "https://api.dexcell.com/v3/locations"
    location_ids = req.get(dexma_locations,
                           headers={'content-type': 'application/json',
                                    'x-dexcell-token': '05b4880757ff1cb56d38'}).json()

    # print(len(location_ids), json.dumps(location_ids, indent=4))
    data = {}

    'Searching just for location id, excluding datasets'

    for i in range(len(location_ids)):
        if 'name' in location_ids[i]:
            data[location_ids[i]['name']] = {'id': location_ids[i]['id']}

    return data


def location_meters(data):
    for location in data:
        data[location]['meters'] = {}

        # Retrieve Device ID Linked To it
        devices = "https://api.dexcell.com/v3/locations/" + str(data[location]['id']) + "/devices"
        ret = req.get(devices,
                      headers={'content-type': 'application/json',
                               'x-dexcell-token': '05b4880757ff1cb56d38'}).json()

        for i in range(len(ret)):
            print(ret[i]['id'])
            data[location]['meters'][ret[i]['id']] = {}

    return data


def meters_info(data):
    for location in data:
        for meter in data[location]['meters']:
            # Retrieve Meter Info
            devices = "https://api.dexcell.com/v3/devices/" + str(meter)
            ret = req.get(devices,
                          headers={'content-type': 'application/json',
                                   'x-dexcell-token': '05b4880757ff1cb56d38'}).json()
            print(ret)
            data[location]['meters'][meter] = {
                "name": ret['name'],
                "description": ret['description'],
                "local_id": ret['local_id'],
                "id": ret['id']
            }

    return data


def meters_last_reading(data):
    for location in data:
        for meter in data[location]['meters']:
            print(meter)

            for desc in water_meter_names:
                if desc in data[location]['meters'][meter]['local_id']:
                    meter_type = "WATERVOL"
                    break
                else:
                    meter_type = "EACTIVE"

            if "15_" in data[location]['meters'][meter]['local_id']:
                operation = "AVG"
            else:
                operation = "DELTA"

            print(meter_type)
            devices = "https://api.dexcell.com/v3/readings/last?device_id=" + \
                      str(meter) + \
                      "&parameter_key=" + meter_type + \
                      "&resolution=QH" + \
                      "&operation=" + operation

            print(devices)
            ret = req.get(devices, headers={'content-type': 'application/json',
                                            'x-dexcell-token': '05b4880757ff1cb56d38'})

            if ret.status_code == 200:
                ret = ret.json()
                try:
                    data[location]['meters'][meter]['last_data'] = ret['values'][0]['ts']
                except:
                    data[location]['meters'][meter]['last_data'] = 0

    return data


def get_static_data():
    # Step 1: Get Locations
    data = locations()
    # Save in file
    with open("./data/dexma_infos/" + "control" + ".json", "w") as jsonFile:
        json.dump(data, jsonFile, indent=4, sort_keys=True, ensure_ascii=False)

    # Step 2: Get Meters for Each Location
    with open("./data/dexma_infos/control.json", "r+") as jsonFile:
        data = json.load(jsonFile)
        data = location_meters(data)
        jsonFile.seek(0)
        json.dump(data, jsonFile, indent=4, sort_keys=True, ensure_ascii=False)
        jsonFile.truncate()
        jsonFile.close()

    # Step 3: Get Info for Each Meter
    with open("./data/dexma_infos/control.json", "r+") as jsonFile:
        data = json.load(jsonFile)
        data = meters_info(data)
        jsonFile.seek(0)
        json.dump(data, jsonFile, indent=4, sort_keys=True, ensure_ascii=False)
        jsonFile.truncate()
        jsonFile.close()

    # Step 4: Get Last Data Timestamp for Each Meter
    with open("./data/dexma_infos/control.json", "r+") as jsonFile:
        data = json.load(jsonFile)
        data = meters_last_reading(data)
        jsonFile.seek(0)
        json.dump(data, jsonFile, indent=4, sort_keys=True, ensure_ascii=False)
        jsonFile.truncate()
        jsonFile.close()


def create_path(path_name):
    baseDir = os.getcwd()
    csvDir = baseDir + "/" + path_name
    if not os.path.exists(csvDir):
        os.makedirs(csvDir)


def uploadData(file1):
    try:
        if "_OE" not in file1:
            file1 = open(file1, 'r')
            Lines = file1.readlines()
            file1.close()
            ml.messageLog(
                "[INFO] Uploading " + str(Lines.__len__()) + " lines of data... [" + file1.name.split("/")[4] + "]")
            for line in Lines:
                ret = req.request("POST",
                                  dexma_url,
                                  headers={'content-type': 'application/json',
                                           'x-dexcell-source-token': 'e06ca4e1fb863c26e7a4'},
                                  data=line.strip("\n"))
                # print(ret.status_code, ret.reason)
        else:
            file1 = open(file1, 'r')
            Lines = reversed(file1.readlines())
            file1.close()
            ml.messageLog("[INFO] Uploading " + file1.name.split("/")[4])
            for line in Lines:
                ret = req.request("POST",
                                  dexma_url,
                                  headers={'content-type': 'application/json',
                                           'x-dexcell-source-token': 'e06ca4e1fb863c26e7a4'},
                                  data=line.strip("\n"))
                print(ret.status_code, ret.reason)

        return True

    except:
        ml.messageLog("[ERROR] Uploading data from " + str(file1.name.split("/")[4]))
        return False


def prepareToUpload():
    try:
        ml.messageLog("[INFO] Preparing to upload")
        entries = os.listdir(PATH_NAME)
        # print(entries)   # saber de onde são carregados os arquivos
        with ThreadPoolExecutor(max_workers=METER_WORKERS) as executor:
            for entry in entries:
                executor.submit(uploadData, PATH_NAME + entry)
    except:
        ml.messageLog("[ERROR] Preparing to upload")
    finally:
        ml.messageLog("[INFO] Upload finished...")
        if APAGAR_ARQUIVOS_TEMPORARIOS:
            try:
                shutil.rmtree(PATH_NAME)
            except:
                ml.messageLog("[ERROR] Deleting path...")


'''
MAIN                
'''
#sys.stdout = open('D:/pythonDSA/CTE/dexma/data/log.txt', 'w')
#get_static_data()

while True:

    create_path("./data/")
    PATH_NAME = "./data/tempData/" + datetime.datetime.now().strftime("%d%m%Y%H%M%S") + "/"
    saveTemp.PATH_NAME = PATH_NAME
    create_path(PATH_NAME)
    create_path("./data/log/")
    ml.messageLog("[INFO] Start!")

    with open("./data/dexma_infos/control.json", "r+") as jsonFile:
        data = json.load(jsonFile)
        #print('VER LOCAITONS AQUI',data)     #####ver os locations aqui
        for location in data:
            #print('VER LOCAITONS AQUI', location,'DATALCATOIONS', data[location]['meters'])
            for meter in data[location]['meters']:
                '''if "_CK" in data[location]['meters'][meter]['local_id']:
                    try:
                        ret = mt.cclapa_query(data[location]['meters'][meter]['local_id'],
                                              data[location]['meters'][meter]['last_data'])
                        data[location]['meters'][meter]['last_data'] = ret
                    except:
                        continue'''

                '''if "_OE" in data[location]['meters'][meter]['local_id']:
                    try:
                        ret = mt.omnieletronica_query(data[location]['meters'][meter]['local_id'],
                                                      data[location]['meters'][meter]['last_data'])
                        data[location]['meters'][meter]['last_data'] = ret
                    except:
                        continue '''

            if 'Faria Lima' in location:
                for meter in data[location]['meters']:
                    if "_CK" not in data[location]['meters'][meter]['local_id'] or \
                            "_OE" not in data[location]['meters'][meter]['local_id']:
                        try:
                            if data[location]['meters'][meter]['last_data'] != 0:
                                ret = mt.vfl_query(data[location]['meters'][meter]['local_id'],
                                                   data[location]['meters'][meter]['last_data'])
                                # print(ret)
                                data[location]['meters'][meter]['last_data'] = ret
                        except:
                            continue

            '''if 'Maua' in location:
                for meter in data[location]['meters']:
                    if "_CK" not in data[location]['meters'][meter]['local_id'] or \
                            "_OE" not in data[location]['meters'][meter]['local_id']:
                        try:
                            if data[location]['meters'][meter]['last_data'] != 0:
                                ret = mt.vma_query(data[location]['meters'][meter]['local_id'],
                                                   data[location]['meters'][meter]['last_data'])
                                # print(ret)
                                data[location]['meters'][meter]['last_data'] = ret
                        except:
                            continue'''
            '''if 'DCC - Cajamar' in location:
                try:
                    for meter in data[location]['meters']:
                        if "_SF" in data[location]['meters'][meter]['local_id']:
                            print('VER AQUI DATETIME TYPE',data[location]['meters'][meter]['local_id'],data[location]['meters'][meter]['last_data'])
                            print('VER AQUI DATETIME TYPE',type(data[location]['meters'][meter]['last_data']))
                            ret = sf.SIGFOX_BACKEND_QUERY(data[location]['meters'][meter]['local_id'],data[location]['meters'][meter]['id'],data[location]['meters'][meter]['last_data'])
                            data[location]['meters'][meter]['last_data'] = ret
                except:
                    continue'''
            # if 'DCC - Cajamar' in str(location):
            #if 'SF_' in str(location):
                #SIGFOX_BACKEND_QUERY(str(meterReference), str(meterId), initialDate)



        jsonFile.seek(0)
        json.dump(data, jsonFile, indent=4, sort_keys=True, ensure_ascii=False)
        jsonFile.truncate()
        jsonFile.close()

    prepareToUpload()

    ml.messageLog("[INFO] Waiting " + str(int(tempo_entre_chamadas / 60)) + " minutes")
    time.sleep(tempo_entre_chamadas)
#sys.stdout.close()

