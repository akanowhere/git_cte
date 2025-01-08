from asyncio.windows_events import NULL
from contextlib import nullcontext
import requests as req
import datetime
import time
import xml.etree.ElementTree as ET
import json
import urllib3
from requests.auth import HTTPBasicAuth
from src import saveTemp
from src import messagelog as ml

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# APIs
vflWebserviceMed = "http://179.191.77.38:9191/obix/histories/AtilioModbus/"
vmaWebserviceMed = "http://187.102.146.226:9191/obix/histories/"
cckwebWebserviceList = "https://telemetria.cclapa.com.br/webservice/?idk=c445aa4eadb3b8f31fde0f4e08c99ff0&mod=lst"
cckwebWebserviceMed = "https://telemetria.cclapa.com.br/webservice/?idk=c445aa4eadb3b8f31fde0f4e08c99ff0&mod=dem15"
omnieletronicaAPI = 'http://www.ami-hub.com/api/'
sigfox_meters                       = "https://api.sigfox.com/v2/devices/"
SIGFOX_MESSAGES                     = "https://api.sigfox.com/v2/devices/"



# Autenticações
vflAuthorization = HTTPBasicAuth('cte', 'Cte1234567')
vmaAuthorization = HTTPBasicAuth('cte', 'Cte1234567')
sigfox_auth                         = HTTPBasicAuth("6101639c988a375ce2098199", "42d0402ee46a7da1a617132f667797be")


# OmniEletronica params
token_inf = 'XIaJ6kMaLbAOaV66RfaeILBcngDlAvuqHR28yVQD0do'
token_vca = 'BHG3JqGZfqNu2RnAIX2GtjwJQKrBV-NxGeQ9ov8l1Co'
token_eld = 'sopMRPYw09wUvXQ5UWld0KH0hakdL-AIqRYhUcYC9Ko'
token_cas = 'ugAPuWtYPdiEGCc-5KslkReiuuEA_82lh81e1YXFsIg'

# Dexma Parameters
dexma_params = {
    "EN": 402,
    "EN15": 40202,
    "AG": 901,
    "AGL15": 50402,
    "PBAR": 923,
    "STS": 501,
    "TAA": 805,
    "TRA": 806,
    "SPTEM": 304,
    "TEMP": 301,
    "TEMP": 301,
    "SPTEM": 304,
    "TAA": 805,
    "TRA": 806,
    "FLOW": 902,
    "POT": 401,
    "VGAS": 419,
    "VOLG": 504,
    "ENG": 505,
    "POTG": 506,
    "UMI": 302,
    "CO2": 307,
    "CEE": 550,
    "CDE": 551,
    "PPSI": 924,
    "USO": 161,
    "EFF": 499,
    "CONT": 502,
    "COP": 807,
    "TINS": 813,
    "SPTINS": 814,
    "PINS": 815,
    "SPPINS": 816,
    "TAR": 819,
    "PAR": 820,
    "SPPAR": 821,
    "DAMP": 825,
    "V3V": 824,
    "OCU": 121,
    "FCV": 913,
}


def vfl_query(meter, last_ts):
    if "MM_" in meter or "Hid_" in meter or "Irrigacao_" in meter:
        print("[INFO] Start VFL!" + ' ' + meter)

        if last_ts == 0:
            after = "2022-11-23T00:00:00-03:00"
        else:
            #after = last_ts
            after = "2022-11-23T00:00:00-03:00"

        datetimenow = datetime.datetime.now()
        tomorrow_datetime = datetimenow + datetime.timedelta(days=1)
        #before = tomorrow_datetime.strftime("%Y-%m-%dT23:59:00.000-03:00")
        before = "2021-11-24T00:00:00-03:00"

        if "Hid" not in meter and "Irrigacao" not in meter:
            # print("ENERGIA!\n")
            if "Pv" in meter:
                url = vflWebserviceMed + \
                      meter + "$20Energia_Ativa_Positiva_Leed/~historyQuery?start=" + \
                      after + "&end=" + before + "&content-type=text/xml"
            else:
                url = vflWebserviceMed + \
                      meter + "$20Consumo_Ativo_Leed/~historyQuery?start=" + \
                      after + "&end=" + before + "&content-type=text/xml"
        else:
            # print("HIDROMETRO!\n")
            url = vflWebserviceMed + \
                  meter + "/~historyQuery?start=" + \
                  after + "&end=" + before + "&content-type=text/xml"

        resp = req.get(url, auth=vflAuthorization)
        # print(resp)
        if resp.status_code == 400:
            return False

        root = ET.fromstring(resp.text)

        timestamp = []
        consumption = []

        loop = 0
        for elem in root.iter("{http://obix.org/ns/schema/1.0}abstime"):
            if 'timestamp' in elem.attrib['name']:
                if elem.get('val') is not None:
                    timestamp.append(elem.get('val'))
                    # print(timestamp[loop])
                    loop = loop + 1

        loop = 0
        for elem in root.iter("{http://obix.org/ns/schema/1.0}real"):
            if elem.get('val') is not None:
                consumption.append(elem.get('val'))
                # print(consumption[loop])
                loop = loop + 1

        for j in range(len(timestamp)):
            data = {}
            data['did'] = meter
            data['sqn'] = 1
            data['ts'] = timestamp[j][:19] + \
                         timestamp[j][-6:]  # Removendo os milésimos
            data['values'] = []
            if "Hid" not in meter and "Irrigacao" not in meter:
                data['values'].append({
                    'p': 402,
                    'v': float(consumption[j]),
                })
            else:
                data['values'].append({
                    'p': 901,
                    'v': float(consumption[j]),
                })

            for k in range(len(data['values'])):
                var = data["values"][k]["p"]

            # UPLOAD DE DADOS
            saveTemp.saveTempData(meter,
                                  timestamp[j][:19] + timestamp[j][-6:],
                                  var,
                                  float(consumption[j]))

        last_ts = timestamp[len(timestamp) - 1]

        return last_ts


def vma_query(meter, last_ts):
    if "MM_" in meter or "Hid_" in meter or "Irrigacao_" in meter:
        print("[INFO] Start VFL!" + ' ' + meter)

        if last_ts == 0:
            after = "2021-11-23T00:00:00-03:00"
        else:
            after = last_ts
            after = "2022-11-23T00:00:00-03:00"

        datetimenow = datetime.datetime.now()
        tomorrow_datetime = datetimenow + datetime.timedelta(days=1)
        before = tomorrow_datetime.strftime("%Y-%m-%dT23:59:00.000-03:00")
        #before = tomorrow_datetime.strftime("2022-11-23T23:59:00.000-03:00")

        network = 'SaoBentoModbus/' if "MBU" in meter else 'SaoBentoBacnet/'

        url = vmaWebserviceMed + network + meter + "/~historyQuery?start=" + after + "&end=" + before

        resp = req.get(url, auth=vmaAuthorization)
        # print(resp)
        if resp.status_code == 400:
            return False

        root = ET.fromstring(resp.text)

        timestamp = []
        consumption = []

        # loop = 0
        for elem in root.iter("{http://obix.org/ns/schema/1.0}abstime"):
            if 'timestamp' in elem.attrib['name']:
                if elem.get('val') is not None:
                    timestamp.append(elem.get('val'))
                    # print(timestamp[loop])
                    # loop = loop + 1

        # loop = 0
        for elem in root.iter("{http://obix.org/ns/schema/1.0}real"):
            if elem.get('val') is not None:
                consumption.append(elem.get('val'))
                # print(consumption[loop])
                # loop = loop + 1

        for j in range(len(timestamp)):
            data = {}
            data['did'] = meter
            data['sqn'] = 1
            data['ts'] = timestamp[j][:19] + \
                         timestamp[j][-6:]  # Removendo os milésimos
            data['values'] = []
            if "Hid" not in meter and "Irrigacao" not in meter:
                data['values'].append({
                    'p': 402,
                    'v': float(consumption[j]),
                })
            else:
                data['values'].append({
                    'p': 901,
                    'v': float(consumption[j]),
                })

            for k in range(len(data['values'])):
                var = data["values"][k]["p"]

            # UPLOAD DE DADOS
            saveTemp.saveTempData(meter,
                                  timestamp[j][:19] + timestamp[j][-6:],
                                  var,
                                  float(consumption[j]))

        last_ts = timestamp[len(timestamp) - 1]

        return last_ts


def omnieletronica_query(meter, last_ts):

        def convertToISO(ts):
            ts = str(ts).replace(" ", "T")
            ts = (datetime.datetime.fromisoformat(ts) - datetime.timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:00.000-03:00")
            return ts

        def convertToISOdexma(ts):
            ts = str(ts).replace(" ", "T")
            ts = (datetime.datetime.fromisoformat(ts) - datetime.timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:00-03:00")
            return ts

        try:
            params = str(meter).split("_")

            if params[3] == "INF":
                token = token_inf
            if params[3] == "VCA":
                token = token_vca
            if params[3] == "CAS":
                token = token_cas
            if params[3] == "ELD":
                token = token_eld

            var = dexma_params[params[2]]

            if last_ts == 0 or last_ts == None:
                #after = "2022-04-01T00:00:00-03:00"
                after = "2022-12-01T00:00:00-03:00"

            else:
                #after = last_ts
                after = "2022-12-01T00:00:00-03:00"
            # after = "2022-10-05T00:00:00-03:00"

            datetimenow = datetime.datetime.now()
            tomorrow_datetime = datetimenow + datetime.timedelta(days=5)
            #before = tomorrow_datetime.strftime("%Y-%m-%dT23:59:00-03:00")
            before = "2022-12-04T00:00:00-03:00"

            url = omnieletronicaAPI + token + '/data2/' + params[0] + "/" + params[1] + \
                  '/?count=1000&start=' + after + '&end=' + before

            tmp = req.get(url).json()

            last_ts = after
            try:
                for data in tmp['payload'][params[0]][params[1]]:

                    if (convertToISO(data['measure_at']) > convertToISO(after)) and (
                            convertToISO(data['measure_at']) != last_ts):
                        saveTemp.saveTempData(meter, convertToISOdexma(data['measure_at']), var, float(data['value']))
                        last_ts = convertToISO(data['measure_at'])

                    else:
                        print("ERRO")
            except:
                ml.messageLog("[ERROR] Creating TEMP archive from " + meter)

            return last_ts
        except:
            ml.messageLog("[ERROR] Invalid meter " + meter)


def cclapa_query(meter, last_ts):
    def convert_datetime_cclapa(date):
        # Dexma to CCLapa
        return datetime.datetime.fromisoformat(str(date)).strftime('%Y%m%d')

    med = str(meter).split("_")

    if last_ts == 0 or last_ts == NULL:
        after = "2022-11-01T00:00:00-03:00"
    else:
        #after = last_ts
        after = "2022-11-01T00:00:00-03:00"

    datetimenow = datetime.datetime.now()
    tomorrow_datetime = datetimenow + datetime.timedelta(days=1)
    before = tomorrow_datetime.strftime('%Y%m%d')
    #before = "2022-11-24T00:00:00-03:00"

    url = cckwebWebserviceMed + "&nsmed=" + str(med[0]).replace("-", "_") + "&dti=" + convert_datetime_cclapa(
        after) + "&dtf=" + before
    print(url)
    resp = req.get(url)
    # print(resp.text)
    if resp.status_code == 400:
        return False

    root = ET.fromstring(resp.text)

    last_ts = after
    try:
        for medicao in root.findall('med'):
            if medicao.find("data") != None:
                dataStr = medicao.find("data").text[6:8] + "/" + medicao.find("data").text[4:6] + "/" + medicao.find(
                    "data").text[:4] + " " + medicao.find("hora").text
                dataStr = datetime.datetime.strptime(dataStr, "%d/%m/%Y %H:%M").strftime("%Y-%m-%dT%H:%M:00-03:00")
                if dataStr > after:
                    saveTemp.saveTempData(meter, dataStr, str(dexma_params[med[1]]),
                                          (float(medicao.find('kw').text)) / 4)
                    last_ts = dataStr

    except:
        ml.messageLog("[ERROR] Creating TEMP archive from " + med[0])
        return 0

    return last_ts


