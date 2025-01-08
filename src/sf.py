#from datetime import datetime

from datetime import datetime
from datetime import datetime, timedelta
import requests as req
import urllib3
from requests.auth import HTTPBasicAuth
#import datetime
import xml.etree.ElementTree as ET
import json
import urllib3
from requests.auth import HTTPBasicAuth
from src import saveTemp
from src import messagelog as ml




sigfox_meters = "https://api.sigfox.com/v2/devices/"
SIGFOX_MESSAGES = "https://api.sigfox.com/v2/devices/"
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


sigfox_auth = HTTPBasicAuth("6101639c988a375ce2098199", "42d0402ee46a7da1a617132f667797be")




#tempo = 0
#print(type(tempo))

#tempo1 = str(tempo)
#print(type(tempo1))
#initialDate = '2021/01/01 00:00:00'

#date_time_str = '18-09-19 01:55:19'

#date_time_obj = datetime.strptime(date_time_str, '%d-%m-%y %H:%M:%S')
#date_time_obj1 = datetime.strptime(date_time_str, '%y-%m-%d %H:%M:%S')

#"%d/%m/%Y %H:%M:%S"

#"%y/%m/%d %H:%M:%S"

#print(type(initialDate))

#date_str = '09-19-2018 01:55:19'
#date_object = datetime.strptime(date_str, '%m-%d-%Y %H:%M:%S').date()
#print(type(date_object))
#print(date_object)  # printed in default formatting

#date = datetime.strptime(initialDate, '%y/%m/%d %H:%M:%S')

#print(date)
#x = datetime.datetime.fromisoformat(str(initialDate)).strftime('%Y%m%d')
#data = datetime.datetime.strptime(initialDate, '%Y%m%d')
#initialDate = datetime.datetime.strftime(initialDate, '%Y%m%d')
#after = initialDate.strftime("%Y-%m-%dT00:00:00.000-03:00")
#y = datetime.strptime(x, '%d/%m/%Y').date()
#print(type(date_time_obj))
#print(type(date_time_obj1))
#print(date_time_obj)
#print(date_time_obj1)

#print(y)

#x = int(initialDate.timestamp()) * 1000


def SIGFOX_BACKEND_QUERY(med, desc, initialDate):
    print(type(initialDate))
    print('VER SIGFOX_BACKEND_QUERY AQUI MED SIG FOX',med, desc, initialDate)
    if initialDate == 0 or initialDate == None:
        x = "2021-01-01 00:00:00"
        #initialDate = x
        initialDate = datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
        print(type( initialDate))
    else:
        initialDate = initialDate

    # Set Query Dates
    default = 1625108400 * 1000  # Default date 2021-07-01T00:00:00-03:00
    now = (datetime.now().timestamp()) * 1000
    initialDate = int(initialDate.timestamp()) * 1000
    # yesterday = ((datetime.now() - timedelta(days=1)).timestamp()) * 1000
    #SF_HID_4439B4 2022-10-30 10:22:48
    # QS-A-4P01-M_43FE58_AGL15_VCA_SF
    # Create temporary dict to store timestamp and consumption
    temp_dict = {'ts': [], 'data': []}

    # Check if it's Water or Energy meter
    if "HID" in med:
        device_type = "Water"
    else:
        device_type = "Energy"

    # Check if it have data in Wattics and set query Dates

    #params = str(med).split("_")
    #params = QS-A-4P01-M_43FE58_AGL15_VCA_SF

    params = str(med).split("_")

    if initialDate < 1:
        sigfox_call = sigfox_meters + \
                      str(med).replace("SF_", "").replace("HID_", "").replace("ENR_", "") + \
                      "/messages?since=" + str(default) + \
                      "&before=" + str(int(now))
        print('VER SIGFOX CALL',sigfox_call)
    else:
        sigfox_call = sigfox_meters + \
                      str(params[1]) + \
                      "/messages?since=" + str(default) + \
                      "&before=" + str(int(now))
        print('VER SIGFOX CALL', sigfox_call)


    response = req.get(sigfox_call, auth=sigfox_auth).json()
    print(response)
    # Check if the last Sigfox TS is newer than previous query
    if response['data'][0]['time'] > initialDate:
        # Let's read all response data
        # The TS returned from Sigfox is EPOCH in miliseconds and GMT
        for j in range(len(response['data'])):
            #print('VER AQUI DADOS SIG FOX',j,response['data'])
            print(j, (datetime.utcfromtimestamp(response['data'][j]['time'] / 1000) - timedelta(
                hours=3)).isoformat(), response['data'][j]['data'], response['data'][j]['seqNumber'])
            print("Direct Pulses: ", int(response['data'][j]['data'][:8], 16))
            # Store disponible ts and data
            temp_dict['ts'].append(response['data'][j]['time'])
            temp_dict['data'].append(int(response['data'][j]['data'][:8], 16))

        # Sigfox returns newer measures first, so we need to reverse our lists
        temp_dict['ts'] = list(reversed(temp_dict['ts']))
        temp_dict['data'] = list(reversed(temp_dict['data']))

        for k in range(len(response['data'])):
            pC_1 = temp_dict['data'][k]
            aP_1 = 504
            datahora = datetime.utcfromtimestamp(
                (temp_dict['ts'][k] / 1000) - 10800).isoformat() + "-03:00"
            saveTemp.saveTempData(str(med), datahora, aP_1, pC_1)



#SF_HID_44375E 24759 2022-10-28 07:35:31
#FOX SF_HID_43FDC6 26165 2022-10-28 07:08:54
#SF_HID_43F7CB 11867 2022-10-28 06:48:23




#med = 'SF_HID_44375E'
#desc = 24759
#initialDate = 0
#print(type(initialDate))
#initialDate = datetime.strptime(initialDate, '%Y-%m-%d %H:%M:%S')
#print(initialDate)

#SIGFOX_BACKEND_QUERY(med, desc, initialDate)

#datetime_string = '2022-04-05 09:07:06'
#datetime_object = datetime.strptime(datetime_string, '%Y-%m-%d %H:%M:%S')
#print(datetime_object, type(datetime_object))
#initialDate = datetime.strptime(initialDate, '%Y-%m-%d %H:%M:%S')
#print(initialDate, type(initialDate))