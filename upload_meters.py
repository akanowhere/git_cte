import json
import requests as req

dexma_url = "https://is3.dexcell.com/readings?source_key=Testing_123&x-dexcell-source-token=e06ca4e1fb863c26e7a4"

dexma_params = {
    "EN":     402,
    "EN15":   40202,
    "AG":     901,
    "AGL15": 50402,
}

meter_list = [

    "SF_43F6CD_AGL15_DCC_SF",
    "SF_4502E5_AGL15_DCC_SF"

]

ts_list = [
            "2022-01-01T00:00:00-03:00",
            "2022-01-01T00:15:00-03:00",
            "2022-01-01T00:30:00-03:00",
            "2022-01-01T00:45:00-03:00"
        ]

for medidor in meter_list:

    params = str(medidor).split("_")
    var = dexma_params[params[2]]

    for ts in ts_list:
        upload = "[{\"did\": \"" + str(medidor) + \
                "\", \"sqn\": 1" + \
                ", \"ts\": " + "\"" + ts + "\"" + \
                ", \"values\":[{\"p\": " + str(var) + \
                ", \"v\": " + str(1) + \
                " }]}]\n"

        print(req.request("POST",
                        dexma_url,
                        headers={'content-type': 'application/json',
                                'x-dexcell-source-token': 'e06ca4e1fb863c26e7a4'},
                        data=upload).reason
            )