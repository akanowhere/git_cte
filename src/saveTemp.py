from src import messagelog as ml
import threading
lock = threading.Lock()

PATH_NAME = ""


'''
Método generateFile(fileBasename, acumulador)
  - Criação do arquivo no formato CSV, é recebido como parâmetro o nome do arquivo e seu conteúdo
'''


def saveTempData(meter, timestamp, var, consumption):

    try:

        meterData = "[{\"did\": \"" + str(meter) + \
                    "\", \"sqn\": 1" + \
                    ", \"ts\": \"" + str(timestamp) + \
                    "\", \"values\":[{\"p\": " + str(var) + \
                    ", \"v\": " + str(consumption) + \
                    " }]}]\n"
        print(meterData)
        lock.acquire()
        file = open(PATH_NAME + str(meter) + ".tmp", 'a')
        file.write(meterData)
        file.close()
        lock.release()
        return True
    except:
        ml.messageLog("[ERROR] Generating file", True)
        return False