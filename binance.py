'''
Example updates for responds of orderbook:
{
  "e": "depthUpdate", // Event type
  "E": 123456789,     // Event time
  "s": "BNBBTC",      // Symbol
  "U": 157,           // First update ID in event
  "u": 160,           // Final update ID in event
  "b": [              // Bids to be updated
    [
      "0.0024",       // Price level to be updated
      "10"            // Quantity
    ]
  ],
  "a": [              // Asks to be updated
    [
      "0.0026",       // Price level to be updated
      "100"           // Quantity
    ]
  ]
}

Example Trade respond:
{
  "e": "trade",     // Event type
  "E": 123456789,   // Event time
  "s": "BNBBTC",    // Symbol
  "t": 12345,       // Trade ID
  "p": "0.001",     // Price
  "q": "100",       // Quantity
  "b": 88,          // Buyer order ID
  "a": 50,          // Seller order ID
  "T": 123456785,   // Trade time
  "m": true,        // Is the buyer the market maker?
  "M": true         // Ignore
}
'''

# install py-cpuinfo, openpyxl, xlrd
import logging
import websocket
import requests
import json
import sys
import gzip
import os
import boto3
import random
import signal
import psutil
import pandas
import socket
import cpuinfo

from datetime import datetime, timedelta
from time import time, sleep
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import RotatingFileHandler
from botocore.config import Config
from psutil import virtual_memory, cpu_count


class Exchange_pairs:
    xPairs = None  # List pairs
    xPairs_future = None  # List futures


class S3_params:
    aBacket = 'data-binance'
    aBacket_records_start = 'data-start-records'  # Bucket to save file with log info about Start/Stop events


class Constant:
    nPid_process = None  # PID of process when program will start. Will using for killing
    aLog_filename = 'binance.log'
    aLog_folder = 'log/'
    aMachine_name = None
    aExchange = 'Binance'
    aStat_file = 'statistic.xlsx'
    nSize_json_file = 40000  # 40000 - its size about 1M in json format
    oSubsr = {
        'book' : 'quote-streams',
        'trade': 'trade-streams'
    }
    oService = {
        'depthUpdate': 'book',
        'trade'      : 'trade'
    }
    nSnapshot_interval = 3600  # Min border in range of time interval for snapshot in seconds
    nLogfile_upload_interval = 600  # in seconds
    nSize_log_file = 10000000  # in bytes
    nInterval_update_pairs = 900  # in seconds
    oBreakline = '\n'.encode('utf-8')  # Breakline symbol using in saving data to file after each record


class Exchange_socket:
    oUrl_endpoint = {
        'future': 'wss://fstream.binance.com/ws/',  # For subscription of futures
        'pair'  : 'wss://stream.binance.com/ws'  # For subscription of spots
    }
    oUrl_orderbook_snapshot = {
        'pair'  : 'https://www.binance.com/api/v1/depth?symbol=',  # Url for request orderbook of pair
        'future': 'https://fapi.binance.com/fapi/v1/depth?symbol='  # Url for request orderbook of future
    }
    oUrl_exchange_info = {
        'future': 'https://fapi.binance.com/fapi/v1/exchangeInfo',  # List futures
        'pair'  : 'https://api.binance.com/api/v3/exchangeInfo'  # List pairs
    }
    oStream_service = {
        'book' : 'depth@100ms',  # Prefix using for websocket connection
        'trade': 'trade'
    }
    oSocket = []  # list of websocket connections

# Class which collecting hardware info
class Hardware_info:
    #Converting bytes to MB or GB
    # https://www.thepythoncode.com/article/get-hardware-system-information-python
    def __get_size(self, bytes, aSuffix="B"):
        """
        Scale bytes to its proper format
        e.g:
            1253656 => '1.20MB'
            1253656678 => '1.17GB'
        """
        nFactor = 1024
        for aUnit in ["", "K", "M", "G", "T", "P"]:
            if bytes < nFactor:
                return f"{bytes:.2f}{aUnit}{aSuffix}"
            bytes /= nFactor
        return

    def ram_size(self):  # Return RAM size total
        aRam = self.__get_size(virtual_memory().total)
        return aRam

    def cpu_name(self):  # Return CPU full info
        aCpu_name = None
        try:
            aCpu_name = cpuinfo.get_cpu_info()['brand_raw']
        except Exception as oErr:
            logging.error('Error in detecting CPU [%s]', oErr)
        return aCpu_name

    def core_count(self):  # Return number cores
        nCores = 0
        try:
            nCores = cpu_count(logical=False)
        except Exception as oErr:
            logging.error('Error 0085 in detecting number cores in CPU [%s]', oErr)
        return nCores

    def ip_address(self):  # Return IP address of machine
        #aHostname = socket.gethostname()
        #aIp_addr = socket.gethostbyname(aHostname)
        oSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            oSocket.connect(("8.8.8.8", 80))
            aIp_addr = oSocket.getsockname()[0]
            oSocket.close()
        except Exception as oErr:
            logging.critical('No internet connection [%s]', oErr)
            aIp_addr = '127.0.0.1'
            exit(1)
        return aIp_addr



# Checking time for pass last snapshot.  nDelta_snapshot - is interval between snapshots
def signal_snapshot(oTime, nDelta_snapshot):
    try:
        bSnapshot = False
        oTime_now = datetime.now()
        oDelta = oTime_now - oTime
        if oDelta.seconds >= Constant.nSnapshot_interval + nDelta_snapshot:
            bSnapshot = True
    except Exception as oErr:
        logging.critical('Error 0115 [%s]', oErr)
    return bSnapshot

# Creating payload for subscription with list of pairs
def create_payload(xPairs, nId):
    xParams = []
    for aPair in xPairs:
        aPair = str(aPair).lower()
        xParams.append(f'{aPair}@trade')
        xParams.append(f'{aPair}@depth@100ms')

    oPayload = {
        "method": "SUBSCRIBE",
        "params": xParams,
        "id"    : nId
    }
    return oPayload

# Create list of pairs where each item consist list of data
def prepare_list_records(xPairs_list):
    xRecord = {}
    try:
        for aPair in xPairs_list:
            aPair = aPair.replace('/', '')
            xRecord[aPair] = {
                'trade'           : [],
                'book'            : [],
                'time_snapshot'   : datetime.now(),  # time count which using for snapshot signal
                'delta_snapshot'  : timedelta(seconds=random.randint(0, len(xPairs_list))).seconds,
                'snapshot_session': None,  # Consist thread of request for snapshot
                'request_snapshot': False  # Signal for save snapshot data to rest list
            }
    except Exception as oErr:
            logging.critical('Error 0099 [%s]', oErr)
    return xRecord

# Creating websocket connection and subscription
# nIndex - index of connection in list of connection
def create_websocket_connection(xPairs, nIndex):
    while True:
        try:
            Exchange_socket.oSocket.append([])
            Exchange_socket.oSocket[nIndex] = websocket.create_connection(Exchange_socket.oUrl_endpoint['pair'])
            oPayload = create_payload(xPairs, nIndex)
            Exchange_socket.oSocket[nIndex].send(json.dumps(oPayload))
            logging.info('Pairs: [%s]', xPairs)
            return
        except Exception as oErr:
            logging.error('Error 0019 in connection [%s]', oErr)
    return

# Saving all data from all pairs in case start new day
def save_all_data(xRecord_all_pairs, xPairs, aType):
    for aPair in xPairs:
        create_and_save_file('book', aPair, xRecord_all_pairs[aPair]['book'], aType)
        create_and_save_file('trade', aPair, xRecord_all_pairs[aPair]['trade'], aType)
    return


# Main function for collecting data from websocket
def request_socket(xPairs, nIndex_session, aType):
    xRecord = prepare_list_records(xPairs)  # List where collecting data from websocket
    nDate_start = datetime.now().day  # For tracking change day
    create_websocket_connection(xPairs, nIndex_session)
    while True:
        try:
            try:
                if Exchange_socket.oSocket[nIndex_session] != []:
                    oWs_receive = Exchange_socket.oSocket[nIndex_session].recv()
                    #print(oWs_receive)
                    #continue
            except Exception as oErr:
                logging.error('Error in connection 0009 [%s]', oErr)
                create_websocket_connection(xPairs, nIndex_session)
                continue

            if oWs_receive is None:  # If respond is empty
                continue
            try:
                oRespond_json = json.loads(oWs_receive)
                aPair = oRespond_json['s']
                aService = Constant.oService[oRespond_json['e']]  # 'book' or 'trade'
            except Exception:
                continue

            try:
                bSave = signal_save_file(nDate_start)  # Signal of change day
                if bSave:
                    ThreadPoolExecutor().submit(save_all_data, xRecord, xPairs, 'pair')
                    nDate_start = datetime.now().day
                    del xRecord
                    xRecord = prepare_list_records(xPairs)
            except Exception as oErr:
                logging.critical('Error 0022 in saving all data [%s]', oErr)

            oRespond_with_timestamp = process_respond(oRespond_json)  # Add timestamp to data
            xRecord[aPair][aService].append(oRespond_with_timestamp)
            if aService == 'book':
                if xRecord[aPair]['request_snapshot'] == True:
                    logging.info('SNAPSHOT request for [%s] [%s]', aPair, aType)
                    xRecord[aPair]['snapshot_session'] = ThreadPoolExecutor().submit(request_snapshot, aPair, aType)
                    xRecord[aPair]['time_snapshot'] = datetime.now()

                # Check is interval for snapshot is pass
                xRecord[aPair]['request_snapshot'] = signal_snapshot(xRecord[aPair]['time_snapshot'], xRecord[aPair]['delta_snapshot'])

                if xRecord[aPair]['snapshot_session'] is not None:
                    # Checking thread of request snapshot
                    # We not want wait result of thread and lost stream data so we just check thread is finished or not
                    if xRecord[aPair]['snapshot_session'].done():
                        xRecord[aPair]['book'].append(process_respond(xRecord[aPair]['snapshot_session'].result()))
                        logging.info('SNAPSHOT add for [%s]', aPair)
                        xRecord[aPair]['snapshot_session'] = None

            # print(sys.getsizeof(xRecord))
            if sys.getsizeof(xRecord[aPair][aService]) >= Constant.nSize_json_file:
                ThreadPoolExecutor().submit(create_and_save_file, aService, aPair, xRecord[aPair][aService], aType)
                xRecord[aPair][aService] = []
        except Exception as oErr:
            n = exception_info()
            logging.critical('Error 0023 line [%s] [%s]', n, oErr)
    return


def exception_info():
    exc_type, exc_obj, tb = sys.exc_info()
    lineno = tb.tb_lineno
    del exc_type, exc_obj, tb
    return lineno


# Saving log file
def save_log_files():
    aFolder_log = Constant.aLog_folder
    if Constant.aMachine_name is not None:
        aFolder_log = f'{aFolder_log}{Constant.aMachine_name}/'
    aSource = f'{aFolder_log}{Constant.aLog_filename}'
    aSource_1 = f'{aFolder_log}{Constant.aLog_filename}.1'
    while True:
        try:
            sleep(Constant.nLogfile_upload_interval)  # Delay in saving logfile
            oS3 = s3_resource()
            oS3.Bucket(S3_params.aBacket).upload_file(Constant.aLog_filename, aSource)
            if os.path.isfile(f'{Constant.aLog_filename}.1'):  # if second logfile exist then we upload too
                oS3.Bucket(S3_params.aBacket).upload_file(f'{Constant.aLog_filename}.1', aSource_1)
                logging.info('Log file saved to S3 [%s]', aSource_1)
            logging.info('Log file saved to S3 [%s]', aSource)
            del oS3
        except Exception as oErr:
            nLine = exception_info()
            logging.critical('Error [%s] in saving log file on line [%s]', oErr, nLine)
    return


# aService - book or trade
# aType  - futures or pair
def create_and_save_file(aService, aPair, xRecord, aType):
    try:
        if xRecord == []:  # If xRecord is empty then we create record with timestamp
            oRecord = process_respond([])
            xRecord.append(oRecord)
            del oRecord
        oFile = create_file(aPair, xRecord[-1], aService, aType)
        save_file(oFile, xRecord, aType, aService, aPair)
        del oFile, xRecord
    except Exception as oErr:
        nLine = exception_info()
        logging.critical('Error [%s] on line [%]', oErr, nLine)
    return

# Return True if day was changed
def signal_save_file(nDay_start):
    bSave = False
    nNow = datetime.now().day
    nDelta = nNow - nDay_start
    if nDelta != 0:
        bSave = True
        logging.info('NEW day start..')
    del nNow
    return bSave


# @profile
def save_file(oFile, xRecord, aType, aService, aPair):
    try:
        for oRecord in xRecord:
            oData_json = json.dumps(oRecord)
            oData_byte = oData_json.encode('utf-8')
            oFile.write(oData_byte)
            oFile.write(Constant.oBreakline)
        aFilename = oFile.name
        oFile.close()
        logging.info('Save [%s] records to file [%s]', len(xRecord), aFilename)
        aPath = create_path_file(xRecord, aType, aService, aPair)
        save_to_s3(aFilename, aPath)
        del oFile, aFilename, aPath
    except Exception as oErr:
        logging.critical('Error 0278 in function for saving file [%s]', oErr)
    return


# Upload to S3
def save_to_s3(aFilename, aPath):
    while True:
        try:
            aSource = f'{aPath}/{aFilename}'
            oS3 = s3_resource()
            oS3.Bucket(S3_params.aBacket).upload_file(aFilename, aSource)
            logging.info('File [%s] was upload to S3', aSource)
            break
        except Exception as oErr:
            logging.critical('Error 0011 [%s] [%s] [%s]', oErr, aFilename, aSource)
            sleep(1)
            del oS3
    os.remove(aFilename)
    del oS3, aSource
    return


# Creating S3 resource object
def s3_resource():
    while True:
        try:
            oConfig_custom = Config(max_pool_connections=1000)  # To avoid limit connection error
            oResurce_s3 = boto3.resource('s3', config=oConfig_custom)
            return oResurce_s3
        except Exception as oErr:
            logging.critical('Error 0006 [%s]', oErr)
    return

# aSubscr - (book or trade)
# aType - (pair or future)
def create_path_file(xRecords, aType, aSubscr, aPair):
    try:
        oLocal_time = datetime.now()
        if len(xRecords) > 0:
            oItem = xRecords[0]  # Use first record for parse date
            oLocal_time = datetime.fromtimestamp(oItem['local_timestamp'] / 1000)
        if aType == 'future':
            aPair = 'Future_' + aPair
        aPath = f'{Constant.oSubsr[aSubscr]}/pair={aPair}/year={oLocal_time.year}/month={oLocal_time.month:02}/day={oLocal_time.day:02}'
        if Constant.aMachine_name is not None:
            aPath = f'{aPath}/{Constant.aMachine_name}'
        del oLocal_time, oItem
    except Exception as oErr:
        logging.critical('Error 0324 in function creating path [%s]', oErr)
    return aPath


# Add local timestamp to stream data
def process_respond(oRespond):
    oRespond_with_timestamp = {
        'local_timestamp': int(time() * 1000),  # timestamp in milisec resolution
        'raw_data'       : oRespond
    }
    return oRespond_with_timestamp


# @profile
# Requesting snapshot of book
# aType is pair or future
def request_snapshot(aPair, aType):
    aUrl_snapshot = f'{Exchange_socket.oUrl_orderbook_snapshot[aType]}{aPair}&limit=1000'
    while True:
        try:
            oRequest = requests.get(aUrl_snapshot)
            oSnapshot_json = json.loads(oRequest.content)
            oRequest.close()
            del aUrl_snapshot, oRequest
            return oSnapshot_json
        except Exception as oErr:
            logging.error('Error 0355 in requesting snapshot by [%s] [%s]', aUrl_snapshot, oErr)
            sleep(random.randint(1, 10))
    return


def create_file(aPair, oItem, aService, aType):
    while True:
        try:
            nTimestamp = int(oItem['local_timestamp']) / 1000
            oTime = datetime.fromtimestamp(nTimestamp)
            aDate_time = f'{oTime.date()}T{oTime.time()}'
            oFile = gzip.GzipFile(f'{aPair}-{aDate_time}-{aService}-{aType}.gz', 'w')
            logging.info('Create file [%s]', oFile.name)
            del nTimestamp, oTime, aDate_time
            return oFile
        except Exception as oErr:
            logging.critical('Error [0030] in creating file [%s]', oErr)
    return


# Checking for new instruments and start collecting
def renew_pairs_list():
    while True:
        sleep(Constant.nInterval_update_pairs)
        logging.info('Checking for new pairs..')

        xPairs_new = request_list_pairs('pair')
        sleep(1)  # Delay for avoid ban
        xPairs_future_new = request_list_pairs('future')

        #xPairs_future_new = ['BTCUSDT', 'BCHUSDT']
        #xPairs_new = ['BTCUSDT', 'LTCBTC']

        # Check difference between old list pairs and new one
        try:
            xNew_future_pairs = list(set(xPairs_future_new) - set(Exchange_pairs.xPairs_future))  # Detecting new futures
            # Detecting futures which not exist anymore
            xPairs_delete_future = list(set(Exchange_pairs.xPairs_future) - set(xPairs_future_new))

            # Detecting new pairs and not existed
            xNew_pairs = list(set(xPairs_new) - set(Exchange_pairs.xPairs))
            xPairs_delete = list(set(Exchange_pairs.xPairs) - set(xPairs_new))
        except Exception as oErr:
            logging.critical('Error 0383 in function for checking new pairs [%s]', oErr)

        if xPairs_delete != []:
            logging.info('DELETE [%s] pairs from list', xPairs_delete)
            for aPair in xPairs_delete:
                Exchange_pairs.xPairs.remove(aPair)

        if xPairs_delete_future != []:
            logging.info('DELETE [%s] futures from list', xPairs_delete_future)
            for aPair in xPairs_delete_future:
                Exchange_pairs.xPairs_future.remove(aPair)

        if xNew_future_pairs != []:
            nSessions_number = len(Exchange_socket.oSocket)
            ThreadPoolExecutor().submit(request_socket, xNew_future_pairs, nSessions_number, 'future')
            Exchange_pairs.xPairs_future = Exchange_pairs.xPairs_future + xPairs_future_new
            logging.info('ADDED new future subscribtions for [%s]', xNew_future_pairs)

        if xNew_pairs != []:  # If new pairs founded then start thread with new pairs
            nSessions_number = len(Exchange_socket.oSocket)
            ThreadPoolExecutor().submit(request_socket, xNew_pairs, nSessions_number, 'pair')
            Exchange_pairs.xPairs = Exchange_pairs.xPairs + xNew_pairs
            logging.info('ADDED new pairs subscribtions for [%s]', xNew_pairs)
        del xPairs_new, xPairs_future_new, xNew_pairs, xNew_future_pairs
    return

# Request list of instruments
def request_list_pairs(aType):
    xPair = []
    try:
        oFull_info = requests.get(Exchange_socket.oUrl_exchange_info[aType])
        oFull_info_json = json.loads(oFull_info.content)
        oFull_info.close()

        for oPair in oFull_info_json['symbols']:
            if oPair not in xPair:  # Avoid dublicate
                xPair.append(oPair['symbol'])
        del oFull_info, oFull_info_json
    except Exception as oErr:
        logging.critical('Error 0434 in requesting pairs. [%s] [%s]. So we will use old list', oErr, oFull_info_json)
        if aType == 'pair':
            xPair = Exchange_pairs.xPairs
        if aType == 'future':
            xPair = Exchange_pairs.xPairs_future
    return xPair

# Function divide list of pairs and create list of lists with pairs. Divide on 100 pairs
# Ex from ['A', 'B', 'C'] to [['A', 'B'], ['C']]
def create_pairs_args(xPairs):
    xArgs = []
    nIndex_first = 0
    for nIndex in range(100, (len(xPairs)), 100) :
        xArgs.append(xPairs[nIndex_first:nIndex])
        nIndex_first = nIndex
    xArgs.append(xPairs[nIndex:])
    return xArgs

def shutdown_program(a, b):
    logging.warning('[%s] got cancel signal and STOP...', Constant.aExchange)
    update_status_file('Stop')  # Make record with 'Stop' events
    os.kill(Constant.nPid_process, signal.SIGKILL)  # Using KILL signal because we can have running several threads
    return

# Reading data from XLS files
def read_stat_file():
    oFile_read_xls = pandas.ExcelFile(Constant.aStat_file, engine='openpyxl')
    xRecords = oFile_read_xls.parse()
    oFile_read_xls.close()
    return xRecords

# Converting dict to DateFrame object
def create_xls_record(aEvent):
    oRecord = {
        'Event'    : [aEvent],
        'Time'     : str(int(time() * 1000)),
        'Collector': Constant.aMachine_name,
        'Exchange' : Constant.aExchange,
        'IP'       : hardware.ip_address(),
        'VM ID'    : '',
        'RAM'      : hardware.ram_size(),
        'CPU'      : hardware.cpu_name(),
        '#Cores'   : hardware.core_count()
    }
    oRecord_df = pandas.DataFrame(oRecord)
    return oRecord_df

# Function updating log file of start/stop events and upload to S3
def update_status_file(aEvent):
    oS3 = s3_resource()
    try:
        oS3.Bucket(S3_params.aBacket_records_start).download_file(Constant.aStat_file, Constant.aStat_file)
    except Exception as oErr:
        logging.warning('Look like file [%s] not exist in bucket [%s] [%s]', Constant.aStat_file, S3_params.aBacket_records_start, oErr)
    oXls_writer = pandas.ExcelWriter(Constant.aStat_file, mode='w')
    oRecord_new_df = create_xls_record(aEvent)
    if os.path.isfile(Constant.aStat_file):  # if files exist we reading records and add new record
        xRecords = read_stat_file()
        xRecords_new = xRecords.append(oRecord_new_df)
    else:
        xRecords_new = oRecord_new_df  # If trying download file was failed then we creating new file with one record
    oDf_to_excel = pandas.DataFrame(xRecords_new)
    oDf_to_excel.to_excel(oXls_writer, index=False)
    oXls_writer.save()
    oXls_writer.close()
    oS3.Bucket(S3_params.aBacket_records_start).upload_file(Constant.aStat_file, Constant.aStat_file)
    logging.info('File [%s] upload to S3 bucket [%s]', Constant.aStat_file, S3_params.aBacket_records_start)
    os.remove(Constant.aStat_file)
    del oS3
    return



Constant.nPid_process = psutil.Process().pid  # Saving PID of process this program
oStream = logging.StreamHandler()
oStream.setLevel(logging.INFO)
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        RotatingFileHandler(Constant.aLog_filename, maxBytes=Constant.nSize_log_file, backupCount=1),
        oStream
    ],
    format='%(asctime)s : %(levelname)s : %(message)s')

logging.info('Start [%s]  at [%s] on machine [%s]', Constant.aExchange, Constant.aMachine_name, int(time() * 1000))

hardware = Hardware_info()
update_status_file('Start')  # Making record about 'Start' event
signal.signal(signal.SIGTERM, shutdown_program)
signal.signal(signal.SIGINT, shutdown_program)
signal.signal(signal.SIGABRT, shutdown_program)
signal.signal(signal.SIGHUP, shutdown_program)

xPairs_main = request_list_pairs('pair')
xPairs_future_main = request_list_pairs('future')
#xPairs_future_main = ['BTCUSDT']
#xPairs_main = ['BTCUSDT']
Exchange_pairs.xPairs = xPairs_main
Exchange_pairs.xPairs_future = xPairs_future_main

xArgs_spot = create_pairs_args(xPairs_main)
#xArgs_spot = [['BTCUSDT']]

with ThreadPoolExecutor(len(xArgs_spot) + 4) as oThread:
    for nIndex, xArg_pair in enumerate(xArgs_spot):
        oThread.submit(request_socket, xArg_pair, nIndex, 'pair')

    oThread.submit(request_socket, xPairs_future_main, nIndex + 1, 'future')
    logging.info('Future pairs [%s]', xPairs_future_main)
    oThread.submit(save_log_files)
    oThread.submit(renew_pairs_list)
