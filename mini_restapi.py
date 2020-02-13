import requests
import json
import signal
import argparse
import logging, os, platform
import csv
import time

#########Logging configuration##########
here = os.path.abspath(os.path.dirname(__file__))
if platform.platform().startswith('Windows'):
    logging_file = os.path.join(here, os.path.splitext(os.path.basename(__file__))[0]+'.log')
else:
    logging_file = os.path.join(here, os.path.splitext(os.path.basename(__file__))[0]+'.log')
log_formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(threadName)-9s : %(message)s')
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

fileHandler = logging.FileHandler(logging_file)
fileHandler.setFormatter(log_formatter)
fileHandler.setLevel(logging.DEBUG)
LOGGER.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(log_formatter)
consoleHandler.setLevel(logging.DEBUG)
LOGGER.addHandler(consoleHandler)
#########Logging configuration ends##########
__version__ = "1.2"


rt = 'http://192.168.56.102:8008'

# define a flow dictionary
flow_dict = {}
flow_dict['tcp'] = {'keys': 'ipsource,ipdestination,tcpsourceport,tcpdestinationport',
               'value': 'frames',
               'log': True}
flow_dict['udp'] = {'keys': 'ipsource,ipdestination,udpsourceport,udpdestinationport',
               'value': 'frames',
               'log': True}
flow_dict['icmp'] = {'keys':'ipsource,ipdestination,icmptype,icmpcode',
                'value':'frames',
                'log':True}
flow_dict['arp'] = {'keys':'arpmacsender,arpipsender,arpmactarget,arpiptarget',
                'value':'frames',
                'log':True}

CSV_HEADER = ['ipsource','ipdestination','sourceport','destinationport','protocol name']

def sig_handler(signal,frame):
    for name in flow_dict.keys():
        requests.delete(rt + '/flow/' + name + '/json')
        pass
    exit(0)


signal.signal(signal.SIGINT, sig_handler)


def main(args):
    if args.url:
        rt = args.url

    # define all the flows
    for name, flow in flow_dict.items():
        try:
            logging.info("Defining flow for "+name)
            r = requests.put(rt + '/flow/' + name + '/json', json=flow, timeout=5)
        except:
            logging.error("some exception occurred here")

    flowurl = rt + '/flows/json?maxFlows=100&timeout=' + str(args.time)
    flowID = -1

    logging.info("Waiting for flows...")
    while True:
        try:
            r = requests.get(flowurl + "&flowID=" + str(flowID), timeout=args.time)
            if r.status_code != 200: break
            flows = r.json()
            if len(flows) == 0:
                logging.info("No data received")
                continue

            flowID = flows[0]["flowID"]
            flows.reverse()

            with open('flows'+str(flowID)+'.csv', 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(CSV_HEADER)
                for f in flows:
                    row = str(f['flowKeys']).split(',')
                    row.append(f['name'])
                    writer.writerow(row)
                    print(str(f['flowKeys']) + ',' + f['name'])

            time.sleep(args.sleep)
        except:
            logging.error("some exception occurred")


if __name__ == "__main__":
    """
    Execution starts here.
    """
    parser = argparse.ArgumentParser(description='Mininet Rest api.')
    parser.add_argument('-u', '--url', help='Rest api url', type=str, default='http://192.168.56.102:8008')
    parser.add_argument('-t', '--time', help='Flow time', type=int, default=30)
    parser.add_argument('-s', '--sleep', help='Sleep time', type=int, default=60)

    args = parser.parse_args()

    main(args)
