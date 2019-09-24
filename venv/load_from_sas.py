
from requests.auth import HTTPBasicAuth
from requests import Session
from zeep import Client, Settings
from zeep.transports import Transport
import json
from datetime import datetime, timedelta
from kafka import KafkaProducer
from lxml import etree
import sys, os



################## variable#########################################
serviceType = ''   #skat_detaches  skat_stops   kasant_failures  skat_passport
pathname = os.path.dirname(sys.argv[0])

def main(args):
    global serviceType
    serviceType = args
    print("The input line is: " + args)

if __name__ == '__main__':
    para = str(sys.argv[1])
    main(para)

##############################################################
#don't work relative path, only global while executing in console
##############################################################
def conf_read_parameters(node_name, type_data='one'):
    node_list = {}
    conf_data = open(pathname+"\conf.json")
    data = json.load(conf_data)
    conf_data.close()

    if type_data == 'dict':
        for row in data[node_name]:
            node_list = row
    else:
        node_list = data[node_name]

    return node_list


############################################################
def save_to_file(path_file, msg, write_type='append'):

    if write_type == 'append':
        fl = open(path_file, 'a')
        fl.writelines(msg +'\n')
    else:
        fl = open(path_file, 'w')
        fl.writelines(msg)

    fl.close()

############################################################
def read_file(path_file):
    f = open(path_file)
    msg = f.read()
    f.close()

    return msg

############################################################
kafka_server = conf_read_parameters('kafkaServer')
paramList = conf_read_parameters(serviceType, 'dict')


##################################### get data from WEB service
def get_soap_data(msg_request):
    session = Session()
    session.auth = HTTPBasicAuth(username=login, password=password)
    cnn_settings = Settings(strict=True, xml_huge_tree=True)

    client = Client(wsdl=cnn_url, transport=Transport(session=session), settings=cnn_settings)

    with client.settings(strict=False, raw_response=True, xml_huge_tree=True):
        saop_data = client.service[serviceName](**msg_request)

    # print saop_data
    # print saop_data.content

    return saop_data.content

####################################################################################
topic = paramList['topic']
cnn_url = paramList['url']
password = paramList['pwd']
login = paramList['user']
path_save = paramList['rootPath']
serviceName = paramList['serviceName']
xmlStart = paramList['xmlStart']
templateRequest = paramList['templateRequest']
roadsID = paramList['roadID']
incType = paramList['increment']
lastDateFile = path_save + serviceType + '\\last_load_date'  ## in linux change \\ to //
sessionLogFile = path_save + serviceType + '\\session_log'  ## in linux change \\ to //
errorLogFile = path_save + serviceType + '\\error_log'  ## in linux change \\ to //
dateFrom = read_file(lastDateFile)
lastLoadDate = 'null'

def get_date_to (dt_from, inc_type):
    get_date_to.now = datetime.now()
    get_date_to.str_dt_to = ''
    get_date_to.delta = datetime

    if inc_type == 'hours':
        get_date_to.delta = get_date_to.now - (dt_from + timedelta(hours=1))
    elif inc_type == 'days':
        get_date_to.delta = get_date_to.now - (dt_from + timedelta(days=1))


    if inc_type == 'hours' and (get_date_to.delta.seconds/60 > 60 and get_date_to.delta.seconds/60 < 115):
        get_date_to.str_dt_to = (dt_from + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S')
    elif inc_type == 'hours' and (get_date_to.delta.seconds/60 > 135):
        get_date_to.str_dt_to = (get_date_to.now.replace(minute=0, second=0, microsecond=0) - timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%S')

    if inc_type == 'days' and (get_date_to.delta.days*24 > 24 and get_date_to.delta.days*24 < 47):
        get_date_to.str_dt_to = (dt_from + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S')
    elif inc_type == 'days' and (get_date_to.delta.days*24 > 52):
        get_date_to.str_dt_to = (get_date_to.now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%S')

    if get_date_to.str_dt_to == '' or get_date_to.str_dt_to == None:
        get_date_to.str_dt_to = dt_from

    # print get_date_to.str_dt_to

    return get_date_to.str_dt_to

########################################################
def create_request(msg_request, road_id, str_dt_from, inc_type):
    # print msg_request
    create_request.str_dt_to = ''
    global lastLoadDate
    request = msg_request.copy()
    dt_from = datetime.strptime(str_dt_from, '%Y-%m-%dT%H:%M:%S')

    create_request.str_dt_to = get_date_to(dt_from, inc_type)

    for key, val in msg_request.items():
        if val == 'roadID':
            request[key] = road_id
        elif val == 'dateFrom':
            request[key] = (dt_from + timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%S')
        elif val == 'dateTo':
            request[key] = create_request.str_dt_to

    lastLoadDate = str(create_request.str_dt_to)

    # print request
    return request


########################################################

save_to_file(sessionLogFile, 'start session - ' + datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
for roadID in roadsID:
    print roadID
    msgRequest = create_request(templateRequest, roadID, dateFrom, incType)

    saop_response = get_soap_data(msgRequest)

    fl = open('C:\\SAStmp\\test.xml', 'w')
    fl.writelines(saop_response)
    fl.close()

    parser = etree.XMLParser(remove_blank_text=True, ns_clean=True, remove_comments=True, remove_pis=True, resolve_entities=True)
    xml_tbl = etree.XML(saop_response, parser)

    xmlForParse = xml_tbl.find(xmlStart)
    print xmlForParse

    if xmlForParse == None:
        save_to_file(errorLogFile, 'Error Web Service road id: ' + str(roadID) + ', Response -  ' + saop_response + ', request parameters: ' + str(msgRequest).encode('utf-8'))
        save_to_file(sessionLogFile, 'EROOR LOAD ROAD ID: ' + str(roadID) + ' more information in Error_log')
    else:
        print '-------------'
        try:
            producer = KafkaProducer(bootstrap_servers=kafka_server)
            count = 0
            for row in xmlForParse.getchildren():
                count += 1
                # print str(etree.tostring(row, encoding='utf-8', method='html'))
                producer.send(topic=topic.encode('utf-8'), value=str(etree.tostring(row, encoding='utf-8', method='html')), key=dateFrom + ' | ' + lastLoadDate)
            print lastLoadDate
            save_to_file(path_file=lastDateFile, msg=lastLoadDate, write_type='rewrite')
            save_to_file(sessionLogFile, 'ROAD ID:'  + str(roadID) + ' request parameters: ' + str(msgRequest).encode('utf-8') + ' count message in package - ' + str(count))
        except Exception:
            print ' ---- error load to kafka - ' + topic
            print lastDateFile
            print Exception.message

        finally:
            producer.close()

save_to_file(sessionLogFile, 'end session - ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

