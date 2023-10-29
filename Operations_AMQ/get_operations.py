import csv
import json
import re
import sys
import time
from collections import defaultdict
import java
import os
import logging
import subprocess
import zipfile
from javax.jms import Session
from org.apache.activemq import ActiveMQConnectionFactory

from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timedelta

# sys.path.append("java/javax.jms.jar")
# sys.path.append("java/activemq.jar")
# from javax.jms import Session
# from org.apache.activemq import ActiveMQConnectionFactory

# TODO change the way we get credentials
# user = "ren_analytics"
# password = "1?hnaOx$"

write_path=sys.argv[2]
to_ingest=sys.argv[3]
region=sys.argv[1]

RB_QUEUE = 'Consumer.CVIMS.VirtualTopic.operation_json_topic'

RENAULT_FILTER = ("93F", "93Y", "KNM",
                  "MX1", "NST", "UU1",
                  "VF1", "VF6", "VFA",
                  "VNVF", "VNVN", "X7L",
                  "Y9Z")

log_directory = "/home/cvims_etl/prod/logs/redbend/Operations_AMQ/"
log_filename = "extract_RB_Operations_"+region+"_"


def setup_logger():
    # Create the logs directory if it doesn't exist
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    file_handler = TimedRotatingFileHandler(os.path.join(log_directory, log_filename), when='midnight', interval=1, backupCount=0, encoding='utf-8', delay=False)
    file_handler.suffix = "%Y%m%d.log"
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def get_json_value(json_msg, column):
    try:
        value = str(json_msg[column])
        value = value.replace("\n", "\\n").replace("\r", "\\r")
    except Exception as e:
        logger.error("Error while parsing json", e)
        value = str()
    return value

def zip_file(source_file, output_zip):
    """
    :param source_file:
    :param output_zip:
    :return:
    """
    with zipfile.ZipFile(output_zip, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        file_name_inside_zip = source_file.split("/")[-1]
        zipf.write(source_file, arcname=file_name_inside_zip)


def send_file_to_bucket():
    """
    compress and send file to GCP bucket
    :return:
    """
    output_zipped_file_name = "%s.zip" % os.path.splitext(operation_obj.data_file.name)[0]  # our output
    zip_file(operation_obj.data_file.name, output_zipped_file_name)  # compress file
    logger.info("Sending %s to bucket" % output_zipped_file_name)
    start_time = time.time()
    try:
        command = ["python3", "bucket_transfer.py", output_zipped_file_name, operation_obj.data_file.name,to_ingest]
        output = subprocess.check_output(command, universal_newlines=True)
        logger.info("%s sent" % output_zipped_file_name)
        end_time = time.time()
        logger.info("took %.3f to send %s" % ((end_time - start_time),
                                              output_zipped_file_name))
    except Exception as e:
        logger.error("Something went wrong while sending the file")
        logger.error(e)


class ActiveMqConnection:
    def __init__(self):
        self.conn_factory = None
        self.conn = None
        self.sess = None
        self.dest = None
        self.consumer = None

        self.RBHOST = None
        self.RBUSER = None
        self.RBPASS = None

    def create_connection(self, queue=RB_QUEUE, session=Session.CLIENT_ACKNOWLEDGE):
        self.conn_factory = ActiveMQConnectionFactory(
                "tcp://" + self.RBHOST + ":61616?wireFormat.tightEncodingEnabled=false&jms.userName="
                + self.RBUSER + "&jms.password=" + self.RBPASS)
        self.conn = self.conn_factory.createConnection()
        # set up manual acknowledge mode after processing the message
        self.sess = self.conn.createSession(False, session)
        self.dest = self.sess.createQueue(queue)
        self.consumer = self.sess.createConsumer(self.dest)

    def get_credentials(self):
        region = sys.argv[1]
        RBCRED = os.environ['RBCRED_' + region]
        print(' : RBCRED = ' + RBCRED)
        # self.region = sys.argv[1]
        try:
            with open(RBCRED) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=';')
                for row in csv_reader:
                    self.RBHOST = row[0]
                    self.RBUSER = row[1]
                    self.RBPASS = row[2]
        except Exception as e:
            logger.error("csv file loading error %s" % RBCRED)
            logger.error(e)
            sys.exit()
        if not self.RBHOST or not self.RBUSER or not self.RBPASS:
            logger.error("RBCREDS contain null values")
            sys.exit()
        logger.info("Config loaded successfully")

    def __repr__(self):
        return str(self.consumer)

    def start_connection(self):
        self.conn.start()

    def stop_connection(self):
        self.conn.stop()


class Operation:
    def __init__(self):
        self.nbr_readers = 6
        self.readers = None
        self.consumer_message_ratio = defaultdict(int)  # dynamic int dict
        self.csv_writer = None
        self.error_count = 0
        self.msg_count = 0
        self.filtered_out = 0
        self.csv_index = 0
        self.msgValues_list = None
        self.msg = None
        self.data_file = None
        self.conn = None
        self.region = None
        self.filtered = False

    def connect_to_activemq(self):
        self.readers = []
        for _ in range(self.nbr_readers):
            reader = ActiveMqConnection()
            reader.get_credentials()
            reader.create_connection()
            logger.info("Client successfully connected")
            reader.start_connection()
            logger.info("Creating %s: " % reader)
            self.readers.append(reader)
            time.sleep(1)  # add some delay before starting the next reader


    def disconnect_all_readers(self):
        for reader in self.readers:
            try:
                reader.stop_connection()
            except java.lang.Exception as err:
                logger.error("Couldn't disconnect reader: %s" % reader)
                logger.error(err)

    def prepare_csv_file(self):

        self.csv_index += 1
        # definition of fields
        cols = ['vin', 'eventDate', 'messageDataType', 'eventType', 'id', 'creationTime', 'username', 'errorCode',
                'resultCode', 'status', 'action', 'campaignId', 'supplementaryId', 'updateContent', 'updates',
                'clientEventDate', 'updateTime', 'errorCodeDisplay', 'inventory', 'terminalInventory',
                'fullyInventoryInfo', 'statusKey', 'statusDisplay', 'actionDisplay', 'campaignRunnerId', 'policyId',
                'retryNumber', 'domain', 'additionalInfo', 'preliminaryReceivedData',
                'preliminaryDmCommandResultErrorCode', 'preliminaryDmCommandResultErrorMessage', 'updateResult',
                "brand", "family_code", "region_platform", "ins_timestamp", "upd_timestamp"]
        self.data_file = open(
            write_path+"Ope_" + self.region + "_" +
            datetime.now().strftime("%Y%m%d_%H%M%S") + "_%05d" % self.csv_index + ".csv", 'w')
        logger.info("Created new file %s" % self.data_file.name)
        self.csv_writer = csv.writer(self.data_file, lineterminator='\n', delimiter='$')
        self.csv_writer.writerow(cols)

    def process_mq_msg(self, consumer):
        self.filtered = False
        self.msg = consumer.receiveNoWait()
        if self.msg is None:
            return None
        try:
            self.consumer_message_ratio[consumer] += 1
            content = json.loads(self.msg.text, strict=False)
            vin = get_json_value(content, 'deviceId')
            if not vin.startswith(RENAULT_FILTER):
                self.filtered_out += 1
                self.filtered = True
                return
            eventDate = get_json_value(content, 'eventDate')
            messageDataType = get_json_value(content, 'messageDataType')
            eventType = get_json_value(content, 'eventType')
            id = get_json_value(content, 'id')
            creationTime = get_json_value(content, 'creationTime')
            username = get_json_value(content, 'username')
            errorCode = get_json_value(content, 'errorCode')
            resultCode = get_json_value(content, 'resultCode')
            status = get_json_value(content, 'status')
            action = get_json_value(content, 'action')
            campaignId = get_json_value(content, 'campaignId')
            supplementaryId = get_json_value(content, 'supplementaryId')
            updateContent = get_json_value(content, 'updateContent')
            updates = get_json_value(content, 'updates')
            clientEventDate = get_json_value(content, 'clientEventDate')
            updateTime = get_json_value(content, 'updateTime')
            errorCodeDisplay = get_json_value(content, 'errorCodeDisplay')
            inventory = get_json_value(content, 'inventory')
            terminalInventory = get_json_value(content, 'terminalInventory')
            fullyInventoryInfo = get_json_value(content, 'fullyInventoryInfo')
            statusKey = get_json_value(content, 'statusKey')
            statusDisplay = get_json_value(content, 'statusDisplay')
            actionDisplay = get_json_value(content, 'actionDisplay')
            campaignRunnerId = get_json_value(content, 'campaignRunnerId')
            policyId = get_json_value(content, 'policyId')
            retryNumber = get_json_value(content, 'retryNumber')
            domain = get_json_value(content, 'domain')
            preliminaryReceivedData = get_json_value(content, 'preliminaryReceivedData')
            preliminaryDmCommandResultErrorCode = get_json_value(content, 'preliminaryDmCommandResultErrorCode')
            preliminaryDmCommandResultErrorMessage = get_json_value(content, 'preliminaryDmCommandResultErrorMessage')
            updateResult = get_json_value(content, 'updateResult')
            additionalInfo = get_json_value(content, 'additionalInfo')
            brand = str()
            family_code = str()
            region_platform = str(self.region)
            ins_timestamp = str()
            upd_timestamp = str()

            self.msgValues_list = [vin, eventDate, messageDataType, eventType, id, creationTime, username, errorCode,
                                   resultCode, status, action, campaignId, supplementaryId, updateContent, updates,
                                   clientEventDate, updateTime, errorCodeDisplay, inventory, terminalInventory,
                                   fullyInventoryInfo, statusKey, statusDisplay, actionDisplay, campaignRunnerId,
                                   policyId, retryNumber, domain, preliminaryReceivedData,
                                   preliminaryDmCommandResultErrorCode, preliminaryDmCommandResultErrorMessage,
                                   updateResult, additionalInfo, brand, family_code, region_platform, ins_timestamp,
                                   upd_timestamp]
            self.msg_count += 1
        except Exception as e:
            logger.error("Received an empty message" + str(e))

    def write_mq_msg_to_csv(self):
        self.csv_writer.writerow(self.msgValues_list)
        self.msgValues_list = []

    def acknowledge_msg(self):
        self.msg.acknowledge()

    def log_stats(self):
        logger.debug("Total received within this session: " + str(self.msg_count))
        logger.debug("Total filtered within this session: " + str(self.filtered_out))
        logger.debug("Total errors within this session: " + str(self.error_count))
        formatted_dict = "\n".join(["consumer_%s   %d" % (re.search(r'value=(.*?),', str(key)).group(1), value)
                                    for key, value in dict(self.consumer_message_ratio).items()])
        logger.debug(formatted_dict)  # messages per consumer

    def close_opened_file(self):
        self.data_file.close()
        logger.info("Closing file " + self.data_file.name)


if __name__ == "__main__":

    consumers_sleep_time = 1
    nbr_null_message = 0
    chunk = 10000
    duration_secs = 300  # 5 min
    expiry_time = time.time() + duration_secs
    logger = setup_logger()
    logger.info("Starting with: chunk: %d, prepare csv every %d seconds"
                % (chunk, duration_secs))
    operation_obj = Operation()
    operation_obj.region = region
    operation_obj.connect_to_activemq()
    operation_obj.prepare_csv_file()
    logger.info("Waiting for messages ...")
    while True:
        try:
            for current_reader in operation_obj.readers:
                if operation_obj.msg_count == chunk or time.time() >= expiry_time:
                    logger.info("Starting new session, resetting stats")
                    operation_obj.log_stats()
                    operation_obj.close_opened_file()
                    # sending file to bucket
                    send_file_to_bucket()
                    # reset
                    operation_obj.prepare_csv_file()
                    expiry_time = time.time() + duration_secs
                    operation_obj.msg_count = 0
                    operation_obj.error_count = 0
                    operation_obj.filtered_out = 0
                operation_obj.process_mq_msg(current_reader.consumer)
                if operation_obj.msg is None:
                    nbr_null_message += 1
                    continue
                if operation_obj.filtered:
                    operation_obj.acknowledge_msg()
                    continue
                if operation_obj.msg_count % 100 == 0:
                    logger.info("Reached 100 operations")
                operation_obj.write_mq_msg_to_csv()
                operation_obj.acknowledge_msg()
            if nbr_null_message == operation_obj.nbr_readers:
                logger.info("sleeping for %d seconds - no activity on any consumers" % consumers_sleep_time)
                time.sleep(consumers_sleep_time)
            nbr_null_message = 0
        except (KeyboardInterrupt, Exception, java.lang.Exception) as e:
            logger.error("Closing connection ...")
            logger.error(e)
            operation_obj.disconnect_all_readers()
            operation_obj.close_opened_file()
            send_file_to_bucket()
            sys.exit()
