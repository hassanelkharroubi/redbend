import csv
import json
import re
import sys
import time
from collections import defaultdict
from datetime import datetime
import java
import os
import zipfile
import logging
import subprocess
from logging.handlers import TimedRotatingFileHandler

# sys.path.append("java/javax.jms.jar")
# sys.path.append("java/activemq.jar")

from javax.jms import Session
from org.apache.activemq import ActiveMQConnectionFactory

# TODO change the way we get credentials
# user = "ren_analytics"
# password = "1?hnaOx$"


region=sys.argv[1]
write_path=sys.argv[2]
to_ingest=sys.argv[3]

RB_QUEUE = 'Consumer.CVIMS.VirtualTopic.campaign_json_topic'

RENAULT_FILTER = ("93F", "93Y", "KNM",
                  "MX1", "NST", "UU1",
                  "VF1", "VF6", "VFA",
                  "VNVF", "VNVN", "X7L",
                  "Y9Z")


log_filename = "extract_RB_campaign_"+region+"_"
# log_directory = "/coredrive/campaigns_stg/logs/"
log_directory="/home/cvims_etl/prod/logs/redbend/Campaigns_AMQ/"


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
    output_zipped_file_name = "%s.zip" % os.path.splitext(campaign_obj.data_file.name)[0]  # our output
    zip_file(campaign_obj.data_file.name, output_zipped_file_name)  # compress file
    
    logger.info("Sending %s to bucket" % output_zipped_file_name)
    start_time = time.time()
    try:
        command = ["python3", "bucket_transfer.py", output_zipped_file_name, campaign_obj.data_file.name,to_ingest]
        output = subprocess.check_output(command, universal_newlines=True)
        logger.info("%s sent " % output_zipped_file_name)
        end_time = time.time()
        logger.info("took %.3f to send %s" % ((end_time - start_time),
                                              output_zipped_file_name))
    except Exception as e:
        logger.error("something went wrong while sending the file")
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


class Campaign:
    def __init__(self):
        self.region = None

        self.nbr_readers = 6
        self.readers = None
        self.consumer_message_ratio = defaultdict(int)  # dynamic int dict

        self.csv_writer = None
        self.error_count = 0
        self.msg_count = 0
        self.csv_index = 0
        self.filtered_out = 0
        self.filtered = False
        self.msgValues_list = None
        self.msg = None
        self.data_file = None
        self.conn = None

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
        cols = ['eventDate', 'username', 'eventType', 'id', 'runnerId', 'name', 'creationTime', 'startTime', 'endTime',
                'completedTime', 'status', 'statusDisplay', 'priority', 'type', 'isOngoing', 'action',
                'actionDisplay', 'domain', 'deviceDetails', 'domainDetails', 'modelDetails',
                'networkLoad', 'downloadTimeSlot', 'serverInitiated', 'numberOfRetries', 'updateContent', 'stageLevel',
                "region_platform", "brand", "model", "ins_timestamp", "upd_timestamp"]
        self.data_file = open(
            write_path +"camp_" + self.region + "_"+datetime.now().strftime("%Y%m%d_%H%M%S") + "_%05d" % self.csv_index + ".csv", 'w')
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
            vin = get_json_value(content, 'deviceDetails').split("\t")[1]
            if not vin.startswith(RENAULT_FILTER):
                self.filtered_out += 1
                self.filtered = True
                return
            eventDate = get_json_value(content, 'eventDate')
            username = get_json_value(content, 'username')
            eventType = get_json_value(content, 'eventType')
            rid = get_json_value(content, 'id')
            runnerId = get_json_value(content, 'runnerId')
            name = get_json_value(content, 'name')
            creationTime = get_json_value(content, 'creationTime')
            startTime = get_json_value(content, 'startTime')
            endTime = get_json_value(content, 'endTime')
            completedTime = get_json_value(content, 'completedTime')
            status = get_json_value(content, 'status')
            statusDisplay = get_json_value(content, 'statusDisplay')
            priority = get_json_value(content, 'priority')
            ctype = get_json_value(content, 'type')
            isOngoing = get_json_value(content, 'isOngoing')
            action = get_json_value(content, 'action')
            actionDisplay = get_json_value(content, 'actionDisplay')
            domain = get_json_value(content, 'domain')
            deviceDetails = get_json_value(content, 'deviceDetails')
            domainDetails = get_json_value(content, 'domainDetails')
            modelDetails = get_json_value(content, 'modelDetails')
            networkLoad = get_json_value(content, 'networkLoad')
            downloadTimeSlot = get_json_value(content, 'downloadTimeSlot')
            serverInitiated = get_json_value(content, 'serverInitiated')
            numberOfRetries = get_json_value(content, 'numberOfRetries')
            updateContent = get_json_value(content, 'updateContent')
            stageLevel = get_json_value(content, 'stageLevel')
            region_platform = self.region
            brand = str()
            model = str()
            ins_timestamp = str()
            upd_timestamp = str()

            self.msgValues_list = [eventDate,
                                   username,
                                   eventType,
                                   rid,
                                   runnerId,
                                   name,
                                   creationTime,
                                   startTime,
                                   endTime,
                                   completedTime,
                                   status,
                                   statusDisplay,
                                   priority,
                                   ctype,
                                   isOngoing,
                                   action,
                                   actionDisplay,
                                   domain,
                                   deviceDetails,
                                   domainDetails,
                                   modelDetails,
                                   networkLoad,
                                   downloadTimeSlot,
                                   serverInitiated,
                                   numberOfRetries,
                                   updateContent,
                                   stageLevel,
                                   region_platform,
                                   brand,
                                   model,
                                   ins_timestamp,
                                   upd_timestamp]
            self.msg_count += 1
        except Exception as e:
            logger.error("Received an empty message " + str(e))

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

    campaign_obj = Campaign()
    campaign_obj.region = region
    campaign_obj.WRITEFILE=write_path
    campaign_obj.connect_to_activemq()
    campaign_obj.prepare_csv_file()
    logger.info("Waiting for messages ...")
    while True:
        try:
            for current_reader in campaign_obj.readers:
                if campaign_obj.msg_count == chunk or time.time() >= expiry_time:
                    logger.info("Starting new session, resetting stats")
                    campaign_obj.log_stats()
                    campaign_obj.close_opened_file()
                    # sending file to bucket
                    send_file_to_bucket()
                    # reset
                    campaign_obj.prepare_csv_file()
                    expiry_time = time.time() + duration_secs
                    campaign_obj.msg_count = 0
                    campaign_obj.error_count = 0
                    campaign_obj.filtered_out = 0
                campaign_obj.process_mq_msg(current_reader.consumer)
                if campaign_obj.msg is None:
                    nbr_null_message += 1
                    continue
                if campaign_obj.filtered:
                    campaign_obj.acknowledge_msg()
                    continue
                if campaign_obj.msg_count % 100 == 0:
                    logger.info("Reached 100 Campaigns")
                campaign_obj.write_mq_msg_to_csv()
                campaign_obj.acknowledge_msg()
            if nbr_null_message == campaign_obj.nbr_readers:
                logger.info("sleeping for %d seconds - no activity on any consumers" % consumers_sleep_time)
                time.sleep(consumers_sleep_time)
            nbr_null_message = 0
        except (KeyboardInterrupt, Exception, java.lang.Exception):
            logger.info("Closing connection ...")
            campaign_obj.disconnect_all_readers()
            campaign_obj.close_opened_file()
            send_file_to_bucket()
            sys.exit()
