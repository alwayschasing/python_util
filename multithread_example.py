import logging
import scipy
import time
import json
from sentence_transformers import SentenceTransformer
import threading
import zmq
import zmq.decorators as zmqd
# from multiprocessing import Process

class QTCosine(threading.Thread):
    def __init__(self, model_path, stop_sign:threading.Event, recv_port=1991, send_port=1992, classid=0, logger=logging.getLogger("QTCosine")):
        super(QTCosine, self).__init__()
        self.model_path = model_path
        self.model = SentenceTransformer(model_path)
        self.logger = logger
        self.id = classid
        self.recv_port = recv_port
        self.send_port = send_port
        self.stop_sign = stop_sign

    def run(self):
        self._run()

    @zmqd.context()
    @zmqd.socket(zmq.PULL)
    @zmqd.socket(zmq.PUSH)
    def _run(self,_, receiver, sender):
        receiver.connect('tcp://127.0.0.1:%d'%(self.recv_port))
        sender.connect('tcp://127.0.0.1:%d'%(self.send_port))
        self.logger.info("QTCosine[%d] connect recv_port:%d, send_por:%d" % (self.id, self.recv_port, self.send_port))

        while self.stop_sign.is_set() == False:
            events = receiver.poll(1000)
            if events:
                data = receiver.recv().decode("utf-8")
                json_part = json.loads(data.split('\t')[0])
                query = json_part["query"]
                title = json_part["title"] 
                texts = [query, title]
                embeddings = self.model.encode(texts, convert_to_numpy=True)
                cosine = 1 - scipy.spatial.distance.cosine(embeddings[0], embeddings[1]) 

                res_data = "%s\t%f" % (data, cosine)
                res_data = res_data.encode("utf-8")
                sender.send(res_data)
        
        self.logger.info("QTCosine[%d] stop run" % (self.id))

class DataReader(threading.Thread):
    def __init__(self, input_file, stop_sign:threading.Event, send_port=1991, logger=logging.getLogger(), encoding="utf-8"):
        super(DataReader, self).__init__()
        self.input_file = input_file
        self.stop_sign = stop_sign
        self.encoding = encoding
        self.send_port = send_port
        self.logger = logger
    
    def run(self):
        self._run()

    @zmqd.context()
    @zmqd.socket(zmq.PUSH)
    def _run(self, _, sender):
        sender.bind('tcp://127.0.0.1:%d'%(self.send_port))
        self.logger.info("DataReader bind send_port:%d" % (self.send_port))
        rfp = open(input_file, "r", encoding=self.encoding)
        while self.stop_sign.is_set() == False:
            for line in rfp:
                data = line.rstrip('\n').encode("utf-8")
                sender.send(data)
        rfp.close()
        self.logger.info("DataReader stop")


class DataReceiver(threading.Thread):
    def __init__(self, output_file, stop_sign:threading.Event, recv_port=1992, logger=logging.getLogger(), encoding="utf-8"):
        super(DataReceiver, self).__init__()
        self.output_file = output_file 
        self.stop_sign = stop_sign
        self.encoding = encoding
        self.recv_port = recv_port
        self.logger = logger
    
    def run(self):
        self._run()

    @zmqd.context()
    @zmqd.socket(zmq.PULL)
    def _run(self, _, receiver):
        receiver.bind('tcp://127.0.0.1:%d'%(self.recv_port))
        self.logger.info("DataReceiver bind recv_port:%d" % (self.recv_port))
        last_time = time.time()
        wfp = open(self.output_file, "w", encoding=self.encoding)
        count = 0
        st = time.time()
        while self.stop_sign.is_set() == False:
            events = receiver.poll(timeout=1000)
            if events:
                data = receiver.recv()
                data = data.decode("utf-8")
                wfp.write("%s\n" % (data))
                count += 1
                last_time = time.time()
            else:
                now = time.time()
                if int(now - last_time) > 20:
                    self.stop_sign.set()
                    self.logger.info("set stop_sign")
            ed = time.time()
            if int(ed - st) >= 60:
                st = time.time()
                speed = float(count) / 60
                self.logger.info("[check speed] %d" % (speed))
                count = 0
        wfp.close()
        self.logger.info("DataReceiver stop")


def predict_query_title_score(input_file, output_file, model_path, thread_num=3):
    logging.basicConfig(level=logging.INFO, 
                        format="[%(levelname).1s %(asctime)s] %(message)s",
                        datefmt="%Y-%m-%d_%H:%M:%S")
    logger = logging.getLogger()

    stop_sign = threading.Event()
    data_read_port = 1991 
    data_recv_port = 1992
    data_reader = DataReader(input_file, stop_sign, data_read_port, logger=logger)
    data_receiver = DataReceiver(output_file, stop_sign, data_recv_port, logger=logger)

    worker_list = []
    for i in range(thread_num):
        worker = QTCosine(model_path, stop_sign, recv_port=data_read_port, send_port=data_recv_port, classid=i, logger=logger) 
        worker_list.append(worker)
    
    for w in worker_list:
        w.start()

    data_reader.start()
    data_receiver.start()

    for w in worker_list:
        w.join()
    
    data_reader.join()
    data_receiver.join()

if __name__ == "__main__":
    input_file = "/search/odin/liruihong/websearch/websearch_data/data_20part"
    output_file = "/search/odin/liruihong/websearch/res_data/data_20part_qtfeature"
    model_path = "/search/odin/liruihong/websearch/model_data/fuse-data-epoch5-utf8"
    thread_num = 30
    predict_query_title_score(input_file, output_file, model_path, thread_num = thread_num)