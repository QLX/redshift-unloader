import os
import io
import shutil
import tempfile
import uuid
import gzip
import logging
import csv
import psutil
import time
#import pandas as pd

#from cStringIO import StringIO
from io import BytesIO
from gzip import GzipFile
from threading import Thread

from queue import Queue

from redshift_unloader.credential import Credential
from redshift_unloader.redshift import Redshift
from redshift_unloader.s3 import S3
from redshift_unloader.logger import logger

KB = 1024
MB = 1024 * 1024


class RedshiftUnloader:
    __redshift: Redshift
    __s3: S3
    __credential: Credential

    def __init__(self, host: str, port: int, user: str, password: str,
                 database: str, s3_bucket: str, access_key_id: str,
                 secret_access_key: str, region: str, verbose: bool = False) -> None:
        credential = Credential(
            access_key_id=access_key_id, secret_access_key=secret_access_key)
        self.__redshift = Redshift(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            credential=credential)
        self.__s3 = S3(credential=credential, bucket=s3_bucket, region=region)
        if verbose:
            logger.disabled = False
            logger.setLevel(logging.DEBUG)
        else:
            logger.disabled = True

    def s3get(self, q, result):
        while not q.empty():
            work = q.get()                      #fetch new work from the Queue
            try:
                bytestream = BytesIO(self.__s3.download(key=work[1]))
                got_text = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')
                #result[work[0]] =list(csv.reader(got_text))
                result[work[0]] = got_text

                logging.info("Requested..." + work[1])
                q.task_done()

            except:
                logging.error('Error with data pull')
                result[work[0]] = {}
                #signal to the queue that task has been processed
        return True

    def unload(self, query: str, filename: str,
               delimiter: str = ',', add_quotes: bool = True, escape: bool = True,
               null_string: str = '', with_header: bool = True) -> None:
        session_id = self.__generate_session_id()
        logger.debug("Session id: %s", session_id)

        s3_path = self.__generate_path("/tmp/redshift-unloader", session_id, '/')
        local_path = self.__generate_path(tempfile.gettempdir(), session_id)

        logger.debug("Get columns")
        columns = self.__redshift.get_columns(query, add_quotes) if with_header else None

        logger.debug("Unload")
        self.__redshift.unload(
            query,
            self.__s3.uri(s3_path),
            gzip=True,
            parallel=True,
            delimiter=delimiter,
            null_string=null_string,
            add_quotes=add_quotes,
            escape=escape,
            allow_overwrite=True)

        logger.debug("Fetch the list of objects")
        s3_keys = self.__s3.list(s3_path.lstrip('/'))
        local_files = list(map(lambda key: os.path.join(local_path, os.path.basename(key)), s3_keys))

        logger.debug("Create temporary directory: %s", local_path)
        os.mkdir(local_path, 0o700)

        logger.debug("Download all objects")
        for s3_key, local_file in zip(s3_keys, local_files):
            self.__s3.download(key=s3_key, filename=local_file)

        logger.debug("Merge all objects")
        with open(filename, 'wb') as out:
            if columns is not None:
                out.write(gzip.compress((delimiter.join(columns) + os.linesep).encode()))

            for local_file in local_files:
                logger.debug("Merge %s into result file", local_file)

                with open(local_file, 'rb') as read:
                    shutil.copyfileobj(read, out, 2 * MB)

        logger.debug("Remove all objects in S3")
        self.__s3.delete(s3_keys)

        logger.debug("Remove temporary directory in local")
        shutil.rmtree(local_path)

    def unload_to_list(self, query: str,
                     delimiter: str = ',', add_quotes: bool = True, escape: bool = True,
                     null_string: str = '', with_header: bool = True) -> []:

        startTime = time.time()
        lines = io.StringIO()
        master_list = []
        
        print(psutil.virtual_memory())
        logger.debug("Get columns")
        columns = self.__redshift.get_columns(query, add_quotes) if with_header else None
        if columns is not None:
            master_list.append((delimiter.join(columns) + os.linesep).encode())
            #lines.write((delimiter.join(columns) + os.linesep).encode())
            print(columns)
            lines.write(",".join(columns)+os.linesep)

        session_id = self.__generate_session_id()
        logger.debug("Session id: %s", session_id)

        s3_path = self.__generate_path("/tmp/redshift-unloader", session_id, '/')

        logger.debug("Unload")
        self.__redshift.unload(
            query,
            self.__s3.uri(s3_path),
            gzip=True,
            parallel=True,
            delimiter=delimiter,
            null_string=null_string,
            add_quotes=add_quotes,
            escape=escape,
            allow_overwrite=True)

        logger.debug("Fetch the list of objects")
        s3_keys = self.__s3.list(s3_path.lstrip('/'))

         #set up the queue to hold all the urls
        q = Queue(maxsize=0)
        # Use many threads (5 max, or one for each url)
        num_theads = min(5, len(s3_keys))

        #Populating Queue with tasks
        results = [{} for x in s3_keys];
        #load up the queue with the keys to fetch and the index for each job (as a tuple):
        for i in range(len(s3_keys)):
            #need the index and the key in each queue item.
            q.put((i,s3_keys[i]))


        for i in range(num_theads):
            logging.debug('Starting thread ', i)
            worker = Thread(target=self.s3get, args=(q,results))
            worker.setDaemon(True)    #setting threads as "daemon" allows main program to 
                              #exit eventually even if these dont finish 
                              #correctly.
            worker.start()

        #print("Waiting on queue to empty")

        #now we wait until the queue has been processed
        q.join()
        print(psutil.virtual_memory())
        for i in range(len(s3_keys)):
            #master_list.append(results[i])
            lines.write(results[i])
            results[i]=''
 
        logging.info('All tasks completed.')

        #print(master_list)
        logger.debug("Remove all objects in S3")
        self.__s3.delete(s3_keys)
        print(psutil.virtual_memory())
        endTime = time.time()
        print("start ",  startTime)
        print("end   ",  endTime)
        #return master_list
        #csv.reader(lines..getvalue()A
        lines.seek(0)
        #return pd.read_csv(lines.read(), sep=",")
        return lines.read()

    @staticmethod
    def __generate_session_id() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def __generate_path(prefix: str, session_id: str, suffix: str = '') -> str:
        return ''.join([os.path.join(prefix, session_id), suffix])
