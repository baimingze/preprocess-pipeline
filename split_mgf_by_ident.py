#!/home/hadoop/bin/python3/bin/python3

import glob,os,re,sys
import subprocess
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import logging
import logging.handlers

LOG_FILE = 'log/splitter.log'
handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 1024000*1024000, backupCount = 5)
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)
logger = logging.getLogger('splitter')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

def splitter(mgfFile):
    commandLine = "python3 splitter.py \"" + mgfFile + "\""
    p = subprocess.Popen(commandLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    logger.info("start to execute: " + commandLine)        
    retval = p.wait()
#    out = p.stdout.read()
    if(retval != 0):
#       logger.error(out)
       logger.error("command error! " + commandLine) 
     

mgfFileList = []
mztabFileList = []
for filename in glob.glob(r'/mnt/nfs_hadoop/phoenixcluster/pride-test/P?D*/*.xml'):
#for filename in glob.glob(r'/mnt/nfs_hadoop/phoenixcluster/pride-test/*.xml'):
    mgfFile = filename[:-3] + "mgf"
    idMgfFile = filename[:-4] + "_id.mgf"
    unidMgfFile = filename[:-4] + "_unid.mgf"
    mztabFile = filename[:-3] + "mztab"

    if os.path.isfile(mgfFile) and os.path.getsize(mgfFile) > 0:
        if not (os.path.isfile(idMgfFile) and \
           os.path.isfile(unidMgfFile)    and \
           os.path.getsize(idMgfFile)==0  and \
           os.path.getsize(unidMgfFile)==0 )  :
            mgfFileList.append(mgfFile)
    else:
        logger.error("ERROR, no proper mgf file for:" + filename)
        sys.exit(1)

    if os.path.isfile(mztabFile) and os.path.getsize(mztabFile) > 0:
        mztabFileList.append(mztabFile)
    else:
        logger.error("ERROR, no proper mztab file for:" + filename)
        sys.exit(1)


pool = ThreadPool(24)
pool.map(splitter, mgfFileList)
pool.close()
