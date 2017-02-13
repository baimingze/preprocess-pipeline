#!/home/hadoop/bin/python3/bin/python3

import glob,os,re,sys
import subprocess
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import logging
import logging.handlers


def converter(commandLine):
    p = subprocess.Popen(commandLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    logger.info("start to execute: " + commandLine)        
    out = p.stdout.read()
    retval = p.wait()
    if(retval != 0):
       logger.info(out)
       logger.info("command error! " + commandLine) 
     
LOG_FILE = 'log/mztabConverter.log'
handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 1024000*1024000, backupCount = 5)
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)
logger = logging.getLogger('convert2mztab')
logger.addHandler(handler)
logger.setLevel(logging.INFO)



filelist = []

for filename in glob.glob(r'/mnt/nfs_hadoop/phoenixcluster/pride-test/P?D*/*.xml'):
#for filename in glob.glob(r'/mnt/nfs_hadoop/phoenixcluster/pride-test/*.xml'):
    filelist.append(filename)

#logger.info("find " + str(len(filelist)) + "README files")

commandLineList = []
filteredfileList = []
for file in filelist:
    p = re.compile('(.*)\.xml')
    m = p.match(file)
    if m:
        fileName = m.group(1)
        inputFileName = fileName+ ".xml"
        outputFileName = fileName + ".mztab"
        if (not os.path.isfile(outputFileName) or os.path.getsize(outputFileName) == 0):
            inputFileName = '"' + inputFileName + '"'
            commandLine =  "java -jar /mnt/nfs_hadoop/phoenixcluster/tools/PGConverter/pg-converter-1.4-SNAPSHOT/pg-converter.jar -c -pridexml " + inputFileName + " -outputformat mztab"
            commandLineList.append(commandLine)
            filteredfileList.append(inputFileName)
    else:
        logger.info("This PRIDE xml file's name is wrong:" + file)

logger.info("Going to convert those files: ")
logger.info(filteredfileList)
pool = ThreadPool(2)
pool.map(converter, commandLineList)
pool.close()
