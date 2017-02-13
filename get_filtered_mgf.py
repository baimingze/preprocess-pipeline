#!/home/hadoop/bin/python3/bin/python3

import glob,os,re,sys
import subprocess
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import logging
import logging.handlers


def getter(filename): 
#    logger.info()
    p = re.compile('.*?(P\wD.*\/).*?')
    m = p.match(filename)
    if m:
        dirName = m.group(1)
    else:
        logger.error("can not find the dir name of " + filename)
        return
#    filename = filename.replace(' ', '\ ')
#    filename = filename.replace('(', '\(')
#    filename = filename.replace(')', '\)')

    commandLine = "python3 filtered_getter.py \"" + filename + "\""
    # + " /mnt/nfs_hadoop/phoenixcluster/pride-test/filter_output_20170104/" + dirName
    p = subprocess.Popen(commandLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    retval = p.wait()
    if retval == 0:
        logger.info("success with file" + filename + str(p.stdout.readlines()) )
        logger.info(p.stdout.readlines())
    elif retval == 1:
        logger.info("failed, can not find mgf file" + filename + str(p.stdout.readlines()))
    elif retval == 2:
        logger.info("failed, can not find related PepNovo output map file for " + filename + str(p.stdout.readlines()) )
        logger.info(p.stdout.readlines())
    elif retval == 3:
        logger.info("already have filtered mgf file for " + filename + str(p.stdout.readlines()))
    elif retval == 4:
        logger.info("build an empty filtered mgf file for " + filename + str(p.stdout.readlines()) )
    else:
        logger.info("error for " + filename + str(p.stdout.readlines()))


filelist = []

for filename in glob.glob(r'/mnt/nfs_hadoop/phoenixcluster/pride-test/P?D*/*_unid.mgf'):
    filelist.append(filename)

print("find " + str(len(filelist)) + "mgf files")

LOG_FILE = 'log/PepNovo_get_filtered_mgf.log'
handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 1024000*1024000, backupCount = 5)
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)
logger = logging.getLogger('get_filtered_mgf')
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.info("start a new building round")

pool = ThreadPool()
pool.map(getter, filelist)
pool.close()

