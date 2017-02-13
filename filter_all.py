import glob,os,re,sys
import subprocess
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import logging
import logging.handlers


def filter(filename): 
#    logger.info()
#    filename = filename.replace(' ', '\ ')
#    filename = filename.replace('(', '\(')
#    filename = filename.replace(')', '\)')
#    logger.info(filename)
   
    outDIR = "" 
    p = re.compile("^(.*P\wD\d{6}\/)(.*\.mgf)$")
    m = p.match(filename)
    if m:
        outDIR = m.group(1)
        shortName = m.group(2)
        print("outdir" + outDIR + "from" + filename)
    else:
        logger.info("ERROR, can not find output dir for " + filename)

    pepnovoBin = "/mnt/nfs_hadoop/phoenixcluster/tools/PepNovo/PepNovo2012/PepNovo_bin "
    pepnoveOptions = " -model CID_IT_TRYP -model_dir /mnt/nfs_hadoop/phoenixcluster/tools/PepNovo/Models -pmcsqs_only -PTMs M+16:C+57:S+80:^+14 -filter_spectra 0.05 "  
    
    outFilename = filename[:-4] + "_fil.out"
    filename = '"' + filename + '"'
   
    commandLine =  pepnovoBin + pepnoveOptions + outDIR + " -file " + filename + " > \"filter_log/" + shortName + ".log\""   
    logger.info("Goting to execute " + commandLine)
    p = subprocess.Popen(commandLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    retval = p.wait()
    if retval == 0:
        logger.info("success with file" + filename)
    else:
        logger.info("failed with file" + filename )
        logger.info(p.stdout.readlines())


filelist = []
for filename in glob.glob(r'/mnt/nfs_hadoop/phoenixcluster/pride-test/P?D*/*_unid.mgf'):
    filelist.append(filename)

LOG_FILE = 'log/PepNovo_filter.log'
handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 1024000*1024000, backupCount = 5)
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)
logger = logging.getLogger('filter_all')
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.info("start a new filtering round")

pool = ThreadPool()
pool.map(filter, filelist)
pool.close()

