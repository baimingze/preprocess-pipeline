#!/home/hadoop/bin/python3/bin/python3

import glob,os,re,sys
import subprocess
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import logging
import logging.handlers


def extracter(commandLine):
    p = subprocess.Popen(commandLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    retval = p.wait()
    


filelist = []

for filename in glob.glob(r'/mnt/nfs_hadoop/phoenixcluster/pride-data/pride/P?D*/README.txt'):
    filelist.append(filename)

for filename in glob.glob(r'/mnt/nfs_hadoop/phoenixcluster/pride-data/pride/P?D*/P?D*/README.txt'):
    filelist.append(filename)


print("find " + str(len(filelist)) + "README files")

commandLineList = []
for file in filelist:
    p = re.compile('.*(P\wD.*)\/(.*)')
    m = p.match(file)
    if m:
        sourcePath = file.replace("README.txt", "")
        prjName = m.group(1)
        with  open(file, 'r') as f:
            lines = f.readlines() 
            for line in lines:
                words =  line.split('\t')
                if(words[3].lower() == "result"):
                    if words[1][-3:] != '.gz':
                        print("ERROR, some pride xml is not gz file:" + words[1])
                        sys.exit(1)
                    destPath = prjName + "/" + words[1].replace(".gz","")
                    mkdirCmdLine = "mkdir -p " + prjName
                    os.popen(mkdirCmdLine)
                    commandLine = "gzip -cd " + sourcePath + words[1] + " >" + destPath           
                    commandLineList.append(commandLine)
#        os.popen(commandLine)
    else:
        print("can not find the dir name of " + filename)
print(commandLineList)        


pool = ThreadPool()
pool.map(extracter, commandLineList)
pool.close()
