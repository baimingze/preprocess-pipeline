#!/home/hadoop/bin/python3/bin/python3

import glob,os,re,sys
import subprocess
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import logging
import logging.handlers


def getter(commandLine):
    p = subprocess.Popen(commandLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print("start to execute: " + commandLine)        
    retval = p.wait()
    if(retval != 0):
       print("command error! " + commandLine) 
    


filelist = []

#for filename in glob.glob(r'/mnt/nfs_hadoop/phoenixcluster/pride-test/P?D*/*.xml'):
for filename in glob.glob(r'/mnt/nfs_hadoop/phoenixcluster/pride-test/*.xml'):
    filelist.append(filename)

print("find " + str(len(filelist)) + "PRIDE XML files")

commandLineList = []
for file in filelist:
    print(file)
    p = re.compile('(.*)\.xml')
    m = p.match(file)
    if m:
        fileName = m.group(1)
        fileName = fileName.replace(' ', '\ ')
        inputFileName = fileName+ ".xml"
        outputFileName = fileName + "ident.list"
        commandLine = "python3  ident_getter.py" + inputFileName+ " " + outputFileName          
        commandLineList.append(commandLine)
    else:
        print("This PRIDE xml file's name is wrong:" + file)

pool = ThreadPool()
pool.map(converter, commandLineList)
pool.close()
