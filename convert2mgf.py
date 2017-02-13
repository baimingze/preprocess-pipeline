#!/home/hadoop/bin/python3/bin/python3

import glob,os,re,sys
import subprocess
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import logging
import logging.handlers


def converter(commandLine):
    p = subprocess.Popen(commandLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print("start to execute: " + commandLine)        
    retval = p.wait()
    out = p.stdout.read()
    if(retval != 0):
       print(out)
       print("command error! " + commandLine) 
     


filelist = []

for filename in glob.glob(r'/mnt/nfs_hadoop/phoenixcluster/pride-test/P?D*/*.xml'):
#for filename in glob.glob(r'/mnt/nfs_hadoop/phoenixcluster/pride-test/*.xml'):
    filelist.append(filename)

#print("find " + str(len(filelist)) + "README files")

commandLineList = []
filteredfileList = []
for file in filelist:
    p = re.compile('(.*)\.xml')
    m = p.match(file)
    if m:
        fileName = m.group(1)
        inputFileName = fileName+ ".xml"
        outputFileName = fileName + ".mgf"
        print(outputFileName)
        if (not os.path.isfile(outputFileName) or os.path.getsize(outputFileName) == 0):
            print(outputFileName + "is not exist")
            inputFileName = '"' + inputFileName + '"'
            outputFileName = '"' + outputFileName + '"'
            commandLine = "java -jar /mnt/nfs_hadoop/phoenixcluster/tools/pridexml2mgf/pridexml2mgf-0.1-SNAPSHOT.jar " + inputFileName+ " " + outputFileName          
            commandLineList.append(commandLine)
            filteredfileList.append(inputFileName)
    else:
        print("This PRIDE xml file's name is wrong:" + file)

print ("Going to convert those files: ")
print (filteredfileList)
pool = ThreadPool(10)
pool.map(converter, commandLineList)
pool.close()
