#!/home/hadoop/bin/python3/bin/python3

import glob,os,re,sys
import subprocess
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import logging
import logging.handlers


def merger(filename): 

    idMgfFile = filename[:-4] + "_id.mgf"
    unidFilMgfFile = filename[:-4] + "_unid_fil.mgf"
    mergedMgfFile = filename[:-4] + "_merge.mgf"
    
    if not (os.path.isfile(idMgfFile) and os.path.isfile(unidFilMgfFile)):
        print("ERROR, no id.mgf or unid_fil.mgf file for " + filename)
        sys.exit(1)

    if (os.path.isfile(mergedMgfFile) and os.path.getsize(mergedMgfFile) >0):
        idMgfSize = os.path.getsize(idMgfFile)
        unidFilMgfSize = os.path.getsize(unidFilMgfFile)
        mergedMgfSize = os.path.getsize(mergedMgfFile)
        if mergedMgfSize == idMgfSize + unidFilMgfSize:
            print("Already merged " + str(mergedMgfSize) + "=" + str(idMgfSize) + "+" + str(unidFilMgfSize))
#            sys.exit(0)   #already has merged file
        return
    commandLine = "cat \"" + idMgfFile + "\" > \"" + mergedMgfFile + "\" && " \
                  "cat \"" + unidFilMgfFile + "\" >> \"" + mergedMgfFile +"\""
    print("Executing: " + commandLine)
    # + " /mnt/nfs_hadoop/phoenixcluster/pride-test/filter_output_20170104/" + dirName
    p = subprocess.Popen(commandLine, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    retval = p.wait()
    if(retval != 0):
        print("ERROR, with merging identified and filtered unidentified mgf files for " + filename)

    idMgfSize = os.path.getsize(idMgfFile)
    unidFilMgfSize = os.path.getsize(unidFilMgfFile)
    mergedMgfSize = os.path.getsize(mergedMgfFile)

    if mergedMgfSize != idMgfSize + unidFilMgfSize:
        print("ERROR, merged size is not equal to idMgfFile plus unidFilMgfFile " + filename)

filelist = []

for filename in glob.glob(r'/mnt/nfs_hadoop/phoenixcluster/pride-test/P?D*/*.xml'):
    filelist.append(filename)

print("find " + str(len(filelist)) + "pride xml files")

#pool = ThreadPool(10)
#pool.map(merger, filelist)
#pool.close()

for file in filelist:
    merger(file)
