#!/home/hadoop/bin/python3/bin/python3

'''
check if these files are all here and not empty
PRIDE xml, mgf, mztab  
'''
import glob,os,re,sys
import subprocess
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import logging
import logging.handlers


readmefilelist = []

for filename in glob.glob(r'/mnt/nfs_hadoop/phoenixcluster/pride-data/pride/P?D*/README.txt'):
    readmefilelist.append(filename)

for filename in glob.glob(r'/mnt/nfs_hadoop/phoenixcluster/pride-data/pride/P?D*/P?D*/README.txt'):
    readmefilelist.append(filename)


print("find " + str(len(readmefilelist)) + "README files")

resultfileList= []
for file in readmefilelist:
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
                    destPath = prjName.lower() + "/" + words[1][:-3].lower()
                    resultfileList.append(destPath)
#        os.popen(commandLine)
    else:
        print("ERROR, can not find the dir name of " + filename)
        sys.exit(1)
#print(*resultfileList, sep='\n')        
print("start to check the other filetypes")
xmlfileList = []
for filename in glob.glob(r'/mnt/nfs_hadoop/phoenixcluster/pride-test/P?D*/*.xml'):
    mgfFilename =  filename[:-3] + "mgf"
    mztabFilename = filename[:-3] + "mztab"
    idMgfFilename = filename[:-4] + "_id.mgf"
    unidMgfFilename = filename[:-4] + "_unid.mgf"
    unidFilMgfFilename = filename[:-4] + "_unid_fil.mgf"
    mergedMgfFilename = filename[:-4] + "_merge.mgf"
    totalSpecInMgf = 0
    totalSpecInIdMgf = 0
    totalSpecInUnidMgf = 0
    if os.path.getsize(filename) > 0 :
        xmlfileList.append(filename.replace("/mnt/nfs_hadoop/phoenixcluster/pride-test/","").lower())
    else:
        print("ERROR, this file is empty: " + filename)

    if not (os.path.isfile(mgfFilename) and os.path.getsize(mgfFilename) > 0):
        print("ERROR, this xml file do not has mgf file >0 " + filename)    
#    else:
#        totalSpecInMgf = int (subprocess.getoutput('grep "BEGIN" "' + mgfFilename + '"|wc -l'))

    if not (os.path.isfile(mztabFilename) and os.path.getsize(mztabFilename) > 0):
        print("ERROR, this xml file do not has mztab file >0 " + filename)    
    
    if not (os.path.isfile(idMgfFilename) and os.path.getsize(idMgfFilename) > 0):
        print("ERROR, this xml file do not has identified mgf file >0 " + filename + idMgfFilename)
        if (os.path.isfile(idMgfFilename)):
            print("The size of " + idMgfFilename + "is" + os.path.getsize(idMgfFilename))
#    else:
#        totalSpecInIdMgf = int (subprocess.getoutput('grep "BEGIN" "' + idMgfFilename + '"|wc -l'))

    if not (os.path.isfile(unidMgfFilename)): #some xml files may have no unidentified mgf file
        print("ERROR, this xml file do not has unidentified mgf file  " + filename + unidMgfFilename)    
#    else:
#        totalSpecInUnidMgf = int (subprocess.getoutput('grep "BEGIN" "' + unidMgfFilename + '"|wc -l'))

#    if totalSpecInMgf != totalSpecInIdMgf + totalSpecInUnidMgf:
#        print("ERROR, this xml file 's spectra in identified and unidentified are not equal to original mgf file  " + \
#            filename + str(totalSpecInMgf) + "!=" + str(totalSpecInIdMgf) + "+" + str(totalSpecInUnidMgf))    

    if not (os.path.isfile(unidFilMgfFilename)): #some xml files may have no unidentified mgf file
        print("ERROR, this xml file do not has unidentified filtered mgf file  " + filename + unidFilMgfFilename)    

#    if not (os.path.isfile(mergedMgfFilename) and os.path.getsize(mergedMgfFilename) > 0):
#        print("ERROR, this xml file do not has merged mgf file >0 " + filename)    
    
for xmlfile in xmlfileList:
    if xmlfile not in resultfileList:
        print("ERROR, this xml file should not be here " + xmlfile) 
    
for resultfile in resultfileList:
    if resultfile not in xmlfileList:
        print("ERROR, this result file do not has xml file " + resultfile) 
