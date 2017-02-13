#!/home/hadoop/bin/python3/bin/python3
import os, sys, re
import pdb 


def writeToFile(list, file):
    for line in list:
        file.write(line + '\n')




mgfFileName = sys.argv[1]
#outputPath = sys.argv[2]


mgfShortName = re.sub(r".*/", "",mgfFileName[:-4])

#mapName = outputPath + mgfShortName + "_map.txt"
#filMgfName = outputPath + mgfShortName + "_fil.mgf"

mapName = mgfFileName[:-4] + "_map.txt"
filMgfName = mgfFileName[:-4] + "_fil.mgf"

if os.path.isfile(filMgfName):
    sys.exit(0) 


charges = {}
precursorIntensities = {}
clusterSizes = {}

#print(mgfFileName)
if not os.path.isfile(mgfFileName):
    print("mgf file" + mgfFileName + "not found")
    sys.exit(1)

if not os.path.isfile(mapName):
    print("map file" + mapName + "not found")
    sys.exit(2)

#if os.path.isfile(filMgfName):
#    print("mgf file" + filMgfName + " is already exists")
#    sys.exit(3)

with open(mapName) as mapFile, open(mgfFileName) as mgfFile, open(filMgfName+".part",'w') as filMgfFile:
    for line in mapFile:
        if "num_spec_written" in line:
            continue
        else:
            numbers = line.split("\t")
            relative_num = numbers[1]
            charges[relative_num] = numbers[2]           
            precursorIntensities[relative_num] = numbers[3]
            clusterSizes[relative_num]	= numbers[4]
    print("got " + str(len(charges)) + "filtered spectra in map file" )
    if len(charges) == 0:
        print("map file" + mapName + "is empty")
        os.rename(filMgfName+".part", filMgfName)
        sys.exit(2)
        
    spectrumNum = -1
    spectrumHeader = []
    spectrumPeakList = []
#    pdb.set_trace()
    for line in mgfFile:
        if re.match(r'^[a-zA-Z]+', line) and "BEGIN IONS" not in line and len(spectrumHeader) == 0 :
            filMgfFile.write(line) # the first line of the mgf file, MASS=Monoisotopic, etc
            
        elif "BEGIN IONS" in line:
            spectrumNum += 1
            spectrumHeader = []
            spectrumPeakList = []

            spectrumHeader.append(line.strip())

        elif "END IONS" in line:
            spectrumPeakList.append(line.strip())
            if str(spectrumNum) in charges.keys():
                spectrumNumStr = str(spectrumNum)
#                spectrumHeader.append("CHARGE=" + charges.get(spectrumNumStr) + "+")
#                spectrumHeader.append("PRECURSORIN_TENSITY=" + precursorIntensities.get(spectrumNumStr)) #can not be deal by pride cluster parser
#                spectrumHeader.append("CLUSTER_SIZE=" + clusterSizes.get(spectrumNumStr))
                writeToFile(spectrumHeader, filMgfFile)
                writeToFile(spectrumPeakList, filMgfFile)

        elif "CHARGE" in line:
            p = re.compile('(^CHARGE=\d+\+).*?')
            m = p.match(line)
            if m:
                spectrumHeader.append(m.group(1))
            else:
                print("The format of CHARGE line is wrong")

        elif re.match(r'^[a-zA-Z]+', line) and len(spectrumHeader) != 0:
            spectrumHeader.append(line.strip())

        elif re.match('^\d+', line):
            spectrumPeakList.append(line.strip())

mapFile.close()
mgfFile.close()
filMgfFile.close()
os.rename(filMgfName+".part", filMgfName)
if os.path.getsize(filMgfName) == 0:
    print("Got an empty filtered mgf file " + filMgfName)
#    os.remove(filMgfName)
    sys.exit(4)
sys.exit(0)
