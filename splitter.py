import glob,os,re,sys
import subprocess
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import logging
import logging.handlers


#----------subroutions---------------

def getSpecIndex(psmLine, specIdx):
#    psmLine = "PSM	AAVEEGIVLGGGCALLR	2	CH60_HUMAN	null	SwissProt_2011.08.fasta	null	[MS, MS:1001207, Mascot, ]|[MS, MS:1001561, Scaffold, ]	0.99120164	28.573324	21.86	13-UNIMOD:4	null	3	562.3080000193991	562.3066211005047	ms_run[1]:index=3151	R	C	430	446	Spec_13566_AAVEEGIVLGGGC+57ALLR	0"
    psmItems = re.split(r'\t', psmLine)
    specIndexString = psmItems[specIdx]
    specIndexPattern = re.compile("ms_run\[1\]\:(index|spectrum)=(\d+)")
    m = specIndexPattern.match(specIndexString)
    if m:
        return m.group(2)
    else:
        logger.error("ERROR, specIndexString not match, we assume only ms_[1], but " + specIndexString)
        sys.exit(1)


def getMod(psmLine, modIdx):
#    psmLine = "PSM	AAVEEGIVLGGGCALLR	2	CH60_HUMAN	null	SwissProt_2011.08.fasta	null	[MS, MS:1001207, Mascot, ]|[MS, MS:1001561, Scaffold, ]	0.99120164	28.573324	21.86	13-UNIMOD:4	null	3	562.3080000193991	562.3066211005047	ms_run[1]:index=3151	R	C	430	446	Spec_13566_AAVEEGIVLGGGC+57ALLR	0"
    psmItems = re.split(r'\t', psmLine)
    modString = psmItems[modIdx]
    if modString == 'null':
        return None 
    modPattern = re.compile("(^\d+\-UNIMOD\:\d+)|(^\d+\-\[PSI)|(^\d+\-MOD\:\d+)")
    m = modPattern.match(modString)
    if m:
        return modString
    else:
        logger.error("ERROR, modstring not match:" + modString +"in line\n" + psmLine)
        sys.exit(1)

def getSeq(psmLine, seqIdx):
#    psmLine = "PSM	AAVEEGIVLGGGCALLR	2	CH60_HUMAN	null	SwissProt_2011.08.fasta	null	[MS, MS:1001207, Mascot, ]|[MS, MS:1001561, Scaffold, ]	0.99120164	28.573324	21.86	13-UNIMOD:4	null	3	562.3080000193991	562.3066211005047	ms_run[1]:index=3151	R	C	430	446	Spec_13566_AAVEEGIVLGGGC+57ALLR	0"
    psmItems = re.split(r'\t', psmLine)
    seqString = psmItems[seqIdx]
    seqPattern = re.compile("^[A-Z\]]+$")
    m = seqPattern.match(seqString)
    if m:
        return seqString 
    else:
        logger.error("ERROR, seqstring not match:" + seqString +" in line\n" + psmLine)
        sys.exit(1)


def getSpeciesString(mtdLine):
    species = []
    speciesP = re.compile("MTD\tsample\[\d+\]\-species\[\d+\]\t\[(.*)\]")
#    mtdline = "MTD	sample[1]-species[1]	[NEWT, 9606, Homo sapiens (Human), ]"
    m = speciesP.match(mtdLine)
    if m:
        temp_species = re.split(r', ', m.group(1),)
        p = re.compile("^\d+$")
        for temp_s in temp_species:
            m = p.match(temp_s)
            if m:
                species.append(temp_s)
    else:
        print("not match")
    
    return ",".join(species)


def writeToFile(list, file):
    for line in list:
        file.write(line + '\n')

#----------subroutions---------------


if len(sys.argv) != 2:
    logger.error("ERROR, please input the mgf file path as paramter like:")
    logger.error("python3 splitter.py /path/to/file.mgf")
    sys.exit(1)

mgfFile = sys.argv[1]
mztabFile = mgfFile[:-3] + "mztab" 
idMgfFile = mgfFile[:-4] + "_id.mgf"
unidMgfFile = mgfFile[:-4] + "_unid.mgf"
sourceXmlFile = ""
projectAcc = ""

LOG_FILE = 'log/splitter.log'
handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 1024000*1024000, backupCount = 5)
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)
logger = logging.getLogger('splitter')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

p = re.compile("^.*(P.D\d{6})\/(.*?)\.mgf$")
m = p.match(mgfFile)
if m:
    projectAcc = m.group(1)
    sourceXmlFile = m.group(2)
else:
    logger.error("ERROR, can not recgonize the project accession and source pride xml file name from the mgf file")
    sys.exit(1)

if not os.path.isfile(mgfFile) or os.path.getsize(mgfFile) == 0:
    logger.error("ERROR, the mgf is empty or not exist ")
    sys.exit(1)

if not os.path.isfile(mztabFile) or os.path.getsize(mztabFile) == 0:
    logger.error("ERROR, the mztab is empty or not exist ")
    sys.exit(1)

species = ""
psmMods = {}
psmSeqs = {}
pshArray = [] 
with open(mztabFile,'r') as mztabF:
    for line in mztabF:
        if line.startswith('MTD'):
            if line.startswith("MTD\tms_run[2]"):
                logger.error("ERROR, we assume only one ms_run in this kind of mztab file")
                sys.exit(1)
            if line.startswith("MTD\tsample[2]"):
                logger.error("ERROR, we assume only one ms_run in this kind of mztab file")
                sys.exit(1)
            if line.startswith("MTD\tsample[1]-species["):  #assume only one sample here
                species = species + getSpeciesString(line)
        
        if line.startswith('PSH'):
            pshArray = re.split('\t',line)
            seqIdx = pshArray.index("sequence")
            modIdx = pshArray.index("modifications")
            specIdx = pshArray.index("spectra_ref")

        if line.startswith('PSM'):
            index = getSpecIndex(line, specIdx)
            seq = getSeq(line, seqIdx)
            mod = getMod(line, modIdx)
            if index in psmSeqs:  #multiple psm for same spectrum
                psmSeqs[index] = psmSeqs[index] + "|" + seq 
                psmMods[index] = psmMods.get(index, "") + "|" + (mod if mod != None else "")  
            else:
                psmSeqs[index] = seq
                if mod != None:
                    psmMods[index] = mod
mztabF.close()    
logger.debug("End of mztab file reading")

spectrumHeader = []
spectrumPeakList = []
seqPrefix = "SEQ="
modPrefix = "USER03="
speciesHeadline = "TAXONOMY=" + species
with open(mgfFile,'r') as mgfF, open(idMgfFile + ".part", 'w') as idMgfF, open(unidMgfFile + ".part", 'w') as unidMgfF:

    logger.debug("start to deal mgf File")
    spectrumIndex = '-1'
    for line in mgfF:
        if re.match(r'^[a-zA-Z]+', line) and "BEGIN IONS" not in line and len(spectrumHeader) == 0 :        
            continue
            #filMgfFile.write(line) # the first line of the mgf file, MASS=Monoisotopic, etc
        elif "BEGIN IONS" in line:
            spectrumIndex = '-1'
            spectrumHeader = []
            spectrumPeakList = []
            spectrumHeader.append(line.strip())

        elif "END IONS" in line:
            spectrumPeakList.append(line.strip())
            if spectrumIndex == '-1':
                logger.error("ERROR, something wrong with the spectrum index retriving " + "////".join(spectrumHeader))
                sys.exit(1)
            if spectrumIndex in psmSeqs.keys():
                spectrumHeader.append(speciesHeadline)
                seq = psmSeqs.get(spectrumIndex, None)
                mod = psmMods.get(spectrumIndex, None)
                spectrumHeader.append(seqPrefix + seq)
                if mod != None:
                    p = re.compile("^\|+$")
                    if not p.match(mod):
                        spectrumHeader.append(modPrefix + mod)
#                    else:
#                        print("This mod " + mod + " is multiple |, delete it")
                    del psmMods[spectrumIndex]
                del psmSeqs[spectrumIndex]
                writeToFile(spectrumHeader, idMgfF)
                writeToFile(spectrumPeakList, idMgfF)
                logger.debug("write to identified mgf " + idMgfF.name)
            else: 
                writeToFile(spectrumHeader, unidMgfF)
                writeToFile(spectrumPeakList, unidMgfF)
                logger.debug("writed to unidentified mgf " + unidMgfF.name)
            logger.debug("end of a spectrum")
        elif "CHARGE" in line:
            p = re.compile('^(CHARGE=[\d\.\-\+]+)$')
            m = p.match(line)
            if m:
                spectrumHeader.append(m.group(1))
            else:
                logger.error("The format of CHARGE line is wrong :" + line)
                sys.exit(1)
        elif "TITLE" in line:
            p = re.compile('^TITLE=(\d+)$')
            m = p.match(line)
            if m:
                spectrumIndex = m.group(1)
                spectrumHeader.append("TITLE=id=" + projectAcc + ";" + sourceXmlFile + ".xml;spectrum=" + spectrumIndex)
            else:
                logger.error("ERROR, The format of TITLE line is wrong: " + line)
                sys.exit(1)

        elif re.match(r'^[a-zA-Z]+', line) and len(spectrumHeader) != 0:
            spectrumHeader.append(line.strip())

        elif re.match('^\d+', line):
            spectrumPeakList.append(line.strip())

if len(psmSeqs.keys()) > 0 or len(psmMods.keys()) > 0:
    logger.error("ERROR, some PSM in mztab file are not match in mgf file:" + "////".join(psmSeqs.keys()) )
    sys.exit(1)
#    sys.exit(2)

mgfF.close()
idMgfF.close()
unidMgfF.close()
logger.debug("write succesful and close")
os.rename(idMgfFile+".part", idMgfFile)
os.rename(unidMgfFile+".part", unidMgfFile)
