#!/usr/bin/python
"""
first pass at reading and parsing the acs dev file to extract required
calibration values

C. Wingard 2014-05-02
A. Teranishi 2015-01-16
"""

import numpy as np
import string
import re


devStr = 'acs128'

devFileName = devStr + '.dev'

calFileName = devStr + '_calData.txt'

calFileFirstLine = devStr + ' Calibration File\n\n'

# read the ac-s device file
with open(devFileName, 'r') as dev:
    data = dev.readlines()

# parse data for the number of temperature bins and the temperature values
nBins = np.array(data[8].strip().split()[0]).astype(np.int)
tbins = np.array(data[9].strip().split())
tbins = tbins[:-3].astype(np.float)

# convert the rest of the data for the wavelengths, offsets and temperature
# correction arrays
nWlngth = np.array(data[7].strip().split()[0]).astype(np.int)
arr = np.zeros([nWlngth, (nBins*2)+5]).astype(np.float)
for iRow in range(10, len(data)-1):
    tmp = data[iRow].strip().split()[:-12]
    tmp[0] = tmp[0].translate(None, string.letters)
    tmp[1] = tmp[1].translate(None, string.letters)
    tmp[2] = 0
    arr[iRow-10, :] = np.array(tmp).astype(np.float)

cwl = arr[:, 0]
awl = arr[:, 1]
coff = arr[:, 3]
aoff = arr[:, 4]
tc_arr = arr[:, 5:5+nBins]
ta_arr = arr[:, 5+nBins:]

tempStr = string.split(data[3])

print 'tempStr', tempStr

print 'tempStr[0]', tempStr[0]


#if tempStr[2] is 'C':
tcalStr = tempStr[1]
icalStr = tempStr[4]

print 'tcalStr', tcalStr
print 'icalStr', icalStr


print 'awl', awl

print 'cwl', cwl

temp01 = tc_arr[0,0]

#print ("%12.6f" % (temp01))

f = open(calFileName, 'w')

f.write(calFileFirstLine)

for iRow in range(0,9):
    f.write(data[iRow])

f.write('\n')

f.write('CC_tcal = ')
f.write(tcalStr)

f.write('\n\n')

f.write('CC_tbins = [')
f.write(str(u"{0:12.8f}".format(tbins[0])))
for iCol in range(1,nBins):
    print iCol
    f.write(', ')
    f.write(str(u"{0:12.8f}".format(tbins[iCol])))
f.write(']\n')

f.write('\n')

f.write('CC_awlngth = [')
f.write(str(u"{0:12.8f}".format(awl[0])))
for iCol in range(1,nWlngth):
    print iCol
    f.write(', ')
    f.write(str(u"{0:12.8f}".format(awl[iCol])))
f.write(']\n')

f.write('\n')

f.write('CC_acwo = [')
f.write(str(u"{0:12.8f}".format(aoff[0])))
for iCol in range(1,nWlngth):
    print iCol
    f.write(', ')
    f.write(str(u"{0:12.8f}".format(aoff[iCol])))
f.write(']\n')

f.write('\n')

f.write('CC_cwlngth = [')
f.write(str(u"{0:12.8f}".format(cwl[0])))
for iCol in range(1,nWlngth):
    print iCol
    f.write(', ')
    f.write(str(u"{0:12.8f}".format(cwl[iCol])))
f.write(']\n')

f.write('\n')

f.write('CC_ccwo = [')
f.write(str(u"{0:12.8f}".format(coff[0])))
for iCol in range(1,nWlngth):
    print iCol
    f.write(', ')
    f.write(str(u"{0:12.8f}".format(coff[iCol])))
f.write(']\n')

f.write('\n')

f.write('CC_taarray =\n')
for iRow in range(0,nWlngth):
    print iRow
    if (iRow == 0):
        f.write('[[')
    else:
        f.write(' [')
    f.write(str(u"{0:12.8f}".format(ta_arr[iRow, 0])))
    for iCol in range(1,nBins):
        print iCol
        f.write(', ')
        f.write(str(u"{0:12.8f}".format(ta_arr[iRow, iCol])))
    if (iRow == nWlngth-1):
        f.write(']]\n')
        print 'at end'
    else:
        f.write('],\n')

f.write('\n')

f.write('CC_tcarray =\n')
for iRow in range(0,nWlngth):
    print iRow
    if (iRow == 0):
        f.write('[[')
    else:
        f.write(' [')
    f.write(str(u"{0:12.8f}".format(tc_arr[iRow, 0])))
    for iCol in range(1,nBins):
        print iCol
        f.write(', ')
        f.write(str(u"{0:12.8f}".format(tc_arr[iRow, iCol])))
    if (iRow == nWlngth-1):
        f.write(']]\n')
        print 'at end'
    else:
        f.write('],\n')


f.close()

# now pretty-print (sort of)
tbins = np.array_str(tbins, max_line_width=10000, precision=6)
tbins = re.sub('\s{1,10}', ',', tbins)

cwl = np.array_str(cwl, max_line_width=10000, precision=1)
cwl = re.sub('\s{1,10}', ',', cwl)
awl = np.array_str(awl, max_line_width=10000, precision=1)
awl = re.sub('\s{1,10}', ',', awl)
coff = np.array_str(coff, max_line_width=10000, precision=6)
coff = re.sub('\s{1,10}', ',', coff)
aoff = np.array_str(aoff, max_line_width=10000, precision=6)
aoff = re.sub('\s{1,10}', ',', aoff)

tc_arr = np.array_str(tc_arr, max_line_width=10000, precision=6)
tc_arr = re.sub('\n\s', ',', tc_arr)
tc_arr = re.sub('\s{1,10}', ',', tc_arr)

#ta_arr = np.array_str(ta_arr, max_line_width=10000, precision=6)
ta_arr = np.array_str(ta_arr, max_line_width=10000000000000000000000000000000, precision=6)
ta_arr = re.sub('\n\s', ',', ta_arr)
ta_arr = re.sub('\s{1,10}', ',', ta_arr)

