#!/usr/bin/python
"""

read_acs_dev.py

Parse ACS (OPTAA) calibration sheets to generate usable calibrations for ingestion.

Usage:
    read_acs_dev.py <dev_file>
"""
import numpy as np
import string
import docopt


def write_2d_array(output_str, data, max_range, max_range2, f):

    f.write(output_str)
    f.write(' =\n')
    for iRow in range(0,max_range):
        if (iRow == 0):
            f.write('[[')
        else:
            f.write(' [')
        f.write(str(u"{0:12.8f}".format(data[iRow, 0])))
        for iCol in range(1,max_range2):
            f.write(', ')
            f.write(str(u"{0:12.8f}".format(data[iRow, iCol])))
        if (iRow == max_range-1):
            f.write(']]\n')
        else:
            f.write('],\n')

    f.write('\n')


def write_array(output_str, data, max_range, f):
    f.write(output_str)
    f.write(' = [')
    f.write(str(u"{0:12.8f}".format(data[0])))
    for iCol in range(1,max_range):
        f.write(', ')
        f.write(str(u"{0:12.8f}".format(data[iCol])))
    f.write(']\n')
    f.write('\n')


def main():

    options = docopt.docopt(__doc__)
    devFileName = options['<dev_file>']

    devStr, _ = devFileName.split('.dev')
    calFileName = devStr + '_calData.txt'
    calFileFirstLine = devStr + ' Calibration File\n\n'
    f = open(calFileName, 'w')

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

    #if tempStr[2] is 'C':
    tcalStr = tempStr[1]
    icalStr = tempStr[4]

    print 'tcalStr', tcalStr
    print 'icalStr', icalStr
    print 'awl', awl
    print 'cwl', cwl

    f.write(calFileFirstLine)

    for iRow in range(0,9):
        f.write(data[iRow])

    f.write('\n')

    f.write('CC_tcal = ')
    f.write(tcalStr)

    f.write('\n\n')

    write_array('CC_tbins', tbins, nBins, f)
    write_array('CC_awlngth', awl, nWlngth, f)
    write_array('CC_acwo', aoff, nWlngth, f)
    write_array('CC_cwlngth', cwl, nWlngth, f)
    write_array('CC_ccwo', coff, nWlngth, f)

    write_2d_array('CC_taarray', ta_arr, nWlngth, nBins, f)
    write_2d_array('CC_tcarray', tc_arr, nWlngth, nBins, f)

    f.close()


if __name__ == '__main__':
    main()





