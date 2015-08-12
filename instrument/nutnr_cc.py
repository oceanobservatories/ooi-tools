#!/usr/bin/env python
"""Instrument Control

Usage:
    nutnr_cc
"""
import csv

stream_handle = open('SNA0344A.cal.csv', 'r')
reader = csv.reader(stream_handle)

wavelength = []
no3 = []
swa = []
reference = []




for row in reader:
    #print(row)
    try:
        wavelength.append(float(row[0]))
        no3.append(float(row[1]))
        swa.append(float(row[2]))
        reference.append(float(row[3]))
    except:
        print("Data cannot be parsed")


print(wavelength)
print(no3)
print(swa)
print(reference)