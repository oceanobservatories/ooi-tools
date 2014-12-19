# mdp_config.py:
# Read and parse gliders / nodes configuration file
# 08jul2013 dpingal@teledyne.com Initial

import mdd_config
import os
from xml.dom.minidom import parse

xml_config_fn = mdd_config.datafile('ooi.xml')

# All info about a node in an object (just data)
class nodeInfo(object):
    def __init__(self, n, posn, name, rate):
        self.id = int(n)
        self.lat = posn[0]
        self.lon = posn[1]
        self.depth = posn[2]
        self.name = name
        self.rate = rate

# Shortcut to get subelement from a node
def getElementData(doc, name):
    return str(doc.getElementsByTagName(name)[0].childNodes[0].data)

# Read xml config file into usable glider and node lists
def getSysConfig():
    config = parse(xml_config_fn).getElementsByTagName('modemConfig')[0]
    deployments = config.getElementsByTagName('deployment')
    result = []
    for data in deployments:
        xgliders = data.getElementsByTagName('gliderList')[0].getElementsByTagName('glider')
        gliders = [str(gld.childNodes[0].data) for gld in xgliders]
        xnodes = data.getElementsByTagName('nodeList')[0].getElementsByTagName('node')
        nodes = []
        for xnode in xnodes:
            n = getElementData(xnode, 'nodeID')
            aposn = getElementData(xnode, 'coordinates')
            name =  getElementData(xnode, 'name')
            rate =  float(getElementData(xnode, 'rate'))
            fposn = [float(val) for val in aposn.split(',')[:3]]
            nodes.append(nodeInfo(n, fposn, name, rate))
        result.append((gliders, nodes))
    return result
