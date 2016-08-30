
# coding: utf-8

# In[14]:

import datetime
import operator
import os
import sys
import time
import pyspark
from operator import add
import numpy as np
import matplotlib.path as mplPath
from heapq import nlargest
start = time.time()


# In[15]:

def create_geojson(filename,data):
    coordinatesList = {}
    count = 0
    import json
    with open ('block-groups-polygons.geojson') as dataFile:
        blockData = json.load(dataFile)
    for i in data:
        for block in blockData['features']:
            if int(i) == block['properties']['OBJECTID']:
                coordinatesList[count] = [block['geometry'],block['properties']]
                count+=1
        
    template =             '''             { "type" : "Feature",
                "id" : %s,
                "properties" : %s,
                "geometry" : %s
                },
            '''

    # the head of the geojson file
    output =         '''     { "type" : "FeatureCollection",
        "features" : [
        '''
            
    for k,v in coordinatesList.iteritems():
        output += template % (k,json.dumps(v[1]),json.dumps(v[0]))

    # the tail of the geojson file
    output +=         '''         ]
    }
        '''

    #opens an geoJSON file to write the output to
    outFileHandle = open(filename+".geojson", "w")
    outFileHandle.write(output)
    outFileHandle.close()


# In[16]:

import re
data = [line.strip() for line in open("/home/satya/BDM_dataset/testbdm/citibikemap.txt", 'r')]
data1 = [line.strip() for line in open("/home/satya/BDM_dataset/testbdm/2009.txt", 'r')]
data2 = [line.strip() for line in open("/home/satya/BDM_dataset/testbdm/2011_05.txt", 'r')]
data1 = [re.findall(r"[\w']+", line) for line in data1]
data2 = [re.findall(r"[\w']+", line) for line in data2]

list_tupple = []
for x in data1:
    it = iter(x[1:])
    list_tupple.append(zip(it, it))

for x in data2:
    it = iter(x[1:])
    list_tupple.append(zip(it, it))

object_id = []
for tupple in list_tupple:
    for item in tupple:
        object_id.append(item[0])

for x in data:
    if x in object_id:
        object_id.remove(x)
    
print len(object_id)
create_geojson("filter_2009_2011_05", object_id)
print (time.time()-start)/60.0


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:



