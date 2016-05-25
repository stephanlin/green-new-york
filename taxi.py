# coding: utf-8

# In[53]:

import datetime
import operator
import os
import sys
import time
import pyspark
from operator import add
import numpy as np
import matplotlib.path as mplPath


# In[54]:

def create_geojson(filename,data):
    import json
    coordinatesList = {}
    with open ('block-groups-polygons-simple.geojson') as dataFile:
        blockData = json.load(dataFile)
    count = 0
    for i in data:
        for block in blockData['features']:
            if i == block['properties']['OBJECTID']:
                coordinatesList[count] = [block['geometry'],block['properties']]
                count+=1

    template =         '''         { "type" : "Feature",
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

    # opens an geoJSON file to write the output to
    outFileHandle = open(filename+".geojson", "w")
    outFileHandle.write(output)
    outFileHandle.close()


# In[55]:

def indexZones(shapeFilename):
    import rtree
    import fiona.crs
    import geopandas as gpd
    index = rtree.Rtree()
    zones = gpd.read_file(shapeFilename).to_crs(fiona.crs.from_epsg(2263))
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findBlock(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        z = mplPath.Path(np.array(zones.geometry[idx].exterior))
        if z.contains_point(np.array(p)):
            return zones['OBJECTID'][idx]
    return -1

def findB(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if any(map(lambda x: x.contains(p), zones.geometry[idx])):
            return zones['boroname'][idx]
    return -1


# In[56]:

def mapToZone(parts):
    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index, zones = indexZones('block-groups-polygons-simple.geojson')
    index2, zones2 = indexZones('boroughs.geojson')
    for line in parts:
        if line.startswith('vendor_id'): continue 
        fields = line.strip('').split(',')
        if fields ==['']: continue
        if all((fields[5],fields[6],fields[9],fields[10])) and float(fields[4])<=1:
            pickup_location  = geom.Point(proj(float(fields[5]), float(fields[6])))
	    dropoff_location = geom.Point(proj(float(fields[9]), float(fields[10])))
            pickup_block = findBlock(pickup_location, index, zones)
            dropoff_block = findBlock(dropoff_location, index, zones)
            pickup_borough = findB(pickup_location, index2, zones2)
	    dropoff_borough = findB(dropoff_location, index2, zones2)
            if pickup_block>=0 and pickup_borough>0 and dropoff_block>0 and dropoff_borough>0:#np.array(pickup_block.exterior)
                yield (pickup_block,pickup_borough,dropoff_block,dropoff_borough)

def mapper2(k2v2):
    from heapq import nlargest
    k, values = k2v2
    top10 = nlargest(10, values, lambda x:x[1])
    return (k,top10)

# In[58]:

if __name__=='__main__':
    if len(sys.argv)<3:
        print "Usage: <input files> <output path>"
        sys.exit(-1)

    sc = pyspark.SparkContext()
    trips = sc.textFile(','.join(sys.argv[1:-1]))
    #trips = sc.textFile('/home/satya/BDM_dataset/yellow_tripdata_2011-05.csv')

    output = trips.mapPartitions(mapToZone)
    pickup = output.map(lambda x: ((x[0],x[1]),1)).reduceByKey(lambda x,y: x+y).map(lambda x:(x[0][1], (x[0][0],x[1]))).groupByKey().map(mapper2)
    dropoff = output.map(lambda x: ((x[2],x[3]),1)).reduceByKey(lambda x,y: x+y).map(lambda x:(x[0][1], (x[0][0],x[1]))).groupByKey().map(mapper2)
    #print final.collect()
    final  = pickup.union(dropoff)
    final.saveAsTextFile(sys.argv[-1])


# In[59]:

    create_geojson("pickup_map",pickup_all.map(lambda x: x[1][0]).collect())
    create_geojson("dropoff_map",pickup_all.map(lambda x: x[1][0]).collect())


# In[60]:



