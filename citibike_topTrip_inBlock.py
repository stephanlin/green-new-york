import datetime
import operator
import os
import sys
import time
import pyspark
from operator import add
import numpy as np
import matplotlib.path as mplPath
from math import radians, cos, sin, asin, sqrt
start = time.time()


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


def mapToZone(parts):
    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index, zones = indexZones('block-groups-polygons-simple.geojson')
    for line in parts[1:]:
#         if line.startswith('vendor_id'): continue 
        fields = line.strip('').split(',')
        if fields ==['']: continue
        if all((fields[7],fields[8],fields[11],fields[12])):# and float(fields[4])<=2:
#             passenger_count = int(fields[3])
            start_location  = geom.Point(proj(float(fields[8]), float(fields[7])))
            end_location = geom.Point(proj(float(fields[12]), float(fields[11])))
            start_zone = findBlock(start_location, index, zones)
            end_zone = findBlock(end_location, index, zones)
            if start_zone>=0 and end_zone>=0:
                yield ((end_zone,start_zone), 1)
        
def mapper2(k2v2):
    from heapq import nlargest
    k, values = k2v2
    top3 = nlargest(3, values,key=lambda a: a[1])
    return (k,top3)

stations = sc.textFile("datasets/citibike.csv")
output = sc.parallelize(mapToZone(stations.take(2000)))
output1 = output.map(lambda x: (x[0][1],x[1])).reduceByKey(lambda x,y: x+y).takeOrdered(10, lambda x: -x[1])
output2 = output.map(lambda x: (x[0][0],x[1])).reduceByKey(lambda x,y: x+y).takeOrdered(10, lambda x: -x[1])
groupByKey().map(mapper2)
print output1
print output2
print (time.time()-start)/60.0