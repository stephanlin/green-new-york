fout=open("CitiBike.csv","a")
citi_list = ["2013-08 - Citi Bike trip data.csv", "2013-09 - Citi Bike trip data.csv",\
            "2013-10 - Citi Bike trip data.csv", "2013-11 - Citi Bike trip data.csv", "2013-12 - Citi Bike trip data.csv",\
            "2014-01 - Citi Bike trip data.csv", "2014-02 - Citi Bike trip data.csv", "2014-03 - Citi Bike trip data.csv",\
            "2014-04 - Citi Bike trip data.csv", "2014-05 - Citi Bike trip data.csv", "2014-06 - Citi Bike trip data.csv",\
            "2014-07 - Citi Bike trip data.csv", "2014-08 - Citi Bike trip data.csv", "201409-citibike-tripdata.csv",\
            "201410-citibike-tripdata.csv", "201411-citibike-tripdata.csv", "201412-citibike-tripdata.csv", "201501-citibike-tripdata.csv",\
            "201502-citibike-tripdata.csv", "201503-citibike-tripdata.csv", "201504-citibike-tripdata.csv", "201505-citibike-tripdata.csv",\
            "201506-citibike-tripdata.csv", "201507-citibike-tripdata.csv", "201508-citibike-tripdata.csv", "201509-citibike-tripdata.csv",\
            "201510-citibike-tripdata.csv", "201511-citibike-tripdata.csv", "201512-citibike-tripdata.csv", "201601-citibike-tripdata.csv",\
            "201602-citibike-tripdata.csv", "201603-citibike-tripdata.csv"]

# first file:
for line in open("2013-07 - Citi Bike trip data.csv"):
    fout.write(line)
# now the rest:
count = 1
for num in citi_list:
    f = open(num)
    f.next() # skip the header
    for line in f:
         fout.write(line)
    f.close() # not really needed
    count+=1
    print count
fout.close()
