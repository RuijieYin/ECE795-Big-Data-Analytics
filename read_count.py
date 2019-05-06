from google.cloud import storage
import pandas
import io, os
import cv2
import numpy as np
import urllib
import time
from pyspark import SparkContext,SparkConf

'''
#-----------------------------------------------------------------------------------------------------------------------------------------------
Name of the program: read_count_save.py
Author: Ruijie Yin
Date Created: 4/17/2019
Date of Last Update: 4/17/2019
Description: This program first load all 125,436 images from HDFS one by one, and detect edges on them, then count number of pixels as edges;
also, the results of the computing times and number of pixels as edges are printed.
References are listed as in the comments.
#-----------------------------------------------------------------------------------------------------------------------------------------------
'''
project_id = 'ECE 795'
bucket_name = 'dataproc-e0525250-5ff8-4148-87f8-02212dcd472a-us-east4'
storage_client = storage.Client()

colnames = ['image_name', 'image_url']
# read the csv file stored in the bucket as a pandas dataframe:
# Reference: https://stackoverflow.com/questions/49357352/read-csv-from-google-cloud-storage-to-pandas-dataframe
data = pandas.read_csv('gs://dataproc-e0525250-5ff8-4148-87f8-02212dcd472a-us-east4/urls.csv', names=colnames)

image_name = data.image_name.tolist()
image_url = data.image_url.tolist()

# Record the starting time of performing all actions:
start_time = time.time()

#Configuring the Spark:
sconf = SparkConf().setMaster("yarn").setAppName("Read_Back_Edge")
sc = SparkContext(conf = sconf)

#iterate through the list of the URLs, and download the image according to the URLs.
i = 1
for i in range(1,125346):

    # Record the starting time of detecting:
    start_time_1=time.time()

    # The following two lines transforms the absolute URLs of where the images are stored:
    gcs_url = 'https://storage.cloud.google.com/dataproc-e0525250-5ff8-4148-87f8-02212dcd472a-us-east4/project2_images/'+image_name[i]
    resp = urllib.urlopen(gcs_url)

    # cv2.read() function load the image as numpy arrays, here image is directly transformed into numpy array:
    img = np.asarray(bytearray(resp.read()), dtype = "uint8")

    # Detecting the edges using cv2.canny() function:
    edges = cv2.Canny(img, 100, 200)  # You need this command to generate edge for the pj

    #convert np array to rdd, to be used by Spark
    edge_rdd = sc.parallelize(edges)

    count = 0

    # When iterate through rdd, it has to be iterated through rdd.collect()
    # Count the number of pixels that can be regarded as edges.
    for j in edge_rdd.collect():
        if 255 in j:
            count += 1
    print(i)
    i = i + 1
    print(count)

sc.stop()

#Calculating the computing times and average computing times:
end_time = time.time()
time_elipse = end_time - start_time
average = time_elipse / 125346

#Print the calculating time:
print("************************Computing Time**************************")
print("total time cost")
print(time_elipse)
print("Average time cost for detecting edges + count number of pixels as edges")
print(average)

