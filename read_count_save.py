#!/usr/bin/env python
from google.cloud import storage
import pandas
import io, os
import cv2
import numpy as np
import urllib
from tempfile import TemporaryFile
import time
from pyspark import SparkContext,SparkConf

'''
#-----------------------------------------------------------------------------------------------------------------------------------------------
Name of the program: read_count_save.py
Author: Ruijie Yin
Date Created: 4/2/2019
Date of Last Update: 4/17/2019
Description: This program first read in all 125,436 images from HDFS one by one, and detect edges on them, then count number of pixels as edges,
finally save the edge images back to HDFS.
References are listed as in the comments.
#-----------------------------------------------------------------------------------------------------------------------------------------------
'''

# Initialize the starting time/ Record the starting time.
total_time_1 = time.time()

# Reference on how to save the files to HDFS:
# store files in google cloud storage: https://stackoverflow.com/questions/54235721/transfer-file-from-url-to-cloud-storage
project_id = 'ECE 795'
bucket_name = 'dataproc-e0525250-5ff8-4148-87f8-02212dcd472a-us-east4'
storage_client = storage.Client()

colnames = ['image_name', 'image_url']

# read the csv file stored in the bucket as a pandas dataframe, so that we can iterate through it and retrieve the URLs of each image from it:
# https://stackoverflow.com/questions/49357352/read-csv-from-google-cloud-storage-to-pandas-dataframe
# The url in the first argument is my bucket id and the relative location of the file where I store the urls.
data = pandas.read_csv('gs://dataproc-e0525250-5ff8-4148-87f8-02212dcd472a-us-east4/urls.csv', names = colnames)

image_name = data.image_name.tolist()
image_url = data.image_url.tolist()


# Initializing the starting time of only detecting the edges, detecting the edges + count the pixels as edges + save the results to HDFS.

total_time_dect = 0
total_time_dect_w_saving = 0
total_canny_save_count = 0


#Configuring the spark:
sconf=SparkConf().setMaster("yarn").setAppName("Edge_Detect")
sc = SparkContext(conf = sconf)


i = 1
for i in range(1,125346):
    # Record the starting time
    start_time_1 = time.time()

    # The following two lines transforms the absolute URLs of where the images are stored:
    gcs_url = 'https://storage.cloud.google.com/dataproc-e0525250-5ff8-4148-87f8-02212dcd472a-us-east4/project2_images/'+image_name[i]
    resp = urllib.urlopen(gcs_url)

    #cv2.read() function read in the image as numpy arrays, here image is directly transformed into numpy array:
    img = np.asarray(bytearray(resp.read()), dtype="uint8")

    #Detecting the edges using cv2.canny() function:
    edges = cv2.Canny(img, 100, 200)  # You need this command to generate edge for the pj
    end_time_dect = time.time()
    total_time_dect += end_time_dect - start_time_1

    #convert the np array to rdd file, rdd is used in Spark.
    edge_rdd = sc.parallelize(edges)

    #count the number of pixels that can be regarded as edges.
    count = 0

    for j in edge_rdd.collect():
        if 255 in j:
            count += 1

    end_time=time.time()
    print(count)

    #save the rdd file to bucket as text files
    edge_rdd.saveAsTextFile(
        'gs://dataproc-e0525250-5ff8-4148-87f8-02212dcd472a-us-east4/edge_results_2/' + str(i) + '.txt')
    end_time_dect_2 = time.time()
    total_time_dect_w_saving += end_time_dect_2 - start_time_1
    total_canny_save_count += end_time_dect_2-start_time_1
    print(i)
    i = i + 1

# The following code will save the edge image as images (.jpg file), the computing time of saving them as images are also compared.
# Uncomment the following code when saving as images.
'''
    bucket = storage_client.get_bucket(bucket_name)
    destination_blob_name = "edge" + image_name[i]
    with TemporaryFile() as gcs_image:
        edges.tofile(gcs_image)
        gcs_image.seek(0)
        blob = bucket.blob("edge_results_3/" + destination_blob_name)
        blob.upload_from_file(gcs_image)
'''

sc.stop()

# record the end time of performing all these actions
total_end = time.time()
total_time=total_end-total_time_1

# Calculating the averages:
mean_time_wo_saving_images=total_time_dect/125346
mean_time_dect_w_saving_images=total_time_dect_w_saving/125346
mean_time_canny_save_count=total_canny_save_count/125346


# print the results of computing times
print('********************Print Computing Times**********************')


#computing time w.o. saving the image:
print("total computing time of only detecting the edges but not saving the images:")
print(total_time_dect)

#average computing time for each image w.o. saving the image:
print("average computing time of only detecting the edges without saving the images for each image:")
print(mean_time_wo_saving_images)


#computing time w. saving the image:
print("total computing time of detecting the edges and saving the images, but without count the number of pixels as edges:")
print(total_time_dect_w_saving)

#average computing time for each image with saving the image:
print("average computing time detecting the edges and saving the images, but without count the number of pixels as edges:")
print(mean_time_dect_w_saving_images)

#total computing time
print("Total Computing time:")
print(total_time)

#total canny+save+count time
print("total computing time of detecting the edges, count the number of pixels as edges, and saving the images to HDFS:")
print(total_canny_save_count)

#average canny+save+count time
print("average computing time of detecting the edges, count the number of pixels as edges, and saving the images to HDFS:")
print(mean_time_canny_save_count)