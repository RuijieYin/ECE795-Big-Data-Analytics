from google.cloud import storage
import pandas
import wget
import io, os


#-----------------------------------------------------------------------------------------------------------------------------------------------
'''
Name of the program: download_image.py
Author: Ruijie Yin
Date Created: 4/2/2019
Date of Last Update: 4/7/2019
Description: This program downloads all 125,436 images from the provided URLs.
References are listed as in the comments.
#-----------------------------------------------------------------------------------------------------------------------------------------------
'''

# Define a blob and save the downloaded images into HDFS, wget library is used when downloading those images.
# Reference: store files in google cloud storage: https://stackoverflow.com/questions/54235721/transfer-file-from-url-to-cloud-storage
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    filename = wget.download(source_file_name)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob("project2_images/" + destination_blob_name)
    # save the downloaded images to a specific folder in the bucket:
    # Reference: https://stackoverflow.com/questions/47141291/upload-file-to-google-cloud-storage-bucket-sub-directory-using-python
    blob.upload_from_filename(filename, content_type='image/jpg')
    os.remove(filename)

project_id = 'ECE 795'
bucket_name = 'dataproc-e0525250-5ff8-4148-87f8-02212dcd472a-us-east4'
storage_client = storage.Client()

colnames = ['image_name', 'image_url']

# read the csv file stored in the bucket as a pandas dataframe, then download each image from the url in the dataframe accordingly:
# Reference: https://stackoverflow.com/questions/49357352/read-csv-from-google-cloud-storage-to-pandas-dataframe
data = pandas.read_csv('gs://dataproc-e0525250-5ff8-4148-87f8-02212dcd472a-us-east4/urls.csv', names=colnames)

image_name = data.image_name.tolist()
image_url = data.image_url.tolist()


#iterate through the list of the URLs in the dataframe, and download the image according to the URLs, then upload the image to the HDFS:
i = 1
for url in image_url:
    if i < len(image_url):
        source_file_name = image_url[i]
        destination_blob_name = image_name[i]
        upload_blob(bucket_name, source_file_name, destination_blob_name)
        # monitor the downloading process
        print(i)
        i = i + 1

# if fail to retrieve the url, a possible solution is listed as below:
# urllib.ContentTooShortError: retrieval incomplete: got only out of bytes
# https://blog.csdn.net/Innovation_Z/article/details/51106601
