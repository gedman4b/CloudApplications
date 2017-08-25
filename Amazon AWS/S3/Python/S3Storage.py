import boto3
from botocore.exceptions import ClientError

class S3Storage:

        def __init__(self):
                self.s3 = boto3.resource('s3')
                self.s3client = boto3.client('s3')

        def ListingBuckets(self):
            # Lists buckets from an S3 account
                    try:
                                    response = s3client.list_buckets()
                                    for bucket in s3.buckets.all():
                                        for key in bucket.objects.all():
                                            print("You own Bucket with name: {0}", key.key)
                    except ClientError as e:
                                    if e.response['Error']['Code'] == "404":
                                        print("The object does not exist.")
                                    else:
                                        raise

        def CreateABucket(self, bucketName):
            # Creates a bucket
                    try:
                                    s3client.create_bucket(Bucket=bucketName)
                                    s3client.create_bucket(Bucket=bucketName, CreateBucketConfiguration={
                                'LocationConstraint': 'us-east-1'})
                    except ClientError as e:
                                    if e.response['Error']['Code'] == "404":
                                        print("The object does not exist.")
                                    else:
                                        raise

        def DeleteABucket(self, bucketName):
            # Deletes a bucket
                    try:
                                    bucket = s3.Bucket(bucketName)
                                    for key in bucket.objects.all():
                                        key.delete()  # all keys in a bucket must be deleted first
                                        bucket.delete()
                    except ClientError as e:
                                    if e.response['Error']['Code'] == "404":
                                        print("The object does not exist.")
                                    else:
                                        raise

        def WritingObjects(self, bucketName, keyNames):
                # Writes objects to a bucket
                    result = False
                    try:
                                    for key in keyNames:
                                        result = self.WritingAnObject(bucketName, file)
                    except ClientError as e:
                                    if e.response['Error']['Code'] == "404":
                                        print("The object does not exist.")
                                    else:
                                        raise
                    return result


        def WritingAnObject(self, bucketName, keyName):
                # Uploads an object to a bucket
                    result = False;
                    try:
                                    bucket = s3.Bucket(bucketName)
                                    bucket.upload_file(keyName, keyName)
                                    result = True
                    except ClientError as e:
                                    if e.response['Error']['Code'] == "404":
                                        print("The object does not exist.")
                                    else:
                                        raise
                    return result

        def GetObjects(self, bucketName, keyNames):
                # Retrieves objects from a bucket
                    result = False
                    try:
                                    for key in keyNames:
                                        self.GetAnObject(bucketName, key)
                                    result = True
                    except ClientError as e:
                                    if e.response['Error']['Code'] == "404":
                                        print("The object does not exist.")
                                    else:
                                        raise
                    return result

        def GetAnObject(self, bucketName, keyName):
                # Downloads an object from a bucket to a file

                    result = False
                    try:
                                    bucket = s3.Bucket(bucketName)
                                    bucket.download_file(keyName, keyName)
                                    result = True
                    except ClientError as e:
                                    if e.response['Error']['Code'] == "404":
                                        print("The object does not exist.")
                                    else:
                                        raise
                    return result

        def DeletingAnObject(self, bucketName, keyName):
                # Deletes an object from a bucket

                    result = False
                    try:
                                    request = s3client.delete_object(BucketName = bucketName, Key = keyName)
                                    result = True
                    except ClientError as e:
                                    if e.response['Error']['Code'] == "404":
                                        print("The object does not exist.")
                                    else:
                                        raise
                    return result

        def ListingObjects(self, bucketName):
                # Lists objects from a bucket

                    result = False
                    try:
                                    for obj in s3.Bucket.objects.all():
                                        print(obj.key)
                                    result = True
                    except ClientError as e:
                                    if e.response['Error']['Code'] == "404":
                                        print("The object does not exist.")
                                    else:
                                        raise
                    return result
