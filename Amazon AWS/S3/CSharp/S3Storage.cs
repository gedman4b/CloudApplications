using System;
using System.IO;
using System.Configuration;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Threading;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

namespace AWSS3Storage
{
    public class S3Storage
    {
        public AmazonS3Client S3Client { get; set; }

        public S3Storage()
        {
            NameValueCollection appConfig = ConfigurationManager.AppSettings;
            S3Client = new AmazonS3Client(appConfig["AWSAccessKey"], appConfig["AWSSecretAccessKey"], RegionEndpoint.USEast1);
        }

        /// <summary>
        /// Lists buckets from an S3 account
        /// </summary>
        public void ListingBuckets()
        {
            try
            {
                ListBucketsResponse response = S3Client.ListBuckets();
                foreach (S3Bucket bucket in response.Buckets)
                {
                    Console.WriteLine("You own Bucket with name: {0}", bucket.BucketName);
                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                    amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                    Console.WriteLine("If you haven't signed up for Amazon S3, please visit http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine("An Error, number {0}, occurred when listing buckets with the message '{1}", amazonS3Exception.ErrorCode, amazonS3Exception.Message);
                }
            }
        }

        /// <summary>
        /// Creates a bucket
        /// </summary>
        /// <param name="bucketName"></param>
        public void CreateABucket(string bucketName)
        {
            try
            {
                PutBucketRequest request = new PutBucketRequest();
                request.BucketName = bucketName;
                S3Client.PutBucket(request);
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null && (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") || amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                    Console.WriteLine("If you haven't signed up for Amazon S3, please visit http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine("An Error, number {0}, occurred when creating a bucket with the message '{1}", amazonS3Exception.ErrorCode, amazonS3Exception.Message);
                }
            }
        }

        /// <summary>
        /// Writes objects to a bucket
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="keyNames"></param>
        /// <returns></returns>
        public bool WritingObjects(string bucketName, List<string> keyNames)
        {
            bool result = false;
            try
            {
                foreach(var file in keyNames)
                {
                    result = WritingAnObject(bucketName, file);
                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                    amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                    Console.WriteLine("If you haven't signed up for Amazon S3, please visit http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message '{0}' when writing objects", amazonS3Exception.Message);
                }
            }
            return result;
        }

        /// <summary>
        /// Writes an object to a bucket
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="keyName"></param>
        /// <returns></returns>
        public bool WritingAnObject(string bucketName, string keyName)
        {
            bool result = false;
            try
            {
                // simple object put
                PutObjectRequest request = new PutObjectRequest()
                {
                    ContentBody = "this is a test",
                    BucketName = bucketName,
                    Key = keyName
                };

                request.Metadata.Add("title", "the title");
                PutObjectResponse response = S3Client.PutObject(request);

                S3Client.PutObject(request);
                result = true;
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                    amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                    Console.WriteLine("If you haven't signed up for Amazon S3, please visit http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message '{0}' when writing an object", amazonS3Exception.Message);
                }
            }
            return result;
        }

        /// <summary>
        /// Writes an object to a bucket Asynchronously
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="keyName"></param>
        /// <param name="async"></param>
        public bool WritingAnObject(string bucketName, string keyName, IAsyncResult asyncResult)
        {
            bool result = false;
            try
            {
                PutObjectRequest request = new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = keyName,
                    ContentBody = "This is sample content..."
                };

                PutObjectResponse response = S3Client.PutObject(request);
                Console.WriteLine("Finished PutObject operation for {0}.", request.Key);
                Console.WriteLine("Service Response:");
                Console.WriteLine("-----------------");
                Console.WriteLine("{0}", response);
                Console.Write("\n\n");

                request.Key = "Item1";
                asyncResult = S3Client.BeginPutObject(request, null, null);
                while (!asyncResult.IsCompleted)
                {
                    //
                    // Do some work here
                    //
                }
            
                response = S3Client.EndPutObject(asyncResult);
                
                Console.WriteLine("Finished Async PutObject operation for {0}.", request.Key);
                Console.WriteLine("Service Response:");
                Console.WriteLine("-----------------");
                Console.WriteLine(response);
                Console.Write("\n\n");

                request.Key = "Item2";
                asyncResult = S3Client.BeginPutObject(request, AsyncCallback.SimpleCallback, null);

                request.Key = "Item3";
                asyncResult = S3Client.BeginPutObject(request, AsyncCallback.CallbackWithClient, S3Client);

                request.Key = "Item4";
                asyncResult = S3Client.BeginPutObject(request, AsyncCallback.CallbackWithState,
                   new ClientState { Client = (AmazonS3Client)S3Client, Start = DateTime.Now });

                Thread.Sleep(TimeSpan.FromSeconds(5));
                result = true;
            }
            catch (AmazonS3Exception s3Exception)
            {
                if (s3Exception.ErrorCode != null &&
                     (s3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                     s3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                    Console.WriteLine("If you haven't signed up for Amazon S3, please visit http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message '{0}' when performing a CallbackWithClient", s3Exception.Message);
                }
            }
            return result;
        }

        /// <summary>
        /// Retrieves objects from a bucket
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="keyNames"></param>
        /// <returns></returns>
        public List<byte[]> GetObjects(string bucketName, List<string> keyNames)
        {
            List<byte[]> bytesList = new List<byte[]>();
            try
            {
                foreach (var file in keyNames)
                {
                    bytesList.Add(GetAnObject(bucketName, file));
                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                    amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                    Console.WriteLine("If you haven't signed up for Amazon S3, please visit http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message '{0}' when retrieving objects", amazonS3Exception.Message);
                }
            }
            return bytesList;
        }

        /// <summary>
        /// Retrieves and object from a bucket
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="keyName"></param>
        /// <returns></returns>
        public byte[] GetAnObject(string bucketName, string keyName)
            {
                byte[] fileBytes = null;
                try
                {
                    GetObjectRequest request = new GetObjectRequest()
                    {
                        BucketName = bucketName,
                        Key = keyName
                    };

                    using (GetObjectResponse response = S3Client.GetObject(request))
                    {
                        using (Stream responseStream = response.ResponseStream)
                        {
                            string title = response.Metadata["x-amz-meta-title"];
                            Console.WriteLine("The object's title is: {0}", title);
                            fileBytes = StreamToByteArray(responseStream);
                        }
                    }
                }
                catch (AmazonS3Exception amazonS3Exception)
                {
                    if (amazonS3Exception.ErrorCode != null &&
                        (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                        amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                    {
                        Console.WriteLine("Please check the provided AWS Credentials.");
                        Console.WriteLine("If you haven't signed up for Amazon S3, please visit http://aws.amazon.com/s3");
                    }
                    else
                    {
                        Console.WriteLine("An error occurred with the message '{0}' when reading an object", amazonS3Exception.Message);
                    }
                }
                return fileBytes;
            }

        /// <summary>
        /// Deletes an object from a bucket
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="keyName"></param>
        public void DeletingAnObject(string bucketName, string keyName)
        {
            try
            {
                DeleteObjectRequest request = new DeleteObjectRequest()
                {
                    BucketName = bucketName,
                    Key = keyName
                };

                S3Client.DeleteObject(request);
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") ||
                    amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                    Console.WriteLine("If you haven't signed up for Amazon S3, please visit http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message '{0}' when deleting an object", amazonS3Exception.Message);
                }
            }
        }

        /// <summary>
        /// Lists objects from a bucket
        /// </summary>
        /// <param name="bucketName"></param>
        public void ListingObjects(string bucketName)
        {
            try
            {
                ListObjectsRequest request = new ListObjectsRequest();
                request.BucketName = bucketName;
                ListObjectsResponse response = S3Client.ListObjects(request);
                foreach (S3Object entry in response.S3Objects)
                {
                    Console.WriteLine("key = {0} size = {1}", entry.Key, entry.Size);
                }
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null && (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId") || amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Please check the provided AWS Credentials.");
                    Console.WriteLine("If you haven't signed up for Amazon S3, please visit http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine("An error occurred with the message '{0}' when listing objects", amazonS3Exception.Message);
                }
            }
        }

        /// <summary>
        /// To make up for Amazon AWS Read all bytes shortcoming
        /// </summary>
        /// <param name="inputStream"></param>
        /// <returns></returns>
        private byte[] StreamToByteArray(Stream inputStream)
        {
            byte[] bytes = new byte[inputStream.Length];  
            
            using (MemoryStream memoryStream = new MemoryStream())
            {
                int count;
                while ((count = inputStream.Read(bytes, 0, bytes.Length)) > 0)
                {
                    memoryStream.Write(bytes, 0, count);
                }
                return memoryStream.ToArray();
            }
        }
    }
}
