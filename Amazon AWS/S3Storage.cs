using System;
using System.IO;
using System.Configuration;
using System.Collections.Specialized;
using System.Threading;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

namespace AWSS3Access
{
    public class S3Storage
    {
        private IAmazonS3 client;

        public S3Storage()
        {
            NameValueCollection appConfig = ConfigurationManager.AppSettings;
            client = new AmazonS3Client(appConfig["AWSAccessKey"], appConfig["AWSSecretAccessKey"], RegionEndpoint.USEast1);
        }

        /// <summary>
        /// Lists buckets from an S3 account
        /// </summary>
        public void ListingBuckets()
        {
            try
            {
                ListBucketsResponse response = client.ListBuckets();
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
                client.PutBucket(request);
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
                PutObjectResponse response = client.PutObject(request);

                client.PutObject(request);
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

                PutObjectResponse response = client.PutObject(request);
                Console.WriteLine("Finished PutObject operation for {0}.", request.Key);
                Console.WriteLine("Service Response:");
                Console.WriteLine("-----------------");
                Console.WriteLine("{0}", response);
                Console.Write("\n\n");

                request.Key = "Item1";
                asyncResult = client.BeginPutObject(request, null, null);
                while (!asyncResult.IsCompleted)
                {
                    //
                    // Do some work here
                    //
                }
            
                response = client.EndPutObject(asyncResult);
                
                Console.WriteLine("Finished Async PutObject operation for {0}.", request.Key);
                Console.WriteLine("Service Response:");
                Console.WriteLine("-----------------");
                Console.WriteLine(response);
                Console.Write("\n\n");

                request.Key = "Item2";
                asyncResult = client.BeginPutObject(request, AsyncCallback.SimpleCallback, null);

                request.Key = "Item3";
                asyncResult = client.BeginPutObject(request, AsyncCallback.CallbackWithClient, client);

                request.Key = "Item4";
                asyncResult = client.BeginPutObject(request, AsyncCallback.CallbackWithState,
                   new ClientState { Client = (AmazonS3Client)client, Start = DateTime.Now });

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

                    using (GetObjectResponse response = client.GetObject(request))
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

                client.DeleteObject(request);
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
                ListObjectsResponse response = client.ListObjects(request);
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
