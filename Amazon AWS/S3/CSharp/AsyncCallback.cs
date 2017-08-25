using System;

using Amazon.S3;
using Amazon.S3.Model;

namespace AWSS3Storage
{
    public static class AsyncCallback
    {
        public static void SimpleCallback(IAsyncResult asyncResult)
        {
            Console.WriteLine("Finished PutObject operation with simple callback");
            Console.Write("\n\n");
        }

        public static void CallbackWithClient(IAsyncResult asyncResult)
        {
            try
            {
                AmazonS3Client s3Client = (AmazonS3Client)asyncResult.AsyncState;
                PutObjectResponse response = s3Client.EndPutObject(asyncResult);
                Console.WriteLine("Finished PutObject operation with client callback");
                Console.WriteLine("Service Response:");
                Console.WriteLine("-----------------");
                Console.WriteLine(response);
                Console.Write("\n\n");
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
        }

        public static void CallbackWithState(IAsyncResult asyncResult)
        {
            try
            {
                ClientState state = asyncResult.AsyncState as ClientState;
                AmazonS3Client s3Client = state.Client;
                PutObjectResponse response = state.Client.EndPutObject(asyncResult);
                Console.WriteLine(
                   "Finished PutObject operation with state callback that started at {0}",
                   (DateTime.Now - state.Start).ToString() + state.Start);
                Console.WriteLine("Service Response:");
                Console.WriteLine("-----------------");
                Console.WriteLine(response);
                Console.Write("\n\n");
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
                    Console.WriteLine("An error occurred with the message '{0}' when performing a CallbackwithState", s3Exception.Message);
                }
            }
        }
    }
}
