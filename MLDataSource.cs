using System;
using System.Collections.Generic;

using AWSS3Storage;

namespace MLDataLayer
{
    public class MLDataSource
    {
        public S3Storage Storage { get; set; }

        public MLDataSource()
        {
            Storage = new S3Storage();
        }

        public byte[] GetS3File(string bucketName, string keyName)
        {
            byte[] fileBytes = null;
            try
            {
                fileBytes = Storage.GetAnObject(bucketName, keyName);
            }
            catch (Exception ex)
            {
                
            }
            return fileBytes;
        }

        public List<byte[]> GetS3Files(string bucketName, List<string> keyNames)
        {
            List<byte[]> ListBytes = new List<byte[]>();
            try
            {
                ListBytes = Storage.GetObjects(bucketName, keyNames);
            }
            catch(Exception ex)
            {

            }
            return ListBytes;
        }

        public bool PutS3File(string bucketName, string keyName)
        {
            bool result = false;
            try
            {
                result = Storage.WritingAnObject(bucketName, keyName);
            }
            catch (Exception ex)
            {

            }
            return result;
        }

        public bool PutS3Files(string bucketName, List<string> keyNames)
        {
            bool result = false;
            try
            {
                result = Storage.WritingObjects(bucketName, keyNames);
            }
            catch (Exception ex)
            {

            }
            return result;
        }

    }
}
