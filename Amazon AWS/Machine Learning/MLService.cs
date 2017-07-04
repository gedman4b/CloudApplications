using System;
using System.Configuration;
using MLDataLayer;

namespace MLServiceLayer
{
    public static class MLService
    {
        public static void GetDataSource()
        {
            MLDataSource mLDS = new MLDataSource();
            byte[] fileBytes = mLDS.GetS3File(ConfigurationManager.AppSettings["bucketName"], "9f0c45b8-044d-430a-900e-286c4e58977a");
        }   
    }
}
