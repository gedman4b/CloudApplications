using System;
using System.Collections.Generic;
using System.Net;

using Amazon.MachineLearning;
using Amazon.MachineLearning.Model;

namespace MLServiceLayer
{
    public class MLService
    {
        public AmazonMachineLearningClient Client { get; set; }
        private string DataSourceID { get; set; }
        private Double ScoreThreshold { get; set; }

        public MLService()
        {
            Client = new AmazonMachineLearningClient(Connection.AKey, Connection.SKey, Connection.Endpoint);
        }
        
        /// <summary>
        /// Create training Datasource
        /// </summary>
        /// <param name="location"></param>
        /// <param name="dataSourceName"></param>
        /// <returns>MLDataSource</returns>
        public void CreateMLDataSource(string location, string dataSourceName)
        {
            try
            {
                CreateDataSourceFromS3Request request = new CreateDataSourceFromS3Request()
                {
                    DataSourceName = dataSourceName,
                    DataSpec = new S3DataSpec()
                    {
                        DataLocationS3 = location,
                        DataRearrangement = "{\"splitting\":{\"percentBegin\":0, \"percentEnd\":25}}"
                    }

                };
                CreateDataSourceFromS3Response modelResponse = Client.CreateDataSourceFromS3(request);
                DataSourceID = modelResponse.DataSourceId;
            }
            catch (AmazonMachineLearningException amlex)
            {
                Console.WriteLine(amlex.Message);
            }
        }

        /// <summary>
        /// Verifies that a Data Source exists for a model
        /// </summary>
        /// <param name="dataSourceID"></param>
        /// <returns>bool</returns>
        public bool VerifyMLDataSource(string dataSourceID)
        {
            bool result = false;
            try
            {
                GetDataSourceRequest request = new GetDataSourceRequest()
                {
                    DataSourceId = dataSourceID
                };
                GetDataSourceResponse response = Client.GetDataSource(request);
                response.ComputeStatistics = false;
                if (response.HttpStatusCode == HttpStatusCode.OK)
                {
                    result = true;
                }
            }
            catch(AmazonMachineLearningException amlex)
            {
                Console.WriteLine(amlex.Message);
            }
            return result;
        }

        /// <summary>
        /// Create ML Model
        /// </summary>
        /// <param name="modelName"></param>
        /// <param name="modelType"></param>
        public void CreateMLModel(string modelName, string modelType)
        {
            try
            {
                CreateMLModelRequest modelRequest = new CreateMLModelRequest()
                {
                    TrainingDataSourceId = DataSourceID,
                    MLModelName = modelName,
                    MLModelType = modelType
                };

                CreateMLModelResponse modelResponse = Client.CreateMLModel(modelRequest);
            }
            catch (AmazonMachineLearningException amlex)
            {
                Console.WriteLine(amlex.Message);
            }
        }

        /// <summary>
        /// Verifies that a Model exists
        /// </summary>
        /// <param name="mlModelID"></param>
        /// <returns>bool</returns>
        public bool VerifyMLModel(string mlModelID)
        {
            bool result = false;
            try
            {
                GetMLModelRequest request = new GetMLModelRequest()
                {
                    MLModelId = mlModelID
                };
                GetMLModelResponse response = Client.GetMLModel(request);

                if (response.HttpStatusCode == HttpStatusCode.OK)
                {
                    result = true;
                }
            }
            catch (AmazonMachineLearningException amlex)
            {
                Console.WriteLine(amlex.Message);
            }
            return result;
        }

        /// <summary>
        /// Review model training and evaluation peformance
        /// </summary>
        /// <param name="evaluationID"></param>
        public void ReviewModelPerformance(string evaluationID)
        {
            try
            {
                GetEvaluationRequest request = new GetEvaluationRequest()
                {
                    EvaluationId = evaluationID
                };
                
                GetEvaluationResponse response = Client.GetEvaluation(request);

                if (String.IsNullOrEmpty(response.Message))
                {
                    Console.WriteLine("There is no Message from the ML Evaluation");
                    Console.WriteLine();
                }
                else
                {
                    Console.WriteLine(response.Message);
                    Console.WriteLine();
                }

                Console.WriteLine("The evaluation with a Threshold at 0.5...");
                var metrics = response.PerformanceMetrics.Properties;
                foreach (var metric in metrics)
                {
                    Console.WriteLine("Metric Key: " + metric.Key + " = {0}", metric.Value);
                }          
            }
            catch (AmazonMachineLearningException amlex)
            {
                Console.WriteLine(amlex.Message);
            }
        }

        /// <summary>
        /// Use ML model to generate predictions
        /// </summary>
        /// <param name="modelID"></param>
        public void GeneratePredictions(string modelID, string predictEndpoint, Dictionary<string, string> dict)
        {
            try
            {
                PredictRequest pRequest = new PredictRequest()
                {
                    MLModelId = modelID,
                    PredictEndpoint = predictEndpoint,
                    Record = dict
                };

                PredictResponse pResponse = Client.Predict(pRequest);
                Console.WriteLine("The Prediction Result for this input is: {0}", pResponse.Prediction.PredictedLabel);
                Console.WriteLine();
                Console.WriteLine("The Prediction Scores for this input is: {0} : {1}", pResponse.Prediction.PredictedLabel, pResponse.Prediction.PredictedScores[pResponse.Prediction.PredictedLabel]);
                Console.WriteLine();
               
            }
            catch (AmazonMachineLearningException amlex)
            {
                Console.WriteLine(amlex.Message);
            }
        }
    }
}
