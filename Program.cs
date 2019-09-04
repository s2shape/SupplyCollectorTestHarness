using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using S2.BlackSwan.SupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;

namespace SupplyCollectorTestHarness
{
    class Program
    {
        static void Main(string[] args)
        {
            string filename = "test_harness.config";

            TestInfo testInfo = GetTestInfoFromFile(filename);

            Console.WriteLine("Testing " + testInfo.SupplyCollectorName);
            Console.WriteLine();

            string supplyCollectorPath = Path.Combine(Environment.CurrentDirectory, testInfo.SupplyCollectorName + ".dll");

            Assembly supplyCollectorAssembly = Assembly.LoadFile(supplyCollectorPath);
            Type supplyCollectorType = supplyCollectorAssembly.GetType(String.Format("{0}.{0}", testInfo.SupplyCollectorName));

            ISupplyCollector supplyCollector = (ISupplyCollector)Activator.CreateInstance(supplyCollectorType);

            DataContainer dataContainer = new DataContainer() {ConnectionString = testInfo.ConnectionString};

            TestDataStoreTypes(supplyCollector);

            TestConnection(supplyCollector, dataContainer);

            TestGetSchema(supplyCollector, dataContainer, testInfo);

            TestCollectSample(supplyCollector, dataContainer, testInfo);
        }

        private static TestInfo GetTestInfoFromFile(string filename)
        {
            var testInfo = new TestInfo();

            var lines = File.ReadAllLines(filename);

            int lineNumber = 0;
            string supplyCollectorName = string.Empty;
            string connectionString = string.Empty;

            foreach (string line in lines)
            {
                string trimmedLine = line.Trim();

                if (String.IsNullOrWhiteSpace(trimmedLine) || trimmedLine.StartsWith("#"))
                {
                    continue;
                }

                if (lineNumber == 0)
                {
                    testInfo.SupplyCollectorName = line;
                }

                if (lineNumber == 1)
                {
                    testInfo.ConnectionString = line;
                }

                if (lineNumber > 1)
                {
                    string[] lineParts = line.Split("|");

                    switch (lineParts[0].Trim().ToLowerInvariant())
                    {
                        case "getschema":
                            testInfo.SchemaTableCount = Convert.ToInt32(lineParts[1]);
                            testInfo.SchemaEntityCount = Convert.ToInt32(lineParts[2]);
                            break;
                        case "collectsample":
                            testInfo.AddCollectSampleTest(lineParts);
                            break;
                        case "datacollectionmetrics":
                            testInfo.DataCollectionRowCounts[lineParts[1].Trim()] = Convert.ToInt32(lineParts[2]);
                            break;
                    }
                }

                lineNumber++;
            }


            return testInfo;
        }

        private static void TestDataStoreTypes(ISupplyCollector supplyCollector)
        {
            var dataStoreTypes = supplyCollector.DataStoreTypes();

            if (dataStoreTypes.Count < 1)
            {
                throw new ApplicationException("No data store types found with DataStoreTypes");
            }

            if (dataStoreTypes.Count == 1)
            {
                Console.WriteLine("Data store type supported:");
            }
            else
            {
                Console.WriteLine("Data store types supported:");
            }
            
            foreach (string dataStoreType in supplyCollector.DataStoreTypes())
            {
                Console.WriteLine(dataStoreType);
            }
            Console.WriteLine();
        }

        private static void TestConnection(ISupplyCollector supplyCollector, DataContainer dataContainer)
        {
            bool result = supplyCollector.TestConnection(dataContainer);

            if (!result)
            {
                throw new ApplicationException("Could not connect to data store using " + dataContainer.ConnectionString);
            }

            Console.WriteLine("Connection to data store succeeded");
            Console.WriteLine();
        }

        private static void TestGetSchema(ISupplyCollector supplyCollector, DataContainer dataContainer, TestInfo testInfo)
        {
            Console.Write("Testing GetSchema() ");

            var (tables, entities) = supplyCollector.GetSchema(dataContainer);

            if (tables.Count != testInfo.SchemaTableCount)
            {
                throw new ApplicationException(
                    $"The table count from GetSchema ({tables.Count}) did not match the expected value of {testInfo.SchemaTableCount}");
            }
            if (entities.Count != testInfo.SchemaEntityCount)
            {
                throw new ApplicationException(
                    $"The entity count from GetSchema ({entities.Count}) did not match the expected value of {testInfo.SchemaEntityCount}");
            }

            Console.WriteLine(" - success.");
            Console.WriteLine();
        }

        private static void TestCollectSample(ISupplyCollector supplyCollector, DataContainer dataContainer, TestInfo testInfo)
        {
            Console.Write("Testing CollectSample() ");

            foreach (var definition in testInfo.CollectSampleTestDefinitions)
            {
                DataCollection collection = new DataCollection(dataContainer, definition.DataCollectionName);

                DataEntity entity = new DataEntity(definition.DataEntityName, DataType.String, "string", dataContainer, collection);

                var samples = supplyCollector.CollectSample(entity, definition.SampleSize);
                if (samples.Count != definition.SampleSize)
                {
                    throw new ApplicationException($"The number of samples ({samples.Count}) from DataCollection '{definition.DataCollectionName}' does not match the expected value of {definition.SampleSize}.");
                }

                foreach (string sampleValue in definition.SampleValues)
                {
                    if (!samples.Contains(sampleValue))
                    {
                        throw new ApplicationException($"The sample value {sampleValue} was not found in the samples");
                    }
                }
            }
            Console.WriteLine(" - success.");
            Console.WriteLine();
            
        }
        private class TestInfo
        {
            public string SupplyCollectorName { get; set; }

            public string ConnectionString { get; set; }

            public int SchemaTableCount { get; set; }
            public int SchemaEntityCount { get; set; }

            public Dictionary<string, int> DataCollectionRowCounts { get; }

            public List<CollectSampleTestDefinition> CollectSampleTestDefinitions { get; }

            public TestInfo()
            {
                DataCollectionRowCounts = new Dictionary<string, int>();
                CollectSampleTestDefinitions = new List<CollectSampleTestDefinition>();
            }

            public void AddCollectSampleTest(string[] definitionValues)
            {
                var definition = new CollectSampleTestDefinition();
                definition.DataCollectionName = definitionValues[1].Trim();
                definition.DataEntityName = definitionValues[2].Trim();
                definition.SampleSize = Convert.ToInt32(definitionValues[3]);
                
                for (int i = 4; i < definitionValues.Length; i++)
                {
                    definition.SampleValues.Add(definitionValues[i].Trim());
                }

                CollectSampleTestDefinitions.Add(definition);
            }
        }

        private class CollectSampleTestDefinition
        {
            public string DataCollectionName { get; set; }
            public string DataEntityName { get; set; }
            public int SampleSize { get; set; }
            public List<string> SampleValues { get; }

            public CollectSampleTestDefinition()
            {
                SampleValues = new List<string>();
            }
        }
    }
}
