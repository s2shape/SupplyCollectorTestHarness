using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;
using System.Threading;
using S2.BlackSwan.SupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;

namespace SupplyCollectorTestHarness
{
    class Program
    {
        private static bool _debug = false;

        private static Assembly Assembly_Resolving(AssemblyLoadContext context, AssemblyName name)
        {
            if (_debug)
                Console.WriteLine($"[DEBUG] Resolving {name.FullName}");

            var foundDlls = Directory.GetFileSystemEntries(new FileInfo(Environment.CurrentDirectory).FullName, name.Name + ".dll", SearchOption.AllDirectories);
            if (foundDlls.Any())
            {
                if (_debug)
                {
                    foreach (var foundDll in foundDlls)
                    {
                        Console.WriteLine($"  resolved to {foundDll}");
                    }
                }

                return context.LoadFromAssemblyPath(foundDlls[0]);
            }

            return context.LoadFromAssemblyName(name);
        }

        static void Main(string[] args)
        {
            Console.WriteLine("SupplyCollectorTestHarness v." + typeof(Program).Assembly.GetName().Version);
            string filename = "test_harness.config";
            if (args.Length > 0) {
                filename = args[args.Length - 1];
            }

            Console.WriteLine($"   loading {filename}, args len={args.Length}...");
            TestInfo testInfo = GetTestInfoFromFile(filename);

            Console.WriteLine("Testing " + testInfo.SupplyCollectorName);
            Console.WriteLine();

            string supplyCollectorPath = Path.Combine(Environment.CurrentDirectory, testInfo.SupplyCollectorName + ".dll");

            AssemblyLoadContext.Default.Resolving += Assembly_Resolving;
            Assembly supplyCollectorAssembly = AssemblyLoadContext.Default.LoadFromAssemblyPath(supplyCollectorPath);

            Type supplyCollectorType = supplyCollectorAssembly.GetType(String.Format("{0}.{0}", testInfo.SupplyCollectorName));

            ISupplyCollector supplyCollector = (ISupplyCollector)Activator.CreateInstance(supplyCollectorType);

            DataContainer dataContainer = new DataContainer() {ConnectionString = testInfo.ConnectionString};

            if (args.Length > 0) {
                if ("-load-test".Equals(args[0], StringComparison.InvariantCultureIgnoreCase)) {
                    int testIndex = Int32.Parse(args[1]);
                    
                    LoadTestEntryPoint(supplyCollector, dataContainer, testInfo, testIndex);
                    return;
                }
            }

            try {
                TestDataStoreTypes(supplyCollector);

                TestConnection(supplyCollector, dataContainer);

                TestGetSchema(supplyCollector, dataContainer, testInfo);

                TestCollectSample(supplyCollector, dataContainer, testInfo);

                TestRandomSampling(supplyCollector, dataContainer, testInfo);

                TestDataMetrics(supplyCollector, dataContainer, testInfo);

                TestMemoryUsageAndProcessingTime(testInfo);

                Console.WriteLine("All tests passed.");
                Environment.ExitCode = 0;
            }
            catch (Exception ex) {
                Console.WriteLine($"FAIL! Err: {ex}");
                Environment.ExitCode = 1;
            }
        }

        private static void TestMemoryUsageAndProcessingTime(TestInfo testInfo) {
            Console.Write("Testing memory usage ");
            if (testInfo.LoadTestDefinitions.Count == 0) {
                Console.WriteLine(" - skipped.");
                Console.WriteLine();
                return;
            }

            //Copy .dll
            var sourceDir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            var destFileName = Path.Combine(Environment.CurrentDirectory, "SupplyCollectorTestHarness.dll");
            var configFileName = Path.Combine(Environment.CurrentDirectory, "SupplyCollectorTestHarness.runtimeconfig.json");

            bool exists = File.Exists(destFileName);

            File.Copy(
                Path.Combine(sourceDir, "SupplyCollectorTestHarness.dll"),
                destFileName,
                true
                );
            File.Copy(
                Path.Combine(sourceDir, "SupplyCollectorTestHarness.runtimeconfig.json"),
                configFileName,
                true
                );

            for (int i = 0; i < testInfo.LoadTestDefinitions.Count; i++) {
                var definition = testInfo.LoadTestDefinitions[i];

                Console.WriteLine();
                Console.Write($"#{i}. Loading {definition.SampleSize} samples... ");
                var process = Process.Start("/bin/sh", "-c 'SupplyCollectorTestHarness -load-test " + i + " test_harness.config'");
                if (process == null)
                {
                    throw new ApplicationException("Failed to start child process for load testing.");
                }

                while (!process.HasExited)
                {
                    //process.PrivateMemorySize64
                    var totalTime = DateTime.Now.Subtract(process.StartTime);

                    if (definition.MaxRunTimeSec > 0 && totalTime.TotalSeconds > definition.MaxRunTimeSec) {
                        try {
                            process.Kill();
                        }
                        catch (Exception) {
                            /* ignore */
                        }

                        throw new ApplicationException(
                            $"Process is running for {totalTime}, which is more than maximum of {definition.MaxRunTimeSec} seconds");
                    }

                    if (definition.MaxMemoryUsageMb > 0 &&
                        process.WorkingSet64 / (1024 * 1024) > definition.MaxMemoryUsageMb) {
                        try {
                            process.Kill();
                        }
                        catch (Exception) {
                            /* ignore */
                        }

                        throw new ApplicationException(
                            $"Process consumes {process.WorkingSet64} bytes, which is more than maximum of {definition.MaxMemoryUsageMb} Mb");
                    }

                    Thread.Sleep(1000);
                }

                Console.WriteLine(" - success.");
            }

            if (!exists) {
                File.Delete(destFileName);
                File.Delete(configFileName);
            }

            Console.WriteLine("All load tests passed.");
            Console.WriteLine();
        }

        private static void LoadTestEntryPoint(ISupplyCollector supplyCollector, DataContainer dataContainer, TestInfo testInfo, int testIndex) {
            Console.Write("Load testing... ");

            var definition = testInfo.LoadTestDefinitions[testIndex];
            DataCollection collection = new DataCollection(dataContainer, definition.DataCollectionName);

            DataEntity entity = new DataEntity(definition.DataEntityName, DataType.String, "string", dataContainer, collection);

            var samples = supplyCollector.CollectSample(entity, definition.SampleSize);
            
            Console.WriteLine($" - success, collected {samples.Count} samples.");
            Console.WriteLine();
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
                        case "randomsample":
                            testInfo.AddRandomSampleTest(lineParts);
                            break;
                        case "datacollectionmetrics":
                            testInfo.AddMetricsTest(lineParts);
                            break;
                        case "loadtest":
                            testInfo.AddLoadTest(lineParts);
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
            if (testInfo.CollectSampleTestDefinitions.Count == 0)
            {
                Console.WriteLine(" - skipped.");
                Console.WriteLine();
                return;
            }

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

        private static void TestRandomSampling(ISupplyCollector supplyCollector, DataContainer dataContainer, TestInfo testInfo) {
            Console.Write("Testing CollectSample() - random sampling method");
            if (testInfo.RandomSampleTestDefinitions.Count == 0)
            {
                Console.WriteLine(" - skipped.");
                Console.WriteLine();
                return;
            }

            foreach (var definition in testInfo.RandomSampleTestDefinitions)
            {
                DataCollection collection = new DataCollection(dataContainer, definition.DataCollectionName);
                DataEntity entity = new DataEntity(definition.DataEntityName, DataType.String, "string", dataContainer, collection);

                bool equals = true;

                List<string> prevSamples = null;
                for (int i = 0; i < 3; i++) {

                    var samples = supplyCollector.CollectSample(entity, definition.SampleSize);
                    if (samples.Count != definition.SampleSize) {
                        throw new ApplicationException(
                            $"The number of samples ({samples.Count}) from DataCollection '{definition.DataCollectionName}' does not match the expected value of {definition.SampleSize}.");
                    }

                    if (prevSamples != null) {
                        equals = !samples.Except(prevSamples).Union(prevSamples.Except(samples)).Any();

                        if (!equals)
                            break;
                    }

                    prevSamples = samples;
                }

                if (equals) {
                    throw new ApplicationException($"Samples from DataCollection '{definition.DataCollectionName}' are equal after 3 read attempts.");
                }
            }

            Console.WriteLine(" - success.");
            Console.WriteLine();
        }

        private static void TestDataMetrics(ISupplyCollector supplyCollector, DataContainer dataContainer, TestInfo testInfo)
        {
            Console.Write("Testing GetDataCollectionMetrics() ");
            if (testInfo.DataMetricsTestDefinitions.Count == 0)
            {
                Console.WriteLine(" - skipped.");
                Console.WriteLine();
                return;
            }

            var metrics = supplyCollector.GetDataCollectionMetrics(dataContainer);

            foreach (var definition in testInfo.DataMetricsTestDefinitions) {
                var metric = metrics.Find(x => x.Name.Equals(definition.DataCollectionName));
                if (metric == null)
                {
                    throw new ApplicationException($"Metric for DataCollection '{definition.DataCollectionName}' is not found.");
                }

                if (metric.RowCount != definition.RowCount) {
                    throw new ApplicationException(
                        $"Row count for DataCollection '{definition.DataCollectionName}' ({metric.RowCount}) does not match expected value {definition.RowCount}.");
                }

                var usedSpaceRounded = Math.Round(metric.UsedSpaceKB, definition.UsedSizePrecision);
                var totalSpaceRounded = Math.Round(metric.TotalSpaceKB, definition.TotalSizePrecision);

                if (!Decimal.Equals(definition.UsedSize, usedSpaceRounded)) {
                    throw new ApplicationException(
                    $"Used size for DataCollection '{definition.DataCollectionName}' ({usedSpaceRounded}, rounded from {metric.UsedSpaceKB}) does not match expected value {definition.UsedSize}.");
                }

                if (!Decimal.Equals(definition.TotalSize, totalSpaceRounded)) {
                    throw new ApplicationException(
                    $"Total size for DataCollection '{definition.DataCollectionName}' ({totalSpaceRounded}, rounded from {metric.TotalSpaceKB}) does not match expected value {definition.TotalSize}.");
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

            public List<CollectSampleTestDefinition> CollectSampleTestDefinitions { get; }
            public List<RandomSampleTestDefinition> RandomSampleTestDefinitions { get; }
            public List<DataMetricsTestDefinition> DataMetricsTestDefinitions { get; }
            public List<LoadTestDefinition> LoadTestDefinitions { get; }

            public TestInfo()
            {
                CollectSampleTestDefinitions = new List<CollectSampleTestDefinition>();
                RandomSampleTestDefinitions = new List<RandomSampleTestDefinition>();
                DataMetricsTestDefinitions = new List<DataMetricsTestDefinition>();
                LoadTestDefinitions = new List<LoadTestDefinition>();
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

            public void AddRandomSampleTest(string[] definitionValues)
            {
                var definition = new RandomSampleTestDefinition();
                definition.DataCollectionName = definitionValues[1].Trim();
                definition.DataEntityName = definitionValues[2].Trim();
                definition.SampleSize = Convert.ToInt32(definitionValues[3]);
                
                RandomSampleTestDefinitions.Add(definition);
            }

            public void AddLoadTest(string[] definitionValues)
            {
                var definition = new LoadTestDefinition();
                definition.DataCollectionName = definitionValues[1].Trim();
                definition.DataEntityName = definitionValues[2].Trim();
                definition.SampleSize = Convert.ToInt32(definitionValues[3]);
                definition.MaxMemoryUsageMb = Convert.ToInt32(definitionValues[4]);
                definition.MaxRunTimeSec = Convert.ToInt32(definitionValues[5]);
                
                LoadTestDefinitions.Add(definition);
            }

            public void AddMetricsTest(string[] definitionValues)
            {
                var definition = new DataMetricsTestDefinition();
                definition.DataCollectionName = definitionValues[1].Trim();
                definition.RowCount = Int64.Parse(definitionValues[2].Trim());
                definition.TotalSize = Decimal.Parse(definitionValues[3].Trim(), CultureInfo.InvariantCulture);
                if (definitionValues[3].IndexOf(".") > 0) {
                    definition.TotalSizePrecision = definitionValues[3].Substring(definitionValues[3].IndexOf(".") + 1)
                        .Trim().Length;
                }
                definition.UsedSize = Decimal.Parse(definitionValues[4].Trim(), CultureInfo.InvariantCulture);
                if (definitionValues[4].IndexOf(".") > 0)
                {
                    definition.UsedSizePrecision = definitionValues[3].Substring(definitionValues[4].IndexOf(".") + 1)
                        .Trim().Length;
                }

                DataMetricsTestDefinitions.Add(definition);
            }
        }

        private class SampleTestDefinition {
            public string DataCollectionName { get; set; }
            public string DataEntityName { get; set; }
            public int SampleSize { get; set; }
        }

        private class CollectSampleTestDefinition : SampleTestDefinition
        {
            public List<string> SampleValues { get; }

            public CollectSampleTestDefinition()
            {
                SampleValues = new List<string>();
            }
        }

        private class RandomSampleTestDefinition : SampleTestDefinition {
        }

        private class LoadTestDefinition : SampleTestDefinition {
            public int MaxMemoryUsageMb { get; set; }
            public int MaxRunTimeSec { get; set; }
        }

        private class DataMetricsTestDefinition {
            public string DataCollectionName { get; set; }
            public long RowCount { get; set; }
            public decimal TotalSize { get; set; }
            public int TotalSizePrecision { get; set; }
            public decimal UsedSize { get; set; }
            public int UsedSizePrecision { get; set; }
        }
    }
}
