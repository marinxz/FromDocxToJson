using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using NLog;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

using RemoveTablesFromDocx;
using ConvertDocxToText1;
using DocumentCategoryMap;
using BankDataDynamoDbDAO;


/*
 * this one is used for special purpose to download docx file from one bucket 
 * and apply filter to eliminate tables 
 * and create json file 
 * and save that to another bucket 
 * and update dynamo db 
 * 
 * extra problem is that we do not have whole path to the file so we have to get id from 
 * file name using map in from bank_data 
 * */
namespace FromDocxToJson
{
    class Program
    {
        private static IList<string> FilesToProcessInS3 = new List<string>();
        private static Logger logger = LogManager.GetCurrentClassLogger();
        /*
         * Main 
         */
        static void Main(string[] args)
        {
            // various settings 

            
            string saveToFile = @"C:\radnidio\japan-ocr-files\list-of-files.txt";
            string processingStatusFile = @"C:\radnidio\japan-ocr-files\processing-status.txt";
            string sentAlreadyFile = @"C:\radnidio\japan-ocr-files\already-sent-to-nucleus.txt";
            string emptyFilesList = @"C:\radnidio\japan-ocr-files\list-of-empty-files.txt";
            string dataDir = @"C:\radnidio\japan-ocr-files\tempdoc";

            // ProcessFiles();
            // GenerateListOfIdsForFiles(dataDir, saveToFile, emptyFilesList);
            // UploadToS3(dataDir, saveToFile);

            UpdateDynamoDb( saveToFile);

            // this method is just for test don't run
            // TestUpdateDynamoDb();
        }

        /*
         * this one will update dynamo db status 
         * for these files we will put special value into field 
         * is_converted_to_text, value will be S that will alow special handling of these files 
         * when loaded to nucleus
         */
        
        public static void UpdateDynamoDb(string saveToFile )
        {
            string pdfBucketName = "sua-liveboard";
            string docxBucketName = "sumup-docx-outbound";

            BankDataProcessingDynamoDbDAO bankDataProcessing =
                new BankDataProcessingDynamoDbDAO(Amazon.RegionEndpoint.USWest2.SystemName,
                pdfBucketName, docxBucketName);
            bool isOk = bankDataProcessing.Connect();
            if (!isOk)
            {
                logger.Error("Error in connecting to dynamo db: ");
                System.Environment.Exit(1);
            }
            // special status is to set is converted to text to S
            string isConvertedToText = "S";
            string source = "bank_of_japan";
            string language = "japanese";
            using (StreamReader sr = new StreamReader(saveToFile))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    string[] items = line.Split('|');
                    string fullJsonFileName = items[2];
                    int id = Int32.Parse(items[1]);
                    string docxFileName = items[0];
                    // we need this to insert new record 
                    string pdfFileName = Path.ChangeExtension(fullJsonFileName, ".pdf");
                    isOk = bankDataProcessing.IsIdPresent(id);
                    if (isOk)
                    {
                        logger.Debug("id in dynamo updating: {0}", id);
                        isOk = bankDataProcessing.UpdateForReprocessing(id, isConvertedToText );
                        if(!isOk)
                        {
                            logger.Error("Update not successful for id {0}", id);
                            System.Environment.Exit(1);
                        }
                    }
                    else
                    {
                        logger.Debug("Not id in dynamo inserting: {0}", id);
                        isOk = bankDataProcessing.InsertSpecial(id, source, language, pdfFileName,
                               fullJsonFileName, isConvertedToText);
                        if (!isOk)
                        {
                            logger.Error("Inseert not successful for id {0}", id);
                            System.Environment.Exit(1);
                        }

                    }
                }
            }
            bankDataProcessing.Disconnect();
        }
        /*
         * this one will report current dynamo db status before update 
         * run this to verify what is sent and what is not and what does exist in dynamo db 
         */
        public static void ReportDynamoDbStatus(string saveToFile, string sentAlreadyFile )
        {
            string pdfBucketName = "sua-liveboard";
            string docxBucketName = "sumup-docx-outbound";

            BankDataProcessingDynamoDbDAO bankDataProcessing =
                new BankDataProcessingDynamoDbDAO(Amazon.RegionEndpoint.USWest2.SystemName,
                pdfBucketName, docxBucketName);
            bool isOk = bankDataProcessing.Connect();
            if (!isOk)
            {
                logger.Error("Error in connecting to dynamo db: ");
                System.Environment.Exit(1);
            }
            StreamWriter sw = new StreamWriter(sentAlreadyFile);
            using (StreamReader sr = new StreamReader(saveToFile))
            {
                string line;
              

                while ((line = sr.ReadLine()) != null)
                {
                    string[] items = line.Split('|');
                    string fullJsonFileName = items[2];
                    int id = Int32.Parse(items[1]);
                    string docxFileName = items[0];
                    isOk = bankDataProcessing.IsIdPresent(id);
                    if (isOk)
                    {
                        logger.Debug("id in dynamo {0}", id);
                    }
                    bool isSent = bankDataProcessing.IsIdSentToNucleus(id);
                    if (isSent)
                    {
                        logger.Debug("It is sent to nucleus {0}", id);
                        sw.WriteLine("{0}|{1}|{2}", docxFileName, id, "Sent to nucleus already");
                        continue;
                    }
                    else
                    {
                    }
                }
            }
            sw.Close();
            bankDataProcessing.Disconnect();
        }

        
        
        /*
         *  this one will upload final files 
         *  
         */
        private static void UploadToS3(string dataDir, string saveToFile)
        {

            RegionEndpoint bucketRegion = RegionEndpoint.USWest2;
            IAmazonS3 s3Client = new AmazonS3Client(bucketRegion);
            string docxBucketName = "sumup-docx-outbound";

            int upCount = 0;
            int upCountErrors = 0;
            int count = 0;
            int maxCount = 3000;   // how many to process 

            using (StreamReader sr = new StreamReader(saveToFile))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    count++;
                    if (count > maxCount)
                        break;

                    string[] items = line.Split('|');
                    string fullJsonFileName = items[2];
                    int id = Int32.Parse(items[1]);
                    string docxFileName = items[0];
                    string localJsonFile = Path.Combine(
                        new string[] { dataDir, Path.ChangeExtension(docxFileName, ".json") });
                    logger.Debug(docxFileName);

                    // upload file to s3 
                    
                    logger.Debug("uploading {0} --> {1}", localJsonFile, fullJsonFileName);
                    
                    bool isOk = UploadFileToS3(s3Client, docxBucketName, fullJsonFileName, localJsonFile);
                    if (!isOk)
                    {
                        logger.Error("Error while uploading");
                        upCountErrors++;
                    }
                    else{
                    upCount++;
                    }
                    
                }
            }
            logger.Info("Uploaded to s3: {0}", upCount);
            logger.Info("Errors Uploading to s3: {0}", upCountErrors);
        }

        private static void ProcessFiles()
        {
            RegionEndpoint bucketRegion = RegionEndpoint.USWest2;

            string bucketName = "sumup-test-mm";
            string localDirForDocxFiles = @"C:\radnidio\japan-ocr-files\input";
            string extractWorkDir = @"C:\radnidio\japan-ocr-files\work";
            string tempDocDir = @"C:\radnidio\japan-ocr-files\tempdoc";

            string dbConnectionString = "server=liveboard0913.cjvgiw4swlyc.us-west-1.rds.amazonaws.com;database=sum_up;uid=yerem;pwd=sua.liveboard.2018;";
            string languageMapFile = @"C:\transfer\solid-conversions\mappings\language-codes.csv";
            string threeMapFile = @"C:\transfer\solid-conversions\mappings\mapping-from-structure-and-data-cleaned-win.csv";
            string twoMapFile = @"C:\transfer\solid-conversions\mappings\mapping-from-structure-and-data-one-level.csv";
            string nonMapFile = @"C:\temp\non-mapped-document-categories.txt";
            string pdfBucketName = "sua-liveboard";

            string docxBucketName = "sumup-docx-outbound";
            // setup various objects neesed 

            DocxTagFilter filter = new DocxTagFilter(extractWorkDir);
            // set default tags
            filter.SetupDefaultTags();

            FileToIdMapCollector collector = new FileToIdMapCollector();
            collector.connectionString = dbConnectionString;
            bool isOk;
            isOk = collector.LoadLists();
            if (!isOk)
            {
                logger.Error("Can not collect file id maps");
                System.Environment.Exit(0);
            }


            MetaDataHolderFactory.connectionString = dbConnectionString;
            MetaDataHolderFactory.loadMaps(languageMapFile, threeMapFile, twoMapFile, nonMapFile);
            MetaDataHolderFactory.S3bucket = pdfBucketName;
            // text is needed like us-west-2
            MetaDataHolderFactory.S3region = Amazon.RegionEndpoint.USWest1.SystemName;

            MetaDataHolderFactory.GetConnection();

            IAmazonS3 client = new AmazonS3Client(bucketRegion);

            BankDataProcessingDynamoDbDAO bankDataProcessing =
                new BankDataProcessingDynamoDbDAO(Amazon.RegionEndpoint.USWest2.SystemName, pdfBucketName, docxBucketName);
            isOk = bankDataProcessing.Connect();
            if (!isOk)
            {
                logger.Error("Error in connecting to dynamo db: ");
                System.Environment.Exit(1);
            }

            // skip list 
            List<string> skipList = new List<string>();
            skipList.Add("1eb5f50c344634929709f81ac09593b365f0120e.docx");
            logger.Info("Started working ");



            ListingObjectsAsync(bucketName, client).Wait();

            int ic = 0;
            int maxFile = 3000;

            foreach (string s3file in FilesToProcessInS3)
            {
                ic++;
                if (ic > maxFile)
                    break;
                Console.WriteLine("Processing: {0}", s3file);
                if (skipList.Contains(s3file))
                {
                    logger.Warn("file is skip list, skipping");
                    continue;
                }

                string docxPath = Path.Combine(localDirForDocxFiles, s3file);
                string newDocxFile = Path.Combine(tempDocDir, s3file);
                string jsonFileName = Path.ChangeExtension(newDocxFile, ".json");

                logger.Info("Local file: {0}", docxPath);
                // check do we have json file anready, if so skip 
                if (File.Exists(jsonFileName))
                {
                    logger.Info("Json file already exist, skipping");
                    continue;

                }

                // first download s3 file to local dir
                // do not load if file already exist ( better to put this in the method for some other time )
                if (!File.Exists(docxPath))
                {
                    isOk = DownloadFileFromS3(client, bucketName, s3file, docxPath);
                    if (!isOk)
                    {
                        logger.Error("file not downloaded {0}", s3file);
                        break;
                    }
                }
                else
                {
                    logger.Info("file aready downloaded");
                }

                // now filter out what is not needed in docx 
                isOk = filter.ApplyFilter(docxPath, newDocxFile, false);
                if (!isOk)
                {
                    logger.Error("Error while filtering docx");
                    break;
                }
                // convert docx to txt 
                logger.Debug("Starting extraction of the text");
                string textFileName = Path.ChangeExtension(newDocxFile, ".txt");
                DocxToText docxToText = new DocxToText();
                isOk = docxToText.ExtractTextAndSave(newDocxFile, textFileName);
                if (!isOk)
                {
                    logger.Error("Error while Extracting text");
                    break;
                }

                // now collect metadata 

                int id = collector.GetId(s3file);
                if (id == FileToIdMapCollector.MISSING_ID)
                {
                    logger.Warn("id not found: {0}", s3file);
                    continue;
                }
                logger.Info("ID: {0}", id);

                List<MetaDataHolder> mhlist = MetaDataHolderFactory.PopulateMetaDataHoldersFromDb(new int[] { id });
                MetaDataHolder holder = mhlist[0];
                isOk = holder.LoadContentFromFile(textFileName);
                if (!isOk)
                {
                    logger.Error("Error while loading content from text file {0}", textFileName);
                    continue;
                }
                // now save json file 

                holder.SaveAsJSON(jsonFileName);

                isOk = bankDataProcessing.IsIdPresent(id);
                if (isOk)
                {
                    logger.Info("id in dynamo db");
                }
                else
                {
                    logger.Info("id NOT in dynamo db");
                }
            }
            MetaDataHolderFactory.CloseConnection();
            bankDataProcessing.Disconnect();
            logger.Info("Done");
        }

        /*
        * this one will list all the files in the bucket 
        */
        static async Task ListingObjectsAsync(string bucketName, IAmazonS3 client)
        {
            try
            {
                ListObjectsV2Request request = new ListObjectsV2Request
                {
                    BucketName = bucketName,
                    MaxKeys = 50
                };
                ListObjectsV2Response response;
                do
                {
                    response = await client.ListObjectsV2Async(request);

                    // Process the response.
                    foreach (S3Object entry in response.S3Objects)
                    {
                        // Console.WriteLine("key = {0} size = {1}", entry.Key, entry.Size);
                        FilesToProcessInS3.Add(entry.Key);
                    }
                    // Console.WriteLine("Next Continuation Token: {0}", response.NextContinuationToken);
                    request.ContinuationToken = response.NextContinuationToken;
                } while (response.IsTruncated);
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                Console.WriteLine("S3 error occurred. Exception: " + amazonS3Exception.ToString());
                Console.ReadKey();
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.ToString());
                Console.ReadKey();
            }
        }
        /*
        * downloads file form s3
        */
        public static bool DownloadFileFromS3(IAmazonS3 s3Client, string bucketName, string objectKey, string filePath)
        {
            IDictionary<string, object> dic = new Dictionary<string, object>();
            bool rc = true;

            try
            {
                s3Client.DownloadToFilePath(bucketName, objectKey, filePath, dic);
            }
            catch (AmazonS3Exception ex)
            {
                rc = false;
                if (ex.ErrorCode != null && (ex.ErrorCode.Equals("InvalidAccessKeyId") ||
                    ex.ErrorCode.Equals("InvalidSecurity")))
                {
                    logger.Error("Please check the provided AWS Credentials.");
                }
                else
                {
                    logger.Error("Caught Exception: " + ex.Message);
                    logger.Error("Response Status Code: " + ex.StatusCode);
                    logger.Error("Error Code: " + ex.ErrorCode);
                    logger.Error("Request ID: " + ex.RequestId);
                }
            }
            return rc;
        }
        

        /*
          * uploads file to s3
        */
        public static bool UploadFileToS3(IAmazonS3 s3Client, string bucketName, string objectKey, string filePath)
        {
            IDictionary<string, object> dic = new Dictionary<string, object>();
            bool rc = true;
            try
            {
                s3Client.UploadObjectFromFilePath(bucketName, objectKey, filePath, dic);
            }
            catch (AmazonS3Exception ex)
            {
                rc = false;
                if (ex.ErrorCode != null && (ex.ErrorCode.Equals("InvalidAccessKeyId") ||
                    ex.ErrorCode.Equals("InvalidSecurity")))
                {
                    logger.Error("Please check the provided AWS Credentials.");
                }
                else
                {
                    logger.Error("Caught Exception: " + ex.Message);
                    logger.Error("Response Status Code: " + ex.StatusCode);
                    logger.Error("Error Code: " + ex.ErrorCode);
                    logger.Error("Request ID: " + ex.RequestId);
                }
            }

            return rc;
        }

        /*
         * this one will generate list of ids  
         * that have to be processed and save them in the file 
         * for future consuption
         */
        public static void GenerateListOfIdsForFiles(string dataDir, string saveToFile, string emptyFilesList)
        {
            FileToIdMapCollector collector = new FileToIdMapCollector();
            string dbConnectionString = "server=liveboard0913.cjvgiw4swlyc.us-west-1.rds.amazonaws.com;database=sum_up;uid=yerem;pwd=sua.liveboard.2018;";
            collector.connectionString = dbConnectionString;
            logger.Info("Collecting list");
            bool isOk;
            isOk = collector.LoadLists();
            if (!isOk)
            {
                logger.Error("Can not collect file id maps");
                System.Environment.Exit(0);
            }

            // list local files
            StreamWriter sw2 = new StreamWriter(emptyFilesList);
            string[] fileArray = Directory.GetFiles(dataDir, "*.json");
            using (StreamWriter sw = new StreamWriter(saveToFile) )
            {
                foreach (string ffile in fileArray)
                {
                    logger.Info(ffile);
                    // test is text file empty 
                    string fpath = Path.ChangeExtension(ffile, ".txt");
                    long length = new System.IO.FileInfo(fpath).Length;
                    if (length == 0)
                    {
                        logger.Info("file len is 0");
                    }
                    string name = Path.GetFileNameWithoutExtension(ffile) + ".docx";
                    int id = collector.GetId(name);

                    if (id == FileToIdMapCollector.MISSING_ID)
                    {
                        logger.Warn("Document without id: {0}", name);
                        sw2.WriteLine(name + "||file does not have id");
                    }
                    else
                    {
                        string fullFileName = collector.GetFullFileName(name);
                        fullFileName = Path.ChangeExtension(fullFileName, ".json");
                        logger.Info("{0} --> {1}", name, id);
                        // sw.WriteLine(name + "|" + id.ToString() + "|" + fullFileName);
                        if (length == 0)
                        {
                            sw2.WriteLine(name + "|" + id.ToString() + "|" + fullFileName);
                        }
                        else
                        {
                            sw.WriteLine(name + "|" + id.ToString() + "|" + fullFileName);
                        }
                    }
                }
            }
            sw2.Close();
        }
        /* 
         * this is not the best place to put this but for now it is good enough
         */
        private static void TestUpdateDynamoDb()
        {
            string pdfBucketName = "sua-liveboard";
            string docxBucketName = "sumup-docx-outbound";

            BankDataProcessingDynamoDbDAO bankDataProcessing =
                new BankDataProcessingDynamoDbDAO(Amazon.RegionEndpoint.USWest2.SystemName,
                pdfBucketName, docxBucketName);
            bool isOk = bankDataProcessing.Connect();
            if (!isOk)
            {
                logger.Error("Error in connecting to dynamo db: ");
                System.Environment.Exit(1);
            }

            int id = 48800;
            string isConvertedToText = "S";
            bankDataProcessing.UpdateForReprocessing(id, isConvertedToText);
        }

    }
}
