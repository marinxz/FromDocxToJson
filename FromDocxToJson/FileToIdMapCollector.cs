using System;
using NLog;
using MySql.Data.MySqlClient;
using System.Collections.Generic;
using System.IO;
    /*
    * this one will populate map of document names to document ids 
    * this might cause some problems with duplicates 
    * most recent will be used
    */


namespace FromDocxToJson
{
    class FileToIdMapCollector
    {
        private static Logger logger = NLog.LogManager.GetCurrentClassLogger();

        public string connectionString { get; set; } = "server=liveboard0913.cjvgiw4swlyc.us-west-1.rds.amazonaws.com;database=sum_up;uid=yerem;pwd=sua.liveboard.2018;";

        public IDictionary<string, int> fileToIdMap { get; } = new Dictionary<string, int>();
        public IDictionary<string, string> fileToFullFileMap { get; } = new Dictionary<string, string >();
        public static readonly int MISSING_ID = -1;
        public bool LoadLists()
        {
            bool isOk = false;
            try
            {
                MySqlConnection cnn = new MySqlConnection(connectionString);
                cnn.Open();
                logger.Debug("Connection established ");

                string query = "SELECT file_url, id " +
                        " from bank_data " +
                        " where source = 'bank_of_japan' " +
                        " and file_url like '%.pdf' " +
                        "order by 1,2 ";

                MySqlCommand cmd = new MySqlCommand(query, cnn);
                logger.Debug(cmd.CommandText);
                MySqlDataReader reader = cmd.ExecuteReader();
                int rcount = 0;
                while (reader.Read())
                {
                    string fileUrl = reader.GetString(0);

                    fileUrl = Path.ChangeExtension(fileUrl, ".docx");
                    string fileName = Path.GetFileName(fileUrl);
                    int id = reader.GetInt32(1);
                    rcount++;

                    // logger.Info("{0} {1} {2}", fileUrl, fileName, id);

                    if(fileToIdMap.ContainsKey( fileName))
                    {
                        fileToIdMap[fileName] = id;
                        fileToFullFileMap[fileName] = fileUrl;
                        logger.Warn("Duplicate detected: {0}", fileName);
                    }
                    else
                    {
                        fileToIdMap.Add(new KeyValuePair<string, int>(fileName, id));
                        fileToFullFileMap.Add( new KeyValuePair<string, string>(fileName, fileUrl));
                    }
                }
                reader.Close();
                cnn.Close();

                isOk = true;
                logger.Info("records red: {0}", rcount);
                logger.Info("file id map size: {0}", fileToIdMap.Count);
                logger.Info("file id map size: {0}", fileToFullFileMap.Count);
            }
            catch (Exception ex)
            {
                logger.Debug("Problem with sql connection or query ! ");
                logger.Debug(ex.Message);
            }
            return isOk;
        }

        public int GetId( string fileName)
        {
            int id = MISSING_ID;
            if ( this.fileToIdMap.ContainsKey(fileName))
            {
                id = fileToIdMap[fileName];
            }
            return id;
        }

        public string GetFullFileName(string fileName)
        {
            string fullFileName = String.Empty;
            if (this.fileToFullFileMap.ContainsKey(fileName))
            {
                fullFileName = this.fileToFullFileMap[fileName];
            }
            return fullFileName;
        }

    }
}
