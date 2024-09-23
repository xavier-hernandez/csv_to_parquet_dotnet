using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

class Program
{
    static void Main(string[] args)
    {
        string csvFilePath = "input.csv";
        string parquetFilePath = "output.parquet";
        string errorFilePath = "errors.txt";
        string delimiter = ",";
        //bool hasHeader = args.Length > 0 && bool.Parse(args[0]);  // Specify via argument if CSV has a header

        bool hasHeader = false;

        //delete error file
        if (File.Exists(errorFilePath))
        {
            File.Delete(errorFilePath);
        }

        //delete otuput file
        if (File.Exists(parquetFilePath))
        {
            File.Delete(parquetFilePath);
        }

        // Load the CSV and convert to Parquet
        ConvertCsvToParquet(csvFilePath, parquetFilePath, errorFilePath, hasHeader, delimiter);
    }
    public static void ConvertCsvToParquet(string csvFilePath, string parquetFilePath, string errorFilePath, bool hasHeader, string delimiter)
    {
        // Convert to Parquet format
        //_ = WriteToParquetViaBatchAsync(csvFilePath, parquetFilePath, errorFilePath, hasHeader);
        _ = WriteToParquetAsync(csvFilePath, parquetFilePath, errorFilePath, hasHeader, delimiter);
        //        _ = WriteToParquetAsync(parquetFilePath, headers, records, progress);

    }

    public static List<string> GenerateFakeHeaders(int columnCount)
    {
        var headers = new List<string>();
        for (int i = 1; i <= columnCount; i++)
        {
            headers.Add($"Column{i}");
        }
        return headers;
    }

    public static void AppendToErrorFile(string filePath, string content)
    {
        try
        {
            using StreamWriter sw = new StreamWriter(filePath, true);
            sw.WriteLine(content);
        }
        catch (Exception ex)
        {
            Console.WriteLine("An error occurred: " + ex.Message);
        }
    }
    public static async Task WriteToParquetViaBatchAsync(string csvFilePath, string parquetFilePath, string errorFilePath, bool hasHeader, string delimiter, int batchSize = 1000)
    {
        try
        {
            using var reader = new StreamReader(csvFilePath);

            var csvConfiguration = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = $"{delimiter}",
                HasHeaderRecord = hasHeader,
                BadDataFound = context =>
                {
                    AppendToErrorFile(errorFilePath, context.RawRecord);
                }
            };

            using var csv = new CsvReader(reader, csvConfiguration);
            // Read the first row to check the number of columns
            csv.Read();
            var columnCount = csv.Parser.Count;

            // Read headers if they exist, otherwise generate fake headers
            var headers = new List<string>();
            if (hasHeader)
            {
                csv.ReadHeader();
                headers.AddRange(csv.HeaderRecord);
            }
            else
            {
                headers = GenerateFakeHeaders(columnCount);
            }

            // Define the schema
            var fields = headers.Select(header => new DataField(header, typeof(string))).ToArray();
            var schema = new ParquetSchema(fields);

            using var fileStream = File.OpenWrite(parquetFilePath);
            using var parquetWriter = await ParquetWriter.CreateAsync(schema, fileStream);
            parquetWriter.CompressionMethod = CompressionMethod.Snappy;

            IProgress<int>? progress = null;
            progress = new Progress<int>(percent =>
            {
                Console.Write($"\rProgress: {percent}% completed");
            });

            int totalRowsProcessed = 0;
            int totalCellsProcessed = 0;

            // Write in batches
            while (csv.Read())
            {
                var batch = new List<string[]>();
                for (int i = 0; i < batchSize && csv.Read(); i++)
                {
                    var row = new string[columnCount];
                    for (int j = 0; j < columnCount; j++)
                    {
                        row[j] = csv.GetField(j);
                    }
                    batch.Add(row);
                }

                if (batch.Count > 0)
                {
                    using var groupWriter = parquetWriter.CreateRowGroup();
                    for (int i = 0; i < columnCount; i++)
                    {
                        var columnData = batch.Select(r => r[i]).ToArray();
                        var dataColumn = new DataColumn(schema.DataFields[i], columnData);
                        await groupWriter.WriteColumnAsync(dataColumn);
                    }
                    totalRowsProcessed += batch.Count;
                    totalCellsProcessed += batch.Count * columnCount;
                    progress.Report((totalCellsProcessed * 100) / totalCellsProcessed);
                }
            }

            Console.WriteLine("\nWrite operation completed.");
        }
        catch (Exception ex)
        {
            throw ex;
        }
    }

    public static async Task WriteToParquetAsync(string csvFilePath, string parquetFilePath, string errorFilePath, bool hasHeader, string delimiter)
    {
        try
        {
            using var reader = new StreamReader(csvFilePath);

            var csvConfiguration = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = $"{delimiter}",
                HasHeaderRecord = hasHeader,
                BadDataFound = context =>
                {
                    AppendToErrorFile(errorFilePath, context.RawRecord);
                }
            };

            using var csv = new CsvReader(reader, csvConfiguration);

            // Read the first row to check the number of columns
            csv.Read();
            var columnCount = csv.Parser.Count;

            // Read headers if they exist, otherwise generate fake headers
            var headers = new List<string>();
            if (hasHeader)
            {
                csv.ReadHeader();
                headers.AddRange(csv.HeaderRecord);
            }
            else
            {
                headers = GenerateFakeHeaders(columnCount);
            }

            // Read CSV data into memory
            var records = new List<List<string>>();

            try
            {
                while (csv.Read())
                {
                    var row = new List<string>();
                    for (int i = 0; i < columnCount; i++)
                    {
                        row.Add(csv.GetField(i));
                    }
                    records.Add(row);
                }
            }
            catch (CsvHelperException ex)
            {
                Console.WriteLine($"Bad headers, check the file or your delimiter:");
                Console.WriteLine($"{ex.Message}");
                return;
            }

            // Progress handler to show progress in the console
            IProgress<int>? progress = null;
            progress = new Progress<int>(percent =>
            {
                Console.Write($"\rProgress: {percent}% completed"); // Update progress in console
            });

            // Define the schema
            var fields = new List<DataField>();
            foreach (var header in headers)
            {
                fields.Add(new DataField(header, typeof(string)));
            }
            var schema = new ParquetSchema(fields.ToArray());


            int totalCells = records.Count * headers.Count;
            int progressCount = 0;

            // Prepare columns of data for each header
            var columns = new List<DataColumn>();
            for (int i = 0; i < headers.Count; i++)
            {
                var columnData = new string[records.Count];
                for (int j = 0; j < records.Count; j++)
                {
                    columnData[j] = records[j][i];

                    // Update progress for each cell written
                    progressCount++;
                    progress?.Report((progressCount * 100) / totalCells); // Report progress as percentage
                }
                columns.Add(new DataColumn(schema.DataFields[i], columnData));
            }

            using (Stream fileStream = File.OpenWrite(parquetFilePath))
            {
                using (var parquetWriter = await ParquetWriter.CreateAsync(schema, fileStream))
                {
                    parquetWriter.CompressionMethod = CompressionMethod.Snappy;

                    using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                    {
                        foreach (var column in columns)
                        {
                            await groupWriter.WriteColumnAsync(column);
                        }
                    }
                }
            }

            Console.WriteLine("\nWrite operation completed.");
        }
        catch (Exception ex)
        {
            throw ex;
        }
    }
}
