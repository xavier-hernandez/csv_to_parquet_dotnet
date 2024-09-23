# CVS file to parquet file, using .net

Converts a csv to a parquet file, per the delimiter, in .NET.

Nuget packages used
- CsvHelper
- Parquet.Net

This solution which is "as-is" converts a CSV file into a Parquet file. I created this because I could not find anything .NET based or Windows based that I could use to convert a file. '

The solution requires an input file, output file, error file, delimiter, and has header (true/false). If the files does not have a header than a fake header is created for the output file. The file is processed under the assumption that all the columns are strings. If an error occurs for a row then that row is written to the error file to be manually looked at. These errors occur sometimes if the row doesn't match the columns already created by the first row of data.

The only other Windows app I could find was this https://superintendent.app but you need to load all the data then export it.

Further enhancements:
- I wrote a batch function to process the file faster but the count appears to be off. Not sure why.
- Performance enhancements. Currently reads and coverts file row by row; which is why I tried doing batches.
- Better errors handling
- Except arguments from the console
- Read from file and do not load file into memory

## Note
- The current solution loads the entire file into memory so be sure you have enough RAM to process the file otherwise your system might become unstable. If your debugging then the memory footprint will be bigger than if you use release.