using System;
using System.Formats.Tar;
using System.IO;
using System.IO.Compression;

namespace Indexer
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                DecompressGzipFile("enron/mikro.tar.gz", "mails.tar");
                if (Directory.Exists("maildir")) Directory.Delete("maildir", true);
                TarFile.ExtractToDirectory("mails.tar", ".", false);

                new Renamer().Crawl(new DirectoryInfo("maildir"));
                new App().Run();
            }
            catch (Exception ex) 
            {
                throw new ApplicationException($"Error in Indexer.Program.Main: {ex.Message}");
            }
            
        }
        
        static void DecompressGzipFile(string compressedFilePath, string decompressedFilePath)
        {
            try
            {
                using (FileStream compressedFileStream = File.OpenRead(compressedFilePath))
                {
                    using (FileStream decompressedFileStream = File.Create(decompressedFilePath))
                    {
                        using (GZipStream gzipStream = new GZipStream(compressedFileStream, CompressionMode.Decompress))
                        {
                            gzipStream.CopyTo(decompressedFileStream);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new ApplicationException($"Error in DecompressGzipFile: {ex.Message}");
            }

        }
    }
}