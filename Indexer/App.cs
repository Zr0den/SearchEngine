using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.Json;

namespace Indexer
{
    public class App
    {
        public void Run()
        {
            var api = new HttpClient() { BaseAddress = new Uri("http://word-service") };
            
            try
            {
                api.Send(new HttpRequestMessage(HttpMethod.Delete, "Database"));
                api.Send(new HttpRequestMessage(HttpMethod.Post, "Database"));
            }
            catch (Exception ex)
            {
                throw new ApplicationException($"Error at api call in App.cs.run. Exception: {ex.Message}");
            }

            Crawler crawler = new Crawler();

            //Could have a single try catch, but separate ones can make it easier to locate which part of the code failed
            try
            {
                var directoryArray = new DirectoryInfo("maildir").GetDirectories();
                var directories = new List<DirectoryInfo>(directoryArray).OrderBy(d => d.Name).AsEnumerable();

                DateTime start = DateTime.Now;
                foreach (var directory in directories)
                {
                    crawler.IndexFilesIn(directory, new List<string> { ".txt" });
                }

                TimeSpan used = DateTime.Now - start;
                Console.WriteLine("DONE! used " + used.TotalMilliseconds);

            }
            catch (Exception ex)
            {
                throw new ApplicationException($"Error in App.cs.run. Exception: {ex.Message}");
            }
        }
    }
}
