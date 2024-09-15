using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ConsoleSearch
{
    public class SearchLogic
    {
        private HttpClient api = new() { BaseAddress = new Uri("http://word-service") };

        Dictionary<string, int> mWords;

        public SearchLogic()
        {
            var response = api.Send(new HttpRequestMessage(HttpMethod.Get, "Word"));
            var content = response.Content.ReadAsStringAsync().Result;
            mWords = JsonSerializer.Deserialize<Dictionary<string, int>>(content);
            //mWords = mDatabase.GetAllWords();
        }

        public int GetIdOf(string word)
        {
            if (mWords.ContainsKey(word))
                return mWords[word];
            return -1;
        }

        public Dictionary<int, int> GetDocuments(List<int> wordIds)
        {
            var url = "Document/GetByWordIds?wordIds=" + string.Join("&wordIds=", wordIds);
            var response = api.Send(new HttpRequestMessage(HttpMethod.Get, url));
            var content = response.Content.ReadAsStringAsync().Result;
            Console.WriteLine(content);
            return JsonSerializer.Deserialize<Dictionary<int, int>>(content);
           //return mDatabase.GetDocuments(wordIds);
        }

        public List<string> GetDocumentDetails(List<int> docIds)
        {
            var url = "Document/GetByDocIds?docIds=" + string.Join("&docIds=", docIds);
            var response = api.Send(new HttpRequestMessage(HttpMethod.Get, url));
            var content = response.Content.ReadAsStringAsync().Result;
            return JsonSerializer.Deserialize<List<string>>(content);
            //return mDatabase.GetDocDetails(docIds);
        }
    }
}