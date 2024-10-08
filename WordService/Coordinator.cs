using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace WordService;

public class Coordinator
{
    private IDictionary<string, DbConnection> ConnectionCache = new Dictionary<string, DbConnection>();
    private const string DOCUMENT_DB = "document-db";
    private const string OCCURRENCE_DB = "occurrence-db";
    private const string SHORT_WORD_DB = "short-word-db";
    private const string MEDIUM_WORD_DB = "medium-word-db";
    private const string LONG_WORD_DB = "long-word-db";

    public DbConnection GetDocumentConnection()
    {
        return GetConnectionByServerName(DOCUMENT_DB);
    }

    public DbConnection GetOccurrenceConnection()
    {
        return GetConnectionByServerName(OCCURRENCE_DB);
    }

    public DbConnection GetWordConnection(string word)
    {
        switch (word.Length)
        {
            case var l when (l <= 10):
                return GetConnectionByServerName(SHORT_WORD_DB);
            case var l when (l > 10 && l <= 20):
                return GetConnectionByServerName(MEDIUM_WORD_DB);
            case var l when (l >= 21):
                return GetConnectionByServerName(LONG_WORD_DB);
            default:
                throw new InvalidDataException();
        }
    }

    public IEnumerable<DbConnection> GetAllConnections()
    {
        yield return GetDocumentConnection();
        yield return GetOccurrenceConnection();
        foreach (var wordConnection in GetAllWordConnections())
        {
            yield return wordConnection;
        }
    }

    public IEnumerable<DbConnection> GetAllWordConnections()
    {
        yield return GetConnectionByServerName(SHORT_WORD_DB);
        yield return GetConnectionByServerName(MEDIUM_WORD_DB);
        yield return GetConnectionByServerName(LONG_WORD_DB);
    }

    private DbConnection GetConnectionByServerName(string serverName, int retryCount = 3)
    {
        if (ConnectionCache.TryGetValue(serverName, out var connection))
        {
            return connection;
        }

        int retries = retryCount;
        bool success = false;

        connection = new SqlConnection($"Server={serverName};User Id=sa;Password=SuperSecret7!;Encrypt=false;");
        try
        {
            //attempt at retry implementation
            while (retries > 0 && success == false)
            {
                connection.OpenAsync();
                ConnectionCache.Add(serverName, connection);
                if (connection.State == ConnectionState.Open)
                {
                    success = true;
                }
                else
                {
                    retries--;
                    Thread.Sleep(2000);
                }
            }
        }
        catch (Exception ex)
        {
            throw new ApplicationException($"Error at 'GetConnectionByServerName'. serverName; {serverName}. Exception: {ex.Message}");
        }
        return connection;
    }
}