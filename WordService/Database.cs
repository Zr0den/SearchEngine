using System;
using System.Data;
using System.Data.Common;
using System.Reflection.Metadata.Ecma335;
using Microsoft.Data.SqlClient;
using Microsoft.Identity.Client;

namespace WordService;

public class Database
{
    private Coordinator coordinator = new Coordinator();
    private static Database instance = new Database();

    public static Database GetInstance()
    {
        return instance;
    }

    public async Task DeleteDatabase()
    {
        foreach (var connection in coordinator.GetAllConnections())
        {
            await ExecuteAsync(connection, "DROP TABLE IF EXISTS Occurrences");
            await ExecuteAsync(connection, "DROP TABLE IF EXISTS Words");
            await ExecuteAsync(connection, "DROP TABLE IF EXISTS Documents");
        }
    }

    public async Task RecreateDatabase()
    {
        await ExecuteAsync(coordinator.GetDocumentConnection(), "CREATE TABLE Documents(id INTEGER PRIMARY KEY, url VARCHAR(500))");
        await ExecuteAsync(coordinator.GetOccurrenceConnection(), "CREATE TABLE Occurrences(wordId INTEGER, docId INTEGER)");

        foreach (var connection in coordinator.GetAllWordConnections())
        {
            await ExecuteAsync(connection, "CREATE TABLE Words(id INTEGER PRIMARY KEY, name VARCHAR(500))");
        }
    }

    // key is the id of the document, the value is number of search words in the document
    public async Task<Dictionary<int, int>> GetDocuments(List<int> wordIds)
    {
        var res = new Dictionary<int, int>();

        var sql = @"SELECT docId, COUNT(wordId) AS count FROM Occurrences WHERE wordId IN " + AsString(wordIds) +
                  " GROUP BY docId ORDER BY count DESC;";

        var connection = coordinator.GetOccurrenceConnection();
        var selectCmd = connection.CreateCommand();
        selectCmd.CommandText = sql;

        try
        {
            using (var reader = await selectCmd.ExecuteReaderAsync())
            {
                while (reader.Read())
                {
                    var docId = reader.GetInt32(0);
                    var count = reader.GetInt32(1);

                    res.Add(docId, count);
                }
            }
        }
        catch (Exception ex)
        {
            throw new ApplicationException($"Error at 'GetDocuments'. Exception: {ex.Message}");
        }

        return res;
    }

    private string AsString(List<int> x)
    {
        return string.Concat("(", string.Join(',', x.Select(i => i.ToString())), ")");
    }

    public async Task<List<string>> GetDocDetails(List<int> docIds)
    {
        List<string> res = new List<string>();

        var connection = coordinator.GetDocumentConnection();
        var selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT * FROM Documents WHERE id IN " + AsString(docIds);

        try
        {
            using (var reader = await selectCmd.ExecuteReaderAsync())
            {
                while (reader.Read())
                {
                    var url = reader.GetString(1);

                    res.Add(url);
                }
            }
        }
        catch (Exception ex)
        {
            throw new ApplicationException($"Error at 'GetDocDetails'. Exception: {ex.Message}");
        }

        return res;
    }

    private static async Task ExecuteAsync(DbConnection connection, string sql)
    {
        try
        {
            using var trans = connection.BeginTransaction();
            var cmd = connection.CreateCommand();
            cmd.Transaction = trans;
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync();
            trans.Commit();
        }
        catch (Exception ex)
        {
            throw new ApplicationException($"Error at 'ExecuteAsync'. connection; {connection}, sql; {sql}. Exception: {ex.Message}");
        }
    }

    private static void Execute(DbConnection connection, string sql)
    {
        try
        {
            using var trans = connection.BeginTransaction();
            var cmd = connection.CreateCommand();
            cmd.Transaction = trans;
            cmd.CommandText = sql;
            cmd.ExecuteNonQueryAsync();
            trans.Commit();
        }
        catch (Exception ex)
        {
            throw new ApplicationException($"Error at 'Execute'. connection; {connection}, sql; {sql}. Exception: {ex.Message}");
        }
    }

    internal async void InsertAllWords(Dictionary<string, int> res)
    {
        try
        {
            foreach (var p in res)
            {
                var connection = coordinator.GetWordConnection(p.Key);
                using (var transaction = await connection.BeginTransactionAsync())
                {
                    var command = connection.CreateCommand();
                    command.Transaction = transaction;
                    command.CommandText = @"INSERT INTO Words(id, name) VALUES(@id,@name)";

                    var paramName = command.CreateParameter();
                    paramName.ParameterName = "name";
                    command.Parameters.Add(paramName);

                    var paramId = command.CreateParameter();
                    paramId.ParameterName = "id";
                    command.Parameters.Add(paramId);

                    paramName.Value = p.Key;
                    paramId.Value = p.Value;
                    await command.ExecuteNonQueryAsync();

                    transaction.Commit();
                }

            }
        }

        catch (Exception ex)
        {
            throw new ApplicationException($"Error at 'InsertAllWords'. Exception: {ex.Message}");
        }
    }

    internal async void InsertAllOcc(int docId, ISet<int> wordIds)
    {
        var connection = coordinator.GetOccurrenceConnection();
        using (var transaction = await connection.BeginTransactionAsync())
        {
            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = @"INSERT INTO Occurrences(wordId, docId) VALUES(@wordId,@docId)";

            var paramwordId = command.CreateParameter();
            paramwordId.ParameterName = "wordId";

            command.Parameters.Add(paramwordId);

            var paramDocId = command.CreateParameter();
            paramDocId.ParameterName = "docId";
            paramDocId.Value = docId;

            command.Parameters.Add(paramDocId);

            try
            {
                foreach (var wordId in wordIds)
                {
                    paramwordId.Value = wordId;
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                throw new ApplicationException($"Error at 'InsertAllOcc'. docId; {docId}. Exception: {ex.Message}");
            }

            transaction.Commit();
        }
    }

    public async void InsertDocument(int id, string url)
    {
        var connection = coordinator.GetDocumentConnection();
        var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = "INSERT INTO Documents(id, url) VALUES(@id,@url)";

        var pName = new SqlParameter("url", url);
        insertCmd.Parameters.Add(pName);

        var pCount = new SqlParameter("id", id);
        insertCmd.Parameters.Add(pCount);

        try
        {
            await insertCmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            throw new ApplicationException($"Error at 'InsertDocument'. id; {id}, url; {url}. Exception: {ex.Message}");
        }
    }

    public async Task<Dictionary<string, int>> GetAllWords()
    {
        Dictionary<string, int> res = new Dictionary<string, int>();

        try
        {
            foreach (var connection in coordinator.GetAllWordConnections())
            {
                var selectCmd = connection.CreateCommand();
                selectCmd.CommandText = "SELECT * FROM Words";

                using (var reader = await selectCmd.ExecuteReaderAsync())
                {
                    while (reader.Read())
                    {
                        var id = reader.GetInt32(0);
                        var w = reader.GetString(1);

                        res.Add(w, id);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            throw new ApplicationException($"Error at 'GetAllWords'. Exception: {ex.Message}");
        }
        return res;
    }
}