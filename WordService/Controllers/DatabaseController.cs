using Microsoft.AspNetCore.Mvc;

namespace WordService.Controllers;

[ApiController]
[Route("[controller]")]
public class DatabaseController : ControllerBase
{
    private Database database = Database.GetInstance();
    
    [HttpDelete]
    public async void Delete()
    {
        await database.DeleteDatabase();
    }

    [HttpPost]
    public async void Post()
    {
        await database.RecreateDatabase();
    }
}