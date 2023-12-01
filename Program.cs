using Azure.Storage.Blobs;
using System.Text.RegularExpressions;
using Azure;
using Microsoft.Extensions.Configuration;

try
{
    var configuration = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
        .AddUserSecrets<Program>()
        .Build();

    var grainStorageConnectionString = configuration["GrainStorageConnectionString"];
    var grainStorageContainerName = configuration["GrainStorageContainerName"];
    var containerClient = new BlobContainerClient(grainStorageConnectionString, grainStorageContainerName);

    //
    // Imagine we have an Account grain type tagged with
    //
    // [PersistentState("accountStateName", "accountStorageName")]
    //
    // and a Client grain type tagged with
    //
    // [PersistentState("clientStateName", "clientStorageName")]
    //
    // Then we will see the following prefixes in the Azure Blob names:
    //
    var grainPrefixesByGrainType = new Dictionary<string, string>()
    {
        { "Account", "accountStateName-accountStorageName" },
        { "Client", "clientStateName-clientStorageName" },
    };

    //
    // In this example, we will only request Azure Blob Storage information
    // for the specific grain types we care about.
    //
    foreach (var (grainType, prefix) in grainPrefixesByGrainType)
    {
        //
        // Prepare a regex to extract the grainId from the blob name.
        // For example, account grains will have blob names of the form:
        // accountStateName-accountStorageName/accountGrainId.json
        //
        var regex = new Regex(prefix + @"\/(?<grainId>.+)\.json");
        
        //
        // Prepare to request blob descriptors from Azure in pages
        // (the Azure SDK will automatically request additional pages
        // as needed).
        //
        var resultSegment = containerClient.GetBlobsAsync(prefix: prefix)
            .AsPages(default);
        
        //
        // Iterate over the Azure records
        //
        await foreach (var blobPage in resultSegment)
        {
            foreach (var blobItem in blobPage.Values)
            {
                var match = regex.Match(blobItem.Name);
                if (match.Success)
                {
                    var grainId = match.Groups["grainId"].Value;
                    Console.WriteLine($"{grainType} {grainId}");
                }
            }
        }
    }
    
    Console.ReadLine();
}
catch (RequestFailedException e)
{
    Console.WriteLine(e.Message);
    Console.ReadLine();
    throw;
}

