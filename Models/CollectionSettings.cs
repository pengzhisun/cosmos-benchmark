using System.Collections.Generic;

public class CollectionSettings
{
    public string DataFilePath { get; set; }

    public string Id { get; set; }

    public List<string> PartitionKeys { get; set; }

    public List<string> UniqueKeys { get; set; }

    public int Throughput { get; set; }
}