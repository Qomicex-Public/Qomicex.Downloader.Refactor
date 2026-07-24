namespace Qomicex.Downloader.Refactor.Model;

public class DownloadTask
{
    public string Id { get; init; } = Guid.NewGuid().ToString("N");
    public required string Url { get; init; }
    public required string SavePath { get; init; }
    public IReadOnlyList<string>? MirrorUrls { get; init; }
    public int Priority { get; init; }
    public Dictionary<string, string>? Headers { get; init; }
}
