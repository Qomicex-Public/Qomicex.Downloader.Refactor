namespace Qomicex.Downloader.Refactor.Model;

public class DownloadChunk
{
    public required string Id { get; init; }
    public long StartOffset { get; init; }
    public long EndOffset { get; init; }
}
