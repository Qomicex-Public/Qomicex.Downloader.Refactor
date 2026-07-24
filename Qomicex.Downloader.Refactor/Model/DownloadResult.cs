namespace Qomicex.Downloader.Refactor.Model;

public class DownloadResult
{
    public required string TaskId { get; init; }
    public bool IsSuccess { get; init; }
    public string? ErrorMessage { get; init; }
    public TimeSpan Elapsed { get; init; }
    public int TotalRetries { get; init; }
    public string? FinalMirror { get; init; }
    public long DownloadedBytes { get; init; }
    public long TotalBytes { get; init; }
}
