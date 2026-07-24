namespace Qomicex.Downloader.Refactor.Progress;

public class DownloadLogEntry
{
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    public required string TaskId { get; init; }
    public required LogLevel Level { get; init; }
    public required string Message { get; init; }
    public string? Detail { get; init; }
}

public enum LogLevel
{
    Info,
    Warning,
    Error,
    Retry
}
