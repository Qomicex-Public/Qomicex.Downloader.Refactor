namespace Qomicex.Downloader.Refactor.Progress;

public class FileProgressInfo
{
    public required string TaskId { get; init; }
    public string? FileName { get; set; }
    public double ProgressPercent { get; set; }
    public double SpeedBytesPerSec { get; set; }
    public long DownloadedBytes { get; set; }
    public long TotalBytes { get; set; }
    public FileProgressStatus Status { get; set; }
}

public enum FileProgressStatus
{
    Pending,
    Downloading,
    Completed,
    Failed
}
