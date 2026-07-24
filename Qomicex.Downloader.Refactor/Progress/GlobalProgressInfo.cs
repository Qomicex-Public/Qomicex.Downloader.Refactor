namespace Qomicex.Downloader.Refactor.Progress;

public class GlobalProgressInfo
{
    public int TotalTasks { get; set; }
    public int CompletedTasks { get; set; }
    public int FailedTasks { get; set; }
    public int ActiveDownloads { get; set; }
    public double GlobalSpeedBytesPerSec { get; set; }
    public long TotalBytes { get; set; }
    public long DownloadedBytes { get; set; }
}
