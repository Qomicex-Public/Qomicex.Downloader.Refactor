namespace Qomicex.Downloader.Refactor.Configuration;

public class DownloadSessionManagerOptions
{
    public string? UserAgent { get; set; }
    public int MaxConcurrency { get; set; } = 64;
    public int DefaultMaxRetries { get; set; } = 3;
    public TimeSpan DefaultRetryDelay { get; set; } = TimeSpan.FromSeconds(1);
    public int ProgressReportIntervalMs { get; set; } = 200;
    public IProgress<Qomicex.Downloader.Refactor.Progress.DownloadLogEntry>? LogProgress { get; set; }
    public Dictionary<string, DownloadUrlConfig>? PerUrlDownloadConfigs { get; set; }
}
