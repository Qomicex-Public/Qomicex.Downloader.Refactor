namespace Qomicex.Downloader.Refactor.Configuration;

public class DownloaderOptions
{
    public int MaxConcurrency { get; set; } = 64;
    public long ChunkThresholdBytes { get; set; } = 10 * 1024 * 1024;
    public int MinChunkSize { get; set; } = 8 * 1024 * 1024;
    public int MaxChunkSize { get; set; } = 16 * 1024 * 1024;
    public int DefaultMaxRetries { get; set; } = 3;
    public TimeSpan DefaultRetryDelay { get; set; } = TimeSpan.FromSeconds(1);
    public Dictionary<string, PerUrlRetryConfig>? PerUrlRetryConfigs { get; set; }
    public double LowSpeedFactor { get; set; } = 0.3;
    public TimeSpan StuckTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan MinSlowDuration { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan WatchdogInterval { get; set; } = TimeSpan.FromSeconds(2);
    public TimeSpan PooledConnectionLifetime { get; set; } = TimeSpan.FromMinutes(5);
    public int ProgressReportIntervalMs { get; set; } = 200;
    public IProgress<Qomicex.Downloader.Refactor.Progress.GlobalProgressInfo>? GlobalProgress { get; set; }
    public IProgress<Qomicex.Downloader.Refactor.Progress.FileProgressInfo>? FileProgress { get; set; }
    public IProgress<Qomicex.Downloader.Refactor.Progress.DownloadLogEntry>? LogProgress { get; set; }
}

public class PerUrlRetryConfig
{
    public int MaxRetries { get; set; }
    public TimeSpan RetryDelay { get; set; }
}
