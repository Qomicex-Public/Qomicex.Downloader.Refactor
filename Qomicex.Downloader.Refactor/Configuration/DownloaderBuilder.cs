namespace Qomicex.Downloader.Refactor.Configuration;

public class DownloaderBuilder
{
    private readonly DownloaderOptions _options = new();

    public DownloaderBuilder WithMaxConcurrency(int max)
    {
        _options.MaxConcurrency = max;
        return this;
    }

    public DownloaderBuilder WithChunkThreshold(long bytes)
    {
        _options.ChunkThresholdBytes = bytes;
        return this;
    }

    public DownloaderBuilder WithChunkSizes(int min, int max)
    {
        _options.MinChunkSize = min;
        _options.MaxChunkSize = max;
        return this;
    }

    public DownloaderBuilder WithRetry(int maxRetries, TimeSpan delay)
    {
        _options.DefaultMaxRetries = maxRetries;
        _options.DefaultRetryDelay = delay;
        return this;
    }

    public DownloaderBuilder WithPerUrlRetry(Dictionary<string, PerUrlRetryConfig> configs)
    {
        _options.PerUrlRetryConfigs = configs;
        return this;
    }

    public DownloaderBuilder WithPerUrlDownloadConfig(Dictionary<string, DownloadUrlConfig> configs)
    {
        _options.PerUrlDownloadConfigs = configs;
        return this;
    }

    public DownloaderBuilder WithWatchdog(double lowSpeedFactor, TimeSpan stuckTimeout, TimeSpan minSlowDuration, TimeSpan interval)
    {
        _options.LowSpeedFactor = lowSpeedFactor;
        _options.StuckTimeout = stuckTimeout;
        _options.MinSlowDuration = minSlowDuration;
        _options.WatchdogInterval = interval;
        return this;
    }

    public DownloaderBuilder WithHttpPool(TimeSpan connectionLifetime)
    {
        _options.PooledConnectionLifetime = connectionLifetime;
        return this;
    }

    public DownloaderBuilder WithProgress(
        IProgress<Progress.GlobalProgressInfo>? global,
        IProgress<Progress.FileProgressInfo>? file,
        IProgress<Progress.DownloadLogEntry>? log)
    {
        _options.GlobalProgress = global;
        _options.FileProgress = file;
        _options.LogProgress = log;
        return this;
    }

    public DownloaderBuilder WithProgressInterval(int ms)
    {
        _options.ProgressReportIntervalMs = ms;
        return this;
    }

    public DownloaderBuilder WithUserAgent(string userAgent)
    {
        _options.DefaultUserAgent = userAgent;
        return this;
    }

    public DownloaderBuilder WithDefaultHeaders(Dictionary<string, string> headers)
    {
        _options.DefaultHeaders = headers;
        return this;
    }

    public DownloaderOptions Build()
    {
        return _options;
    }
}
