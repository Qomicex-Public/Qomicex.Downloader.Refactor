using Qomicex.Downloader.Refactor.Core;

namespace Qomicex.Downloader.Refactor.Configuration;

public class DownloadSessionManagerBuilder
{
    private readonly DownloadSessionManagerOptions _options = new();

    public DownloadSessionManagerBuilder WithUserAgent(string userAgent)
    {
        _options.UserAgent = userAgent;
        return this;
    }

    public DownloadSessionManagerBuilder WithMaxConcurrency(int max)
    {
        _options.MaxConcurrency = max;
        return this;
    }

    public DownloadSessionManagerBuilder WithRetry(int maxRetries, TimeSpan delay)
    {
        _options.DefaultMaxRetries = maxRetries;
        _options.DefaultRetryDelay = delay;
        return this;
    }

    public DownloadSessionManagerBuilder WithProgressInterval(int ms)
    {
        _options.ProgressReportIntervalMs = ms;
        return this;
    }

    public DownloadSessionManagerBuilder WithLogProgress(
        IProgress<Qomicex.Downloader.Refactor.Progress.DownloadLogEntry>? logProgress)
    {
        _options.LogProgress = logProgress;
        return this;
    }

    public DownloadSessionManagerBuilder WithPerUrlDownloadConfig(Dictionary<string, DownloadUrlConfig> configs)
    {
        _options.PerUrlDownloadConfigs = configs;
        return this;
    }

    public DownloadSessionManager Build()
    {
        return new DownloadSessionManager(_options);
    }
}
