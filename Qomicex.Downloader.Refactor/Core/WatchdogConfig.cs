using Qomicex.Downloader.Refactor.Configuration;

namespace Qomicex.Downloader.Refactor.Core;

public class WatchdogConfig
{
    public TimeSpan ScanInterval { get; init; } = TimeSpan.FromSeconds(2);
    public TimeSpan StuckTimeout { get; init; } = TimeSpan.FromSeconds(30);
    public double LowSpeedFactor { get; init; } = 0.3;
    public TimeSpan MinSlowDuration { get; init; } = TimeSpan.FromSeconds(10);

    public static WatchdogConfig FromOptions(DownloaderOptions options) => new()
    {
        ScanInterval = options.WatchdogInterval,
        StuckTimeout = options.StuckTimeout,
        LowSpeedFactor = options.LowSpeedFactor,
        MinSlowDuration = options.MinSlowDuration
    };
}
