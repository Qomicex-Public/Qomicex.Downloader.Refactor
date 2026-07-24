using Qomicex.Downloader.Refactor.Configuration;
using Qomicex.Downloader.Refactor.Core;
using Qomicex.Downloader.Refactor.Model;

namespace Qomicex.Downloader.Refactor;

public class Downloader : IDisposable
{
    private readonly DownloadEngine _engine;

    public Downloader(DownloaderOptions options)
    {
        _engine = new DownloadEngine(options);
    }

    public Downloader(Action<DownloaderBuilder> configure)
    {
        var builder = new DownloaderBuilder();
        configure(builder);
        _engine = new DownloadEngine(builder.Build());
    }

    public Task<DownloadResult> DownloadAsync(DownloadTask task, CancellationToken ct = default)
    {
        return _engine.EnqueueAsync(task, ct);
    }

    public Task<IReadOnlyList<DownloadResult>> DownloadBatchAsync(
        IReadOnlyList<DownloadTask> tasks,
        CancellationToken ct = default)
    {
        return _engine.EnqueueBatchAsync(tasks, ct);
    }

    public void Dispose()
    {
        _engine.Dispose();
    }
}
