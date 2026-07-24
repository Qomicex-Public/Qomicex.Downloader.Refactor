using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Threading.Channels;
using Qomicex.Downloader.Refactor.Configuration;
using Qomicex.Downloader.Refactor.Http;
using Qomicex.Downloader.Refactor.Model;
using Qomicex.Downloader.Refactor.Progress;

namespace Qomicex.Downloader.Refactor.Core;

internal enum TaskState
{
    Chunking,
    AwaitingFallback,
    Finalized
}

internal sealed class TaskTracker
{
    public required DownloadTask Task { get; init; }
    public required TaskCompletionSource<DownloadResult> Tcs { get; init; }
    public TaskState State = TaskState.Chunking;
    public List<DownloadUnit> Units = new();
    public int CompletedUnits;
    public int FailedUnits;
    public long TotalBytes;
    public long DownloadedBytes;
    public int TotalRetries;
    public string? FinalMirror;
    public Stopwatch Timer = Stopwatch.StartNew();
    public SpeedTracker FileSpeed = new();
}

internal sealed class DownloadEngine : IDisposable
{
    private readonly DownloaderOptions _options;
    private readonly HttpClient _httpClient;
    private readonly SocketsHttpHandler _httpHandler;
    private readonly HttpFileFetcher _fetcher;
    private readonly ChunkStrategy _chunkStrategy;
    private readonly MirrorSelector _mirrorSelector;
    private readonly RetryPolicy _retryPolicy;
    private readonly Watchdog _watchdog;
    private readonly DnsResolver _dnsResolver;
    private readonly SpeedCache _speedCache;
    private readonly SpeedTracker _globalSpeed;

    private readonly Channel<DownloadUnit> _workChannel;
    private readonly ConcurrentDictionary<string, TaskTracker> _trackers = new();
    private readonly object _pauseLock = new();
    private CancellationTokenSource? _cts;
    private Task? _workersTask;
    private TaskCompletionSource _pauseTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private volatile bool _paused;
    private int _activeCount;

    public bool IsPaused => _paused;

    public DownloadEngine(DownloaderOptions options)
    {
        _options = options;
        _httpHandler = new SocketsHttpHandler
        {
            PooledConnectionLifetime = options.PooledConnectionLifetime,
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
            MaxConnectionsPerServer = int.MaxValue,
            EnableMultipleHttp2Connections = true,
            AllowAutoRedirect = true,
            AutomaticDecompression = DecompressionMethods.All,
            ConnectCallback = HttpFileFetcher.CreateConnectCallback(),
        };
        _httpClient = new HttpClient(_httpHandler)
        {
            Timeout = Timeout.InfiniteTimeSpan,
        };
        _fetcher = new HttpFileFetcher(_httpClient);
        _globalSpeed = new SpeedTracker(TimeSpan.FromSeconds(10));
        _dnsResolver = new DnsResolver();
        _speedCache = new SpeedCache();
        _mirrorSelector = new MirrorSelector(_speedCache, _dnsResolver);
        _chunkStrategy = new ChunkStrategy(options.ChunkThresholdBytes, options.MinChunkSize, options.MaxChunkSize);
        _retryPolicy = new RetryPolicy(options);
        _watchdog = new Watchdog(WatchdogConfig.FromOptions(options), _globalSpeed);
        _workChannel = Channel.CreateUnbounded<DownloadUnit>(new UnboundedChannelOptions { SingleReader = false });

        _cts = new CancellationTokenSource();
        _watchdog.Start(_cts.Token);
        _workersTask = RunWorkers(_cts.Token);
    }

    private async Task RunWorkers(CancellationToken ct)
    {
        var workers = new Task[_options.MaxConcurrency];
        for (int i = 0; i < _options.MaxConcurrency; i++)
            workers[i] = WorkerLoop(ct);
        await Task.WhenAll(workers);
    }

    private async Task WorkerLoop(CancellationToken ct)
    {
        try
        {
            while (await _workChannel.Reader.WaitToReadAsync(ct))
            {
                await WaitIfPausedAsync();

                while (_workChannel.Reader.TryRead(out var unit))
                {
                    if (_paused)
                    {
                        await _workChannel.Writer.WriteAsync(unit, ct);
                        break;
                    }

                    await ProcessUnit(unit, ct);
                }
            }
        }
        catch (OperationCanceledException) { }
    }

    private async Task WaitIfPausedAsync()
    {
        TaskCompletionSource tcs;
        lock (_pauseLock)
        {
            if (!_paused) return;
            tcs = _pauseTcs;
        }
        await tcs.Task;
    }

    public async Task<DownloadResult> EnqueueAsync(DownloadTask task, CancellationToken ct = default)
    {
        var tracker = CreateTracker(task);
        var fileSize = await ProbeFileSizeAsync(task, ct);
        if (fileSize.HasValue)
            tracker.TotalBytes = fileSize.Value;

        var units = CreateUnits(task, fileSize, isFallback: false);
        tracker.Units = units;
        await EnqueueUnitsAsync(units, ct);

        using var reg = ct.Register(() => tracker.Tcs.TrySetCanceled(ct));
        return await tracker.Tcs.Task;
    }

    public async Task<IReadOnlyList<DownloadResult>> EnqueueBatchAsync(
        IReadOnlyList<DownloadTask> tasks,
        CancellationToken ct = default)
    {
        var tasksList = new List<(DownloadTask Task, TaskTracker Tracker)>(tasks.Count);
        foreach (var t in tasks)
        {
            var tracker = CreateTracker(t);
            var fileSize = await ProbeFileSizeAsync(t, ct);
            if (fileSize.HasValue)
                tracker.TotalBytes = fileSize.Value;

            var units = CreateUnits(t, fileSize, isFallback: false);
            tracker.Units = units;
            tasksList.Add((t, tracker));
            await EnqueueUnitsAsync(units, ct);
        }

        var results = new List<DownloadResult>(tasks.Count);
        foreach (var (_, tracker) in tasksList)
        {
            using var reg = ct.Register(() => tracker.Tcs.TrySetCanceled(ct));
            results.Add(await tracker.Tcs.Task);
        }
        return results;
    }

    public void Pause()
    {
        lock (_pauseLock)
        {
            if (_paused) return;
            _paused = true;
            _pauseTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        foreach (var tracker in _trackers.Values)
        {
            foreach (var unit in tracker.Units)
            {
                if (unit.Status == DownloadUnitStatus.Downloading)
                    unit.Cts.Cancel();
            }
        }
    }

    public void Resume()
    {
        lock (_pauseLock)
        {
            if (!_paused) return;
            _paused = false;
            _pauseTcs.TrySetResult();
        }
    }

    public async Task StopAsync()
    {
        Pause();

        await Task.Delay(200);

        var trackers = _trackers.Values.ToArray();
        foreach (var tracker in trackers)
        {
            foreach (var unit in tracker.Units)
                unit.Cts.Cancel();
        }

        await Task.Delay(100);

        foreach (var tracker in trackers)
        {
            if (tracker.State != TaskState.Finalized)
                FinalizeTracker(tracker, success: false);
        }

        while (_workChannel.Reader.TryRead(out _)) { }
    }

    private async Task EnqueueUnitsAsync(List<DownloadUnit> units, CancellationToken ct)
    {
        foreach (var unit in units)
            await _workChannel.Writer.WriteAsync(unit, ct);
    }

    private TaskTracker CreateTracker(DownloadTask task)
    {
        var tracker = new TaskTracker
        {
            Task = task,
            Tcs = new TaskCompletionSource<DownloadResult>(TaskCreationOptions.RunContinuationsAsynchronously),
        };
        _trackers[task.Id] = tracker;
        return tracker;
    }

    private List<DownloadUnit> CreateUnits(DownloadTask task, long? fileSize, bool isFallback)
    {
        if (isFallback)
        {
            return new List<DownloadUnit>
            {
                new()
                {
                    Id = $"{task.Id}_fallback",
                    ParentTask = task,
                    Chunk = new DownloadChunk { Id = "fallback", StartOffset = 0, EndOffset = -1 },
                    IsFallback = true,
                    TempFilePath = task.SavePath,
                    WriteToFinal = true,
                }
            };
        }

        var chunks = _chunkStrategy.CalculateChunks(fileSize ?? 0);
        var units = new List<DownloadUnit>(chunks.Count);

        for (int i = 0; i < chunks.Count; i++)
        {
            var chunk = chunks[i];
            var isSingleChunk = chunks.Count == 1;

            units.Add(new DownloadUnit
            {
                Id = $"{task.Id}_{chunk.Id}",
                ParentTask = task,
                Chunk = chunk,
                TempFilePath = isSingleChunk ? task.SavePath : $"{task.SavePath}.chunk_{chunk.Id}.tmp",
                WriteToFinal = isSingleChunk,
            });
        }

        return units;
    }

    private async Task<long?> ProbeFileSizeAsync(DownloadTask task, CancellationToken ct)
    {
        try
        {
            var mirrors = GetMirrorUrls(task);
            foreach (var mirror in mirrors)
            {
                try
                {
                    using var request = new HttpRequestMessage(HttpMethod.Head, mirror);
                    var mergedHeaders = GetMergedHeaders(task);
                    if (mergedHeaders is not null)
                    {
                        foreach (var (k, v) in mergedHeaders)
                            request.Headers.TryAddWithoutValidation(k, v);
                    }
                    using var response = await _httpClient.SendAsync(request, ct);
                    if (response.IsSuccessStatusCode && response.Content.Headers.ContentLength.HasValue)
                        return response.Content.Headers.ContentLength.Value;
                }
                catch { }
            }
        }
        catch { }

        return null;
    }

    private Dictionary<string, string>? GetMergedHeaders(DownloadTask task)
    {
        var hasGlobal = _options.DefaultUserAgent is not null || _options.DefaultHeaders is { Count: > 0 };
        if (!hasGlobal)
            return task.Headers;

        var merged = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (_options.DefaultHeaders is not null)
        {
            foreach (var (k, v) in _options.DefaultHeaders)
                merged[k] = v;
        }

        if (_options.DefaultUserAgent is not null)
            merged["User-Agent"] = _options.DefaultUserAgent;

        if (task.Headers is not null)
        {
            foreach (var (k, v) in task.Headers)
                merged[k] = v;
        }

        return merged;
    }

    private static IReadOnlyList<string> GetMirrorUrls(DownloadTask task)
    {
        var urls = new List<string> { task.Url };
        if (task.MirrorUrls is not null)
            urls.AddRange(task.MirrorUrls);
        return urls;
    }

    private async Task ProcessUnit(DownloadUnit unit, CancellationToken engineCt)
    {
        if (!_trackers.TryGetValue(unit.ParentTask.Id, out var tracker))
            return;

        var unitSpeed = new SpeedTracker();
        _watchdog.Register(unit, unitSpeed);
        Interlocked.Increment(ref _activeCount);

        try
        {
            var mirrorUrls = GetMirrorUrls(unit.ParentTask);
            var sortedMirrors = await _mirrorSelector.SelectMirrorsAsync(mirrorUrls, engineCt);
            var retryCfg = _retryPolicy.GetConfigForUrl(unit.ParentTask.Url);

            var success = false;
            var mirrorIdx = 0;

            while (unit.Retries <= retryCfg.MaxRetries)
            {
                var (mirror, bestIp) = sortedMirrors[mirrorIdx % sortedMirrors.Count];
                mirrorIdx++;

                try
                {
                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(engineCt, unit.Cts.Token);

                    var dir = Path.GetDirectoryName(unit.TempFilePath);
                    if (dir is not null)
                        Directory.CreateDirectory(dir);

                    var fileStream = new FileStream(unit.TempFilePath, FileMode.Create, FileAccess.Write,
                        FileShare.None, 81920, FileOptions.Asynchronous);
                    await using (fileStream.ConfigureAwait(false))
                    {
                        long? fetcherEndOffset = unit.Chunk.EndOffset >= 0 ? unit.Chunk.EndOffset : null;

                        await _fetcher.DownloadRangeAsync(
                            mirror,
                            unit.Chunk.StartOffset,
                            fetcherEndOffset,
                            fileStream,
                            0,
                            GetMergedHeaders(unit.ParentTask),
                            bestIp,
                            bytes =>
                            {
                                unitSpeed.RecordBytes(bytes);
                                _globalSpeed.RecordBytes(bytes);
                                _watchdog.UpdateActivity(unit.Id);
                                unit.LastActivity = DateTime.UtcNow;
                                Interlocked.Add(ref tracker.DownloadedBytes, bytes);
                            },
                            linkedCts.Token);

                        var speedKey = bestIp ?? mirror;
                        _speedCache.UpdateSpeed(speedKey, unitSpeed.CurrentSpeed);
                    }

                    tracker.FinalMirror ??= mirror;
                    success = true;
                    break;
                }
                catch (OperationCanceledException) when (unit.Cts.IsCancellationRequested)
                {
                    unit.Retries++;
                    Interlocked.Increment(ref tracker.TotalRetries);
                    TryDeleteFile(unit.TempFilePath);

                    if (unit.Retries > retryCfg.MaxRetries)
                    {
                        Log(unit.ParentTask.Id, LogLevel.Warning,
                            $"被看门狗终止，重试次数已耗尽 ({unit.Retries}/{retryCfg.MaxRetries})");
                        break;
                    }

                    Log(unit.ParentTask.Id, LogLevel.Retry,
                        $"被看门狗终止，重试 ({unit.Retries}/{retryCfg.MaxRetries})");

                    unit.Cts.Dispose();
                    unit.Cts = new CancellationTokenSource();
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    unit.Retries++;
                    Interlocked.Increment(ref tracker.TotalRetries);
                    TryDeleteFile(unit.TempFilePath);

                    if (unit.Retries > retryCfg.MaxRetries)
                    {
                        Log(unit.ParentTask.Id, LogLevel.Error,
                            $"下载失败，重试次数已耗尽 ({unit.Retries}/{retryCfg.MaxRetries}): {ex.Message}");
                        break;
                    }

                    Log(unit.ParentTask.Id, LogLevel.Retry,
                        $"下载失败，{retryCfg.RetryDelay.TotalSeconds:F1}s 后重试 ({unit.Retries}/{retryCfg.MaxRetries}): {ex.Message}");

                    unit.Cts.Dispose();
                    unit.Cts = new CancellationTokenSource();

                    await Task.Delay(retryCfg.RetryDelay, engineCt);
                }
            }

            if (success)
            {
                unit.Status = DownloadUnitStatus.Completed;
                Interlocked.Increment(ref tracker.CompletedUnits);
            }
            else
            {
                unit.Status = DownloadUnitStatus.Failed;
                Interlocked.Increment(ref tracker.FailedUnits);
            }

            ReportFileProgress(tracker, unit);

            var totalDone = tracker.CompletedUnits + tracker.FailedUnits;
            if (totalDone == tracker.Units.Count)
                await HandleTrackerDone(tracker, engineCt);
        }
        finally
        {
            _watchdog.Unregister(unit.Id);
            Interlocked.Decrement(ref _activeCount);
            ReportGlobalProgress();
        }
    }

    private async Task HandleTrackerDone(TaskTracker tracker, CancellationToken ct)
    {
        if (tracker.State == TaskState.Finalized)
            return;

        if (tracker.State == TaskState.Chunking)
        {
            if (tracker.FailedUnits == 0 && tracker.Units.Count > 1)
            {
                await MergeChunksAsync(tracker);
            }

            if (tracker.FailedUnits == 0)
            {
                FinalizeTracker(tracker, success: true);
                return;
            }

            Log(tracker.Task.Id, LogLevel.Warning, "切片下载失败，降级为单文件下载");
            tracker.State = TaskState.AwaitingFallback;
            var fallbackUnit = CreateUnits(tracker.Task, null, isFallback: true)[0];
            tracker.Units = new List<DownloadUnit> { fallbackUnit };
            tracker.CompletedUnits = 0;
            tracker.FailedUnits = 0;

            await _workChannel.Writer.WriteAsync(fallbackUnit, ct);
            return;
        }

        if (tracker.State == TaskState.AwaitingFallback)
        {
            FinalizeTracker(tracker, success: tracker.FailedUnits == 0);
        }
    }

    private void FinalizeTracker(TaskTracker tracker, bool success)
    {
        tracker.State = TaskState.Finalized;
        tracker.Timer.Stop();

        tracker.Tcs.TrySetResult(new DownloadResult
        {
            TaskId = tracker.Task.Id,
            IsSuccess = success,
            ErrorMessage = success ? null : "所有下载尝试均失败",
            Elapsed = tracker.Timer.Elapsed,
            TotalRetries = tracker.TotalRetries,
            FinalMirror = tracker.FinalMirror,
            DownloadedBytes = tracker.DownloadedBytes,
            TotalBytes = tracker.TotalBytes,
        });

        ReportGlobalProgress();
    }

    private async Task MergeChunksAsync(TaskTracker tracker)
    {
        var tempPaths = new List<string>(tracker.Units.Count);
        try
        {
            var sorted = tracker.Units.OrderBy(u => u.Chunk.StartOffset).ToList();
            var finalDir = Path.GetDirectoryName(tracker.Task.SavePath);
            if (finalDir is not null)
                Directory.CreateDirectory(finalDir);

            using var output = new FileStream(tracker.Task.SavePath, FileMode.Create, FileAccess.Write,
                FileShare.None, 81920, FileOptions.Asynchronous);

            foreach (var unit in sorted)
            {
                var tempPath = unit.TempFilePath;
                tempPaths.Add(tempPath);

                using var input = new FileStream(tempPath, FileMode.Open, FileAccess.Read,
                    FileShare.Read, 81920, FileOptions.Asynchronous | FileOptions.DeleteOnClose);
                await input.CopyToAsync(output);
            }
        }
        catch (Exception ex)
        {
            Log(tracker.Task.Id, LogLevel.Error, $"合并切片文件失败: {ex.Message}");
            throw;
        }
    }

    private static void TryDeleteFile(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch { }
    }

    private void ReportFileProgress(TaskTracker tracker, DownloadUnit unit)
    {
        var progress = _options.FileProgress;
        if (progress is null)
            return;

        var fileName = Path.GetFileName(tracker.Task.SavePath);
        var status = unit.Status switch
        {
            DownloadUnitStatus.Completed => FileProgressStatus.Completed,
            DownloadUnitStatus.Failed => FileProgressStatus.Failed,
            _ => FileProgressStatus.Downloading,
        };

        progress.Report(new FileProgressInfo
        {
            TaskId = tracker.Task.Id,
            FileName = fileName,
            ProgressPercent = tracker.TotalBytes > 0
                ? (double)tracker.DownloadedBytes / tracker.TotalBytes * 100.0
                : 0,
            SpeedBytesPerSec = tracker.FileSpeed.CurrentSpeed,
            DownloadedBytes = tracker.DownloadedBytes,
            TotalBytes = tracker.TotalBytes,
            Status = status,
        });
    }

    private void ReportGlobalProgress()
    {
        var progress = _options.GlobalProgress;
        if (progress is null)
            return;

        var all = _trackers.Values.ToArray();
        var completed = all.Count(t => t.State == TaskState.Finalized && t.FailedUnits == 0);
        var failed = all.Count(t => t.State == TaskState.Finalized && t.FailedUnits > 0);

        progress.Report(new GlobalProgressInfo
        {
            TotalTasks = _trackers.Count,
            CompletedTasks = completed,
            FailedTasks = failed,
            ActiveDownloads = _activeCount,
            GlobalSpeedBytesPerSec = _globalSpeed.CurrentSpeed,
            TotalBytes = all.Sum(t => t.TotalBytes),
            DownloadedBytes = all.Sum(t => t.DownloadedBytes),
        });
    }

    private void Log(string taskId, LogLevel level, string message, string? detail = null)
    {
        _options.LogProgress?.Report(new DownloadLogEntry
        {
            TaskId = taskId,
            Level = level,
            Message = message,
            Detail = detail,
        });
    }

    public void Dispose()
    {
        _cts?.Cancel();
        _watchdog.Stop();
        _workChannel.Writer.TryComplete();
        _httpClient.Dispose();
        _httpHandler.Dispose();
        _cts?.Dispose();
    }
}
