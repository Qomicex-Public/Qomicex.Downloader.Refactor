using System.Collections.Concurrent;
using System.Diagnostics;
using Qomicex.Downloader.Refactor.Configuration;
using Qomicex.Downloader.Refactor.Model;
using Qomicex.Downloader.Refactor.Progress;

namespace Qomicex.Downloader.Refactor.Core;

public class DownloadSession : IDisposable
{
    private readonly DownloadSessionManagerOptions _options;
    private readonly object _lock = new();
    private readonly CancellationTokenSource _cts;

    private string _status = "queued";
    private string _stage = "";
    private double _progress;
    private string _currentFile = "";
    private int _totalFiles;
    private int _completedFiles;
    private int _failedFiles;
    private double _speed;
    private string? _error;

    public string Id { get; }
    public string Type { get; }
    public string? InstanceId { get; }

    internal DownloadSession(string id, string type, string? instanceId, DownloadSessionManagerOptions options)
    {
        Id = id;
        Type = type;
        InstanceId = instanceId;
        _options = options;
        _cts = new CancellationTokenSource();
    }

    public void SetStage(string stage, double progress, string? currentFile = null)
    {
        lock (_lock)
        {
            _stage = stage;
            _progress = progress;
            if (currentFile is not null) _currentFile = currentFile;
        }
    }

    public void ReportCompleted(string stage = "completed")
    {
        lock (_lock)
        {
            _status = "completed";
            _stage = stage;
            _progress = 100;
            _currentFile = "";
        }
    }

    public void ReportFailed(string error, string stage = "failed")
    {
        lock (_lock)
        {
            _status = "failed";
            _stage = stage;
            _error = error;
        }
    }

    public void ReportCancelled()
    {
        lock (_lock)
        {
            _status = "cancelled";
        }
    }

    public async Task<BatchResult> RunBatchAsync(
        IReadOnlyList<DownloadTask> tasks,
        CancellationToken ct = default,
        double startPercent = 0,
        double endPercent = 100,
        int? maxConcurrency = null,
        IReadOnlyDictionary<string, string>? perBatchHeaders = null)
    {
        var totalFiles = tasks.Count;
        lock (_lock)
        {
            _status = "downloading";
            _totalFiles = totalFiles;
            _completedFiles = 0;
            _failedFiles = 0;
            _progress = startPercent;
        }

        var fileProgress = new Progress<FileProgressInfo>(fp =>
        {
            if (fp.Status == FileProgressStatus.Downloading)
                lock (_lock) { _currentFile = fp.FileName ?? ""; }
        });

        var globalProgress = new Progress<GlobalProgressInfo>(gp =>
        {
            var completed = gp.CompletedTasks + gp.FailedTasks;
            lock (_lock)
            {
                _completedFiles = gp.CompletedTasks;
                _failedFiles = gp.FailedTasks;
                _speed = gp.GlobalSpeedBytesPerSec;
                if (totalFiles > 0)
                    _progress = startPercent + (endPercent - startPercent) * completed / totalFiles;
            }
        });

        var mergedHeaders = new Dictionary<string, string>();
        if (perBatchHeaders is not null)
        {
            foreach (var (k, v) in perBatchHeaders)
                mergedHeaders[k] = v;
        }

        using var batchDl = new Downloader(builder =>
        {
            builder
                .WithMaxConcurrency(maxConcurrency ?? _options.MaxConcurrency)
                .WithRetry(_options.DefaultMaxRetries, _options.DefaultRetryDelay)
                .WithProgress(globalProgress, fileProgress, _options.LogProgress)
                .WithProgressInterval(_options.ProgressReportIntervalMs);
            if (_options.UserAgent is not null)
                builder.WithUserAgent(_options.UserAgent);
            if (mergedHeaders.Count > 0)
                builder.WithDefaultHeaders(mergedHeaders);
            if (_options.PerUrlDownloadConfigs is { Count: > 0 })
                builder.WithPerUrlDownloadConfig(_options.PerUrlDownloadConfigs);
        });

        if (ct.CanBeCanceled && ct.IsCancellationRequested)
            System.Diagnostics.Trace.WriteLine($"[Session] [{Id}] RunBatchAsync: ct already cancelled before linkedCts");
        using var linkedCts = ct.CanBeCanceled && !ct.IsCancellationRequested
            ? CancellationTokenSource.CreateLinkedTokenSource(ct)
            : new CancellationTokenSource();

        var results = await batchDl.DownloadBatchAsync(tasks, linkedCts.Token);

        var failed = new List<DownloadTask>();
        foreach (var r in results)
        {
            if (!r.IsSuccess)
            {
                var task = tasks.FirstOrDefault(t => t.Id == r.TaskId);
                if (task is not null) failed.Add(task);
                System.Diagnostics.Trace.WriteLine($"[Session] [{Id}] Download failed: task={r.TaskId} mirror={r.FinalMirror} bytes={r.DownloadedBytes} err={r.ErrorMessage} retries={r.TotalRetries}");
            }
            else
            {
                System.Diagnostics.Trace.WriteLine($"[Session] [{Id}] Download OK: task={r.TaskId} bytes={r.DownloadedBytes} elapsed={r.Elapsed.TotalSeconds:F1}s");
            }
        }

        if (failed.Count > 0)
            System.Diagnostics.Trace.WriteLine($"[Session] [{Id}] Batch complete: {totalFiles - failed.Count}/{totalFiles} OK, {failed.Count} failed");
        else
            System.Diagnostics.Trace.WriteLine($"[Session] [{Id}] Batch complete: all {totalFiles} OK");

        return new BatchResult(
            TotalTasks: totalFiles,
            CompletedTasks: totalFiles - failed.Count,
            FailedTasks: failed.Count,
            FailedTaskList: failed
        );
    }

    public async Task<DownloadResult> RunSingleAsync(
        DownloadTask task,
        CancellationToken ct = default,
        double startPercent = 0,
        double endPercent = 100,
        IReadOnlyDictionary<string, string>? headers = null)
    {
        var fileProgress = new Progress<FileProgressInfo>(fp =>
        {
            lock (_lock)
            {
                if (fp.Status == FileProgressStatus.Downloading)
                    _currentFile = fp.FileName ?? "";
                _speed = fp.SpeedBytesPerSec;
                _progress = startPercent + (endPercent - startPercent) * fp.ProgressPercent / 100.0;
            }
        });

        lock (_lock) { _status = "downloading"; }

        var mergedHeaders = new Dictionary<string, string>();
        if (headers is not null)
        {
            foreach (var (k, v) in headers)
                mergedHeaders[k] = v;
        }

        using var singleDl = new Downloader(builder =>
        {
            builder
                .WithMaxConcurrency(1)
                .WithRetry(_options.DefaultMaxRetries, _options.DefaultRetryDelay)
                .WithProgress(null, fileProgress, _options.LogProgress)
                .WithProgressInterval(_options.ProgressReportIntervalMs);
            if (_options.UserAgent is not null)
                builder.WithUserAgent(_options.UserAgent);
            if (mergedHeaders.Count > 0)
                builder.WithDefaultHeaders(mergedHeaders);
            if (_options.PerUrlDownloadConfigs is { Count: > 0 })
                builder.WithPerUrlDownloadConfig(_options.PerUrlDownloadConfigs);
        });

        if (ct.CanBeCanceled && ct.IsCancellationRequested)
            System.Diagnostics.Trace.WriteLine($"[Session] [{Id}] RunSingleAsync: ct already cancelled before linkedCts");
        using var linkedCts = ct.CanBeCanceled && !ct.IsCancellationRequested
            ? CancellationTokenSource.CreateLinkedTokenSource(ct)
            : new CancellationTokenSource();

        return await singleDl.DownloadAsync(task, linkedCts.Token);
    }

    public SessionSnapshot GetSnapshot()
    {
        lock (_lock)
        {
            return new SessionSnapshot(
                SessionId: Id,
                Type: Type,
                Status: _status,
                Stage: _stage,
                Progress: _progress,
                CurrentFile: _currentFile.Length > 0 ? _currentFile : null,
                TotalFiles: _totalFiles,
                CompletedFiles: _completedFiles,
                FailedFiles: _failedFiles,
                Speed: _speed,
                Error: _error,
                IsPaused: false,
                InstanceId: InstanceId
            );
        }
    }

    public void Cancel()
    {
        lock (_lock) { _status = "cancelling"; }
        System.Diagnostics.Trace.WriteLine($"[Session] [{Id}] Cancel() called\n{Environment.StackTrace}");
        try { _cts.Cancel(); } catch (ObjectDisposedException) { }
    }

    public void Dispose()
    {
        System.Diagnostics.Trace.WriteLine($"[Session] [{Id}] Dispose() called\n{Environment.StackTrace}");
        try { _cts.Cancel(); } catch (ObjectDisposedException) { }
        try { _cts.Dispose(); } catch (ObjectDisposedException) { }
    }
}
