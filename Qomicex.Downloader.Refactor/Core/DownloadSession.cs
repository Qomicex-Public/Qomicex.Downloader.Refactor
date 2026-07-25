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
    private CancellationTokenSource? _cts;

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
        using var batchDl = CreateDownloader(maxConcurrency ?? _options.MaxConcurrency, perBatchHeaders);

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
            {
                lock (_lock) { _currentFile = fp.FileName ?? ""; }
            }
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

        _cts ??= new CancellationTokenSource();
        using var linkedCts = ct.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token)
            : CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);

        var results = await batchDl.DownloadBatchAsync(tasks, linkedCts.Token);

        var failed = new List<DownloadTask>();
        foreach (var r in results)
        {
            if (!r.IsSuccess)
            {
                var task = tasks.FirstOrDefault(t => t.Id == r.TaskId);
                if (task is not null) failed.Add(task);
            }
        }

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

        using var singleDl = CreateDownloader(1, headers);

        _cts ??= new CancellationTokenSource();
        using var linkedCts = ct.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token)
            : CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);

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
        lock (_lock)
        {
            _status = "cancelling";
        }
        _cts?.Cancel();
    }

    public void Dispose()
    {
        _cts?.Cancel();
        _cts?.Dispose();
    }

    private Downloader CreateDownloader(int maxConcurrency, IReadOnlyDictionary<string, string>? extraHeaders)
    {
        return new Downloader(builder =>
        {
            builder
                .WithMaxConcurrency(maxConcurrency)
                .WithRetry(_options.DefaultMaxRetries, _options.DefaultRetryDelay)
                .WithProgress(null, null, _options.LogProgress)
                .WithProgressInterval(_options.ProgressReportIntervalMs);

            if (_options.UserAgent is not null)
                builder.WithUserAgent(_options.UserAgent);

            var mergedHeaders = new Dictionary<string, string>();
            if (extraHeaders is not null)
            {
                foreach (var (k, v) in extraHeaders)
                    mergedHeaders[k] = v;
            }
            if (mergedHeaders.Count > 0)
                builder.WithDefaultHeaders(mergedHeaders);
        });
    }
}
