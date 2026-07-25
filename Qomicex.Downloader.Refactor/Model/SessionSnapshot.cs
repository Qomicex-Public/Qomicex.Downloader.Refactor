namespace Qomicex.Downloader.Refactor.Model;

public sealed record SessionSnapshot(
    string SessionId,
    string Type,
    string Status,
    string Stage,
    double Progress,
    string? CurrentFile,
    int TotalFiles,
    int CompletedFiles,
    int FailedFiles,
    double Speed,
    string? Error,
    bool IsPaused,
    string? InstanceId = null,
    string? Url = null,
    string? TargetPath = null,
    long DownloadedBytes = 0,
    long TotalBytes = 0
);
