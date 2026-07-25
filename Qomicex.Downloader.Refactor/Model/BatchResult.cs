namespace Qomicex.Downloader.Refactor.Model;

public sealed record BatchResult(
    int TotalTasks,
    int CompletedTasks,
    int FailedTasks,
    IReadOnlyList<DownloadTask> FailedTaskList
);
