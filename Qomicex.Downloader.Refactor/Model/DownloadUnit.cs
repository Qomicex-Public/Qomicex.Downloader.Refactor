using System.Diagnostics;

namespace Qomicex.Downloader.Refactor.Model;

internal class DownloadUnit
{
    public required string Id { get; init; }
    public required DownloadTask ParentTask { get; init; }
    public required DownloadChunk Chunk { get; init; }
    public bool IsFallback { get; set; }
    public CancellationTokenSource Cts { get; set; } = new();
    public Stopwatch SpeedTimer { get; } = new();
    public DateTime LastActivity { get; set; } = DateTime.UtcNow;
    public DownloadUnitStatus Status { get; set; } = DownloadUnitStatus.Pending;
    public int Retries { get; set; }
    public string TempFilePath { get; set; } = string.Empty;
    public bool WriteToFinal { get; set; }
}

internal enum DownloadUnitStatus
{
    Pending,
    Downloading,
    Completed,
    Failed
}
