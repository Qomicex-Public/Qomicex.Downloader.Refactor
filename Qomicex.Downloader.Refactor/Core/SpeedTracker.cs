using System.Collections.Concurrent;

namespace Qomicex.Downloader.Refactor.Core;

internal class SpeedTracker
{
    private readonly TimeSpan _window;
    private readonly object _lock = new();
    private readonly Queue<(DateTime Time, long Bytes)> _samples = new();
    private double _emaSpeed;
    private double _peakSpeed;
    private long _totalBytes;
    private DateTime _lastActivity = DateTime.UtcNow;

    private const double Alpha = 0.3;

    public SpeedTracker(TimeSpan? window = null)
    {
        _window = window ?? TimeSpan.FromSeconds(3);
    }

    public double CurrentSpeed
    {
        get { lock (_lock) return _emaSpeed; }
    }

    public double PeakSpeed
    {
        get { lock (_lock) return _peakSpeed; }
    }

    public long TotalBytes
    {
        get { lock (_lock) return _totalBytes; }
    }

    public DateTime LastActivity
    {
        get { lock (_lock) return _lastActivity; }
    }

    public void RecordBytes(long bytes)
    {
        lock (_lock)
        {
            var now = DateTime.UtcNow;
            _totalBytes += bytes;
            _lastActivity = now;

            _samples.Enqueue((now, bytes));

            var cutoff = now - _window;
            while (_samples.Count > 0 && _samples.Peek().Time < cutoff)
                _samples.Dequeue();

            long windowBytes = 0;
            foreach (var (_, b) in _samples)
                windowBytes += b;

            double instantSpeed = windowBytes / _window.TotalSeconds;
            _emaSpeed = Alpha * instantSpeed + (1.0 - Alpha) * _emaSpeed;
            if (instantSpeed > _peakSpeed)
                _peakSpeed = instantSpeed;
        }
    }
}
