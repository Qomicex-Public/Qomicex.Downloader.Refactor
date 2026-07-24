using System.Collections.Concurrent;

namespace Qomicex.Downloader.Refactor.Core;

internal class SpeedCache
{
    private readonly ConcurrentDictionary<string, double> _cache = new();

    private const double Alpha = 0.3;

    public void UpdateSpeed(string key, double bytesPerSec)
    {
        _cache.AddOrUpdate(
            key,
            bytesPerSec,
            (_, existing) => Alpha * bytesPerSec + (1.0 - Alpha) * existing);
    }

    public double GetAverageSpeed(string key)
    {
        return _cache.TryGetValue(key, out var speed) ? speed : 0;
    }
}
