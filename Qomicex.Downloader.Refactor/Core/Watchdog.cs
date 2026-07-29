using System.Collections.Concurrent;
using Qomicex.Downloader.Refactor.Model;

namespace Qomicex.Downloader.Refactor.Core;

internal class Watchdog
{
    private sealed class WatchdogEntry
    {
        public required DownloadUnit Unit;
        public required SpeedTracker SpeedTracker;
        public DateTime LastActivity;
        public DateTime SlowStart = DateTime.MinValue;
    }

    private readonly WatchdogConfig _config;
    private readonly SpeedTracker _globalSpeedTracker;
    private readonly ConcurrentDictionary<string, WatchdogEntry> _entries = new();
    private CancellationTokenSource? _loopCts;

    public Watchdog(WatchdogConfig config, SpeedTracker globalSpeedTracker)
    {
        _config = config;
        _globalSpeedTracker = globalSpeedTracker;
    }

    public void Start(CancellationToken ct)
    {
        _loopCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _ = RunLoop(_loopCts.Token);
    }

    public void Stop()
    {
        _loopCts?.Cancel();
    }

    public void Register(DownloadUnit unit, SpeedTracker unitSpeedTracker)
    {
        _entries[unit.Id] = new WatchdogEntry
        {
            Unit = unit,
            SpeedTracker = unitSpeedTracker,
            LastActivity = DateTime.UtcNow
        };
    }

    public void UpdateActivity(string unitId)
    {
        if (_entries.TryGetValue(unitId, out var entry))
        {
            entry.LastActivity = DateTime.UtcNow;
        }
    }

    public void Unregister(string unitId)
    {
        _entries.TryRemove(unitId, out _);
    }

    private async Task RunLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await Task.Delay(_config.ScanInterval, ct);

            var snapshot = _entries.ToArray();
            foreach (var kvp in snapshot)
            {
                var entry = kvp.Value;
                var now = DateTime.UtcNow;

                if (now - entry.LastActivity > _config.StuckTimeout)
                {
                    entry.Unit.Cts.Cancel();
                    continue;
                }

                if (entry.SpeedTracker.CurrentSpeed > 0 && entry.SpeedTracker.PeakSpeed > 0
                    && entry.SpeedTracker.CurrentSpeed < entry.SpeedTracker.PeakSpeed * _config.LowSpeedFactor)
                {
                    if (entry.SlowStart == DateTime.MinValue)
                    {
                        entry.SlowStart = now;
                    }

                    if (now - entry.SlowStart > _config.MinSlowDuration)
                    {
                        entry.Unit.Cts.Cancel();
                        continue;
                    }
                }
                else
                {
                    entry.SlowStart = DateTime.MinValue;
                }
            }
        }
    }
}
