namespace Qomicex.Downloader.Refactor.Core;

internal class MirrorSelector
{
    private readonly SpeedCache _speedCache;

    public MirrorSelector(SpeedCache speedCache)
    {
        _speedCache = speedCache;
    }

    public IReadOnlyList<string> SelectMirrors(IReadOnlyList<string> urls)
    {
        return urls
            .OrderByDescending(u => _speedCache.GetAverageSpeed(u))
            .ToList();
    }
}
