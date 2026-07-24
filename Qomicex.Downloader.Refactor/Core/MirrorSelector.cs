namespace Qomicex.Downloader.Refactor.Core;

internal class MirrorSelector
{
    private readonly SpeedCache _speedCache;
    private readonly DnsResolver _dnsResolver;

    public MirrorSelector(SpeedCache speedCache, DnsResolver dnsResolver)
    {
        _speedCache = speedCache;
        _dnsResolver = dnsResolver;
    }

    public async Task<IReadOnlyList<(string Url, string? BestIp)>> SelectMirrorsAsync(
        IReadOnlyList<string> urls, CancellationToken ct)
    {
        var scored = new List<(string Url, string? BestIp, double BestSpeed)>(urls.Count);

        foreach (var url in urls)
        {
            string? bestIp = null;
            double bestSpeed = 0;

            try
            {
                var uri = new Uri(url);
                var ips = await _dnsResolver.ResolveAsync(uri.Host, ct);

                foreach (var ip in ips)
                {
                    var speed = _speedCache.GetAverageSpeed(ip);
                    if (speed > bestSpeed)
                    {
                        bestSpeed = speed;
                        bestIp = ip;
                    }
                }
            }
            catch { }

            scored.Add((url, bestIp, bestSpeed));
        }

        return scored
            .OrderByDescending(s => s.BestSpeed)
            .Select(s => ((string Url, string? BestIp))(s.Url, s.BestIp))
            .ToList();
    }
}
