using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace Qomicex.Downloader.Refactor.Core;

internal class DnsResolver
{
    private readonly ConcurrentDictionary<string, (string[] Ips, DateTime Expiry)> _cache = new();
    private readonly TimeSpan _ttl;

    public DnsResolver(TimeSpan? ttl = null)
    {
        _ttl = ttl ?? TimeSpan.FromMinutes(2);
    }

    public async Task<string[]> ResolveAsync(string hostname, CancellationToken ct)
    {
        if (_cache.TryGetValue(hostname, out var entry) && DateTime.UtcNow < entry.Expiry)
            return entry.Ips;

        try
        {
            var addresses = await Dns.GetHostAddressesAsync(hostname, ct);
            var ips = addresses
                .Where(a => a.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6)
                .Select(a => a.ToString())
                .ToArray();

            _cache[hostname] = (ips, DateTime.UtcNow + _ttl);
            return ips;
        }
        catch
        {
            return entry.Ips ?? Array.Empty<string>();
        }
    }
}
