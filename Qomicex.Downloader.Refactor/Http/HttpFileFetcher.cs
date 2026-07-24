using System.Net.Http.Headers;
using System.Net.Sockets;

namespace Qomicex.Downloader.Refactor.Http;

internal class HttpFileFetcher
{
    private readonly HttpClient _httpClient;

    private const int BufferSize = 80 * 1024;
    internal const string ConnectIpKey = "X-Connect-Ip";

    private static readonly HashSet<string> RestrictedHeaders = new(StringComparer.OrdinalIgnoreCase)
    {
        "Range", "Host", "Connection", "Transfer-Encoding", "Keep-Alive"
    };

    public HttpFileFetcher(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    public async Task<long> DownloadRangeAsync(
        string url,
        long startOffset,
        long? endOffset,
        Stream destination,
        long fileOffset,
        Dictionary<string, string>? headers,
        string? connectIp,
        Action<long>? progressCallback,
        CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, url);

        if (connectIp is not null)
            request.Options.Set(new HttpRequestOptionsKey<string>(ConnectIpKey), connectIp);

        if (endOffset.HasValue)
            request.Headers.Range = new RangeHeaderValue(startOffset, endOffset.Value);
        else if (startOffset > 0)
            request.Headers.Range = new RangeHeaderValue(startOffset, null);

        if (headers is not null)
        {
            foreach (var (key, value) in headers)
            {
                if (RestrictedHeaders.Contains(key))
                    continue;
                ApplyHeader(request, key, value);
            }
        }

        using var response = await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
        response.EnsureSuccessStatusCode();

        if (destination.CanSeek)
            destination.Seek(fileOffset, SeekOrigin.Begin);

        using var stream = await response.Content.ReadAsStreamAsync(ct);

        var buffer = new byte[BufferSize];
        long totalRead = 0;
        int read;

        while ((read = await stream.ReadAsync(buffer, 0, buffer.Length, ct)) > 0)
        {
            await destination.WriteAsync(buffer, 0, read, ct);
            totalRead += read;
            progressCallback?.Invoke(read);
        }

        return totalRead;
    }

    private static void ApplyHeader(HttpRequestMessage request, string key, string value)
    {
        if (string.Equals(key, "User-Agent", StringComparison.OrdinalIgnoreCase))
        {
            request.Headers.UserAgent.Clear();
            request.Headers.UserAgent.ParseAdd(value);
            return;
        }

        if (string.Equals(key, "Accept", StringComparison.OrdinalIgnoreCase))
        {
            request.Headers.Accept.Clear();
            request.Headers.Accept.ParseAdd(value);
            return;
        }

        if (string.Equals(key, "Referer", StringComparison.OrdinalIgnoreCase))
        {
            request.Headers.Referrer = new Uri(value);
            return;
        }

        request.Headers.TryAddWithoutValidation(key, value);
    }

    public static Func<SocketsHttpConnectionContext, CancellationToken, ValueTask<Stream>> CreateConnectCallback()
    {
        return async (context, ct) =>
        {
            var request = context.InitialRequestMessage;
            if (request?.Options.TryGetValue(
                new HttpRequestOptionsKey<string>(ConnectIpKey), out var ip) == true && ip is not null)
            {
                var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
                await socket.ConnectAsync(ip, context.DnsEndPoint.Port, ct);
                return new NetworkStream(socket, ownsSocket: true);
            }

            var defaultSocket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            await defaultSocket.ConnectAsync(context.DnsEndPoint, ct);
            return new NetworkStream(defaultSocket, ownsSocket: true);
        };
    }
}
