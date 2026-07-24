using System.Net.Http.Headers;

namespace Qomicex.Downloader.Refactor.Http;

internal class HttpFileFetcher
{
    private readonly HttpClient _httpClient;

    private const int BufferSize = 80 * 1024;

    private static readonly HashSet<string> RestrictedHeaders = new(StringComparer.OrdinalIgnoreCase)
    {
        "Range", "Host"
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
        Action<long>? progressCallback,
        CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, url);

        if (endOffset.HasValue)
        {
            request.Headers.Range = new RangeHeaderValue(startOffset, endOffset.Value);
        }
        else if (startOffset > 0)
        {
            request.Headers.Range = new RangeHeaderValue(startOffset, null);
        }

        if (headers is not null)
        {
            foreach (var (key, value) in headers)
            {
                if (RestrictedHeaders.Contains(key))
                    continue;

                request.Headers.TryAddWithoutValidation(key, value);
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
}
