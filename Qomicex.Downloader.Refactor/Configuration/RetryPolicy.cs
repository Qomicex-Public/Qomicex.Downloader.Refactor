namespace Qomicex.Downloader.Refactor.Configuration;

public class RetryPolicy
{
    private readonly DownloaderOptions _options;

    internal RetryPolicy(DownloaderOptions options)
    {
        _options = options;
    }

    public PerUrlRetryConfig GetConfigForUrl(string url)
    {
        if (_options.PerUrlRetryConfigs is not null)
        {
            foreach (var kvp in _options.PerUrlRetryConfigs)
            {
                if (url.Contains(kvp.Key, StringComparison.OrdinalIgnoreCase))
                {
                    return kvp.Value;
                }
            }
        }

        return new PerUrlRetryConfig
        {
            MaxRetries = _options.DefaultMaxRetries,
            RetryDelay = _options.DefaultRetryDelay
        };
    }
}
