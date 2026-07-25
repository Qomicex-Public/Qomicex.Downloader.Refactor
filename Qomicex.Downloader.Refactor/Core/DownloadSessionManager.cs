using System.Collections.Concurrent;
using Qomicex.Downloader.Refactor.Configuration;
using Qomicex.Downloader.Refactor.Model;

namespace Qomicex.Downloader.Refactor.Core;

public class DownloadSessionManager : IDisposable
{
    private readonly DownloadSessionManagerOptions _options;
    private readonly ConcurrentDictionary<string, DownloadSession> _sessions = new();

    internal DownloadSessionManager(DownloadSessionManagerOptions options)
    {
        _options = options;
    }

    public DownloadSession CreateSession(string sessionId, string type = "general", string? instanceId = null)
    {
        var session = new DownloadSession(sessionId, type, instanceId, _options);
        _sessions[sessionId] = session;
        return session;
    }

    public DownloadSession? GetSession(string sessionId)
    {
        _sessions.TryGetValue(sessionId, out var session);
        return session;
    }

    public IReadOnlyList<SessionSnapshot> GetActiveSnapshots()
    {
        return _sessions.Values
            .Select(s => s.GetSnapshot())
            .Where(s => s.Status is not ("completed" or "failed" or "cancelled"))
            .ToList();
    }

    public IReadOnlyList<SessionSnapshot> GetAllSnapshots()
    {
        return _sessions.Values.Select(s => s.GetSnapshot()).ToList();
    }

    public void CancelSession(string sessionId)
    {
        if (_sessions.TryRemove(sessionId, out var session))
            session.Cancel();
    }

    public void Dispose()
    {
        foreach (var (_, session) in _sessions)
            session.Dispose();
        _sessions.Clear();
    }
}
