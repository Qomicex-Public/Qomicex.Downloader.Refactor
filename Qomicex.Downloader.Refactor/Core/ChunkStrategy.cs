using Qomicex.Downloader.Refactor.Model;

namespace Qomicex.Downloader.Refactor.Core;

internal class ChunkStrategy
{
    private readonly long _chunkThreshold;
    private readonly int _minChunkSize;
    private readonly int _maxChunkSize;

    private const int MinTargetChunks = 4;
    private const int MaxTargetChunks = 16;

    public ChunkStrategy(long chunkThreshold, int minChunkSize, int maxChunkSize)
    {
        _chunkThreshold = chunkThreshold;
        _minChunkSize = minChunkSize;
        _maxChunkSize = maxChunkSize;
    }

    public IReadOnlyList<DownloadChunk> CalculateChunks(long fileSize)
    {
        if (fileSize <= _chunkThreshold)
        {
            return new[]
            {
                new DownloadChunk
                {
                    Id = "0",
                    StartOffset = 0,
                    EndOffset = fileSize - 1
                }
            };
        }

        int targetChunks = (int)Math.Clamp(fileSize / _maxChunkSize, MinTargetChunks, MaxTargetChunks);
        long chunkSize = Math.Clamp(fileSize / targetChunks, _minChunkSize, _maxChunkSize);

        int chunkCount = (int)Math.Ceiling((double)fileSize / chunkSize);
        var chunks = new DownloadChunk[chunkCount];

        for (int i = 0; i < chunkCount; i++)
        {
            long start = (long)i * chunkSize;
            long end = Math.Min(start + chunkSize - 1, fileSize - 1);

            chunks[i] = new DownloadChunk
            {
                Id = i.ToString(),
                StartOffset = start,
                EndOffset = end
            };
        }

        return chunks;
    }
}
