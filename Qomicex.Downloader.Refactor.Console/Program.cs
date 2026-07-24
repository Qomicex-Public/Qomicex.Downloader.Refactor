using Qomicex.Downloader.Refactor;
using Qomicex.Downloader.Refactor.Configuration;
using Qomicex.Downloader.Refactor.Model;
using Qomicex.Downloader.Refactor.Progress;
using System.Security.Cryptography;

var downloadDir = Path.Combine(
    AppContext.BaseDirectory, "downloads");
if (Directory.Exists(downloadDir))
    Directory.Delete(downloadDir, true);
Directory.CreateDirectory(downloadDir);

System.Console.OutputEncoding = System.Text.Encoding.UTF8;
System.Console.WriteLine("=== Qomicex Downloader 功能测试 ===");
System.Console.WriteLine($"下载目录: {downloadDir}");
System.Console.WriteLine();

// GitHub 测试（网络可能不稳）
var githubFiles = new (string Name, string Url, string Mirror, string Sha256)[]
{
    ("latest.json",
     "https://github.com/Qomicex-Public/Qomicex.Tauri/releases/download/v0.1.0-beta4.0/latest.json",
     "https://gh-proxy.com/https://github.com/Qomicex-Public/Qomicex.Tauri/releases/download/v0.1.0-beta4.0/latest.json",
     "b6d3d9fee7eac46d80e2ee71d02aac56e21d810ac6145651729b6b19e1fb239a"),
    ("qomicex-launcher_v0.1.0-beta4.0_x86_64.dmg.sig",
     "https://github.com/Qomicex-Public/Qomicex.Tauri/releases/download/v0.1.0-beta4.0/qomicex-launcher_v0.1.0-beta4.0_x86_64.dmg.sig",
     "https://gh-proxy.com/https://github.com/Qomicex-Public/Qomicex.Tauri/releases/download/v0.1.0-beta4.0/qomicex-launcher_v0.1.0-beta4.0_x86_64.dmg.sig",
     "10123b42e999aa33e6f43124dd355fd6d9918693946ea63d3c52b0c47de4fdef"),
    ("qomicex-launcher_v0.1.0-beta4.0_arm64.dmg.sig",
     "https://github.com/Qomicex-Public/Qomicex.Tauri/releases/download/v0.1.0-beta4.0/qomicex-launcher_v0.1.0-beta4.0_arm64.dmg.sig",
     "https://gh-proxy.com/https://github.com/Qomicex-Public/Qomicex.Tauri/releases/download/v0.1.0-beta4.0/qomicex-launcher_v0.1.0-beta4.0_arm64.dmg.sig",
     "d92cb476a9950c94a4b02f97d6aa48aae83edc2ad05d143857e03bd62e38e66c"),
};

var qqFiles = new (string Name, string Url, string? Sha256)[]
{
    ("QQ9.7.25.29417.exe",
     "https://qqdl.gtimg.cn/qqfile/qq/PCQQ/PCQQ9.7.25/QQ9.7.25.29417.exe",
     null),
    ("QQ_9.9.32_260716_x64_01.exe",
     "https://qqdl.gtimg.cn/qqfile/QQNT/9.9.32/release/9d4083e2/QQ_9.9.32_260716_x64_01.exe",
     null),
};

await TestBatchGitHubSmallFiles();
System.Console.WriteLine();
await TestSingleGitHubLargeFile();
System.Console.WriteLine();
await TestMirrorSwitch();
System.Console.WriteLine();
await TestDynamicAdd();
System.Console.WriteLine();
await TestBatchQQFiles();
System.Console.WriteLine();
System.Console.WriteLine("=== 全部测试完成 ===");

async Task TestBatchGitHubSmallFiles()
{
    System.Console.WriteLine("--- 测试1: GitHub 小文件批量下载 (3个) ---");
    int completed = 0;
    var startTime = DateTime.UtcNow;

    var globalProgress = new Progress<GlobalProgressInfo>(info =>
    {
        var elapsed = (DateTime.UtcNow - startTime).TotalSeconds;
        System.Console.Write(
            $"\r  进度: {info.CompletedTasks}/{info.TotalTasks} | " +
            $"{info.ActiveDownloads}活跃 | " +
            $"{info.GlobalSpeedBytesPerSec / 1024:F0} KB/s | " +
            $"{elapsed:F1}s   ");
    });

    var fileProgress = new Progress<FileProgressInfo>(info =>
    {
        if (info.Status == FileProgressStatus.Completed)
        {
            var n = Interlocked.Increment(ref completed);
            System.Console.WriteLine();
            System.Console.WriteLine($"  ✓ [{n}] {info.FileName}: {info.DownloadedBytes} B");
        }
        else if (info.Status == FileProgressStatus.Failed)
        {
            System.Console.WriteLine();
            System.Console.WriteLine($"  ✗ {info.FileName}: 失败");
        }
    });

    var logProgress = new Progress<DownloadLogEntry>(entry =>
    {
        if (entry.Level >= LogLevel.Warning)
            System.Console.WriteLine($"  [{entry.Level}] {entry.Message}");
    });

    using var downloader = new Downloader(builder => builder
        .WithMaxConcurrency(64)
        .WithRetry(3, TimeSpan.FromSeconds(1))
        .WithProgress(globalProgress, fileProgress, logProgress));

    var tasks = new List<DownloadTask>();
    foreach (var (name, url, mirror, _) in githubFiles)
    {
        tasks.Add(new DownloadTask
        {
            Url = url,
            SavePath = Path.Combine(downloadDir, name),
            MirrorUrls = new[] { mirror },
        });
    }

    var results = await downloader.DownloadBatchAsync(tasks);
    System.Console.WriteLine();

    int ok = 0;
    for (int i = 0; i < tasks.Count; i++)
    {
        var r = results[i];
        var fileName = tasks[i].SavePath.Split(Path.DirectorySeparatorChar).Last();

        if (r.IsSuccess)
        {
            var expected = githubFiles.First(f => tasks[i].SavePath.EndsWith(f.Name)).Sha256;
            var sha = await ComputeSha256Async(tasks[i].SavePath);
            var match = string.Equals(sha, expected, StringComparison.OrdinalIgnoreCase);
            System.Console.WriteLine($"  {fileName}: {(match ? "✓" : "✗")} | {r.Elapsed.TotalSeconds:F1}s | 重试{r.TotalRetries}次");
            if (match) ok++;
        }
        else
        {
            System.Console.WriteLine($"  {fileName}: ✗ | {r.ErrorMessage ?? "无详情"}");
        }
    }
    System.Console.WriteLine($"  结果: {ok}/{tasks.Count} 通过");
}

async Task TestSingleGitHubLargeFile()
{
    System.Console.WriteLine("--- 测试2: GitHub 大文件下载 (≈100MB AppImage) ---");

    var savePath = Path.Combine(downloadDir, "qomicex-launcher_0.1.0-beta4.0_amd64.AppImage");
    var expectedSha = "3a7bdd211614fea6c4203905f5772d70ef461e550d34be904566cb5155661e91";

    var fileProgress = new Progress<FileProgressInfo>(info =>
    {
        if (info.Status == FileProgressStatus.Downloading && info.TotalBytes > 0)
            System.Console.Write($"\r  下载: {info.ProgressPercent:F1}% | {info.SpeedBytesPerSec / 1024 / 1024:F1} MB/s | {info.DownloadedBytes / 1024 / 1024}/{info.TotalBytes / 1024 / 1024} MB   ");
    });

    var logProgress = new Progress<DownloadLogEntry>(entry =>
    {
        if (entry.Level >= LogLevel.Retry)
            System.Console.WriteLine($"\n  [{entry.Level}] {entry.Message}");
    });

    using var downloader = new Downloader(builder => builder
        .WithMaxConcurrency(64)
        .WithRetry(5, TimeSpan.FromSeconds(2))
        .WithProgress(null, fileProgress, logProgress));

    var task = new DownloadTask
    {
        Url = "https://github.com/Qomicex-Public/Qomicex.Tauri/releases/download/v0.1.0-beta4.0/qomicex-launcher_0.1.0-beta4.0_amd64.AppImage",
        SavePath = savePath,
        MirrorUrls = new[] { "https://gh-proxy.com/https://github.com/Qomicex-Public/Qomicex.Tauri/releases/download/v0.1.0-beta4.0/qomicex-launcher_0.1.0-beta4.0_amd64.AppImage" },
    };

    using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(10));
    var sw = System.Diagnostics.Stopwatch.StartNew();

    try
    {
        var result = await downloader.DownloadAsync(task, cts.Token);
        sw.Stop();
        System.Console.WriteLine();

        if (result.IsSuccess)
        {
            var fi = new FileInfo(savePath);
            var sha = await ComputeSha256Async(savePath);
            var match = string.Equals(sha, expectedSha, StringComparison.OrdinalIgnoreCase);

            System.Console.WriteLine($"  大小: {fi.Length / 1024 / 1024} MB");
            System.Console.WriteLine($"  耗时: {sw.Elapsed.TotalSeconds:F1}s");
            System.Console.WriteLine($"  速度: {fi.Length / sw.Elapsed.TotalSeconds / 1024 / 1024:F1} MB/s");
            System.Console.WriteLine($"  重试: {result.TotalRetries}次");
            System.Console.WriteLine($"  SHA256: {(match ? "✓" : "✗")}");
        }
        else
        {
            System.Console.WriteLine($"  失败: {result.ErrorMessage}");
        }
    }
    catch (OperationCanceledException)
    {
        System.Console.WriteLine();
        var partial = File.Exists(savePath) ? new FileInfo(savePath).Length / 1024 / 1024 : 0;
        System.Console.WriteLine($"  超时(10min)，已下载 {partial} MB");
    }
}

async Task TestMirrorSwitch()
{
    System.Console.WriteLine("--- 测试3: 智能镜像切换 ---");

    var savePath = Path.Combine(downloadDir, "test_mirror.json");
    var logEntries = new List<string>();

    var logProgress = new Progress<DownloadLogEntry>(entry =>
        logEntries.Add($"[{entry.Level}] {entry.Message}"));

    var fileProgress = new Progress<FileProgressInfo>(info =>
    {
        if (info.Status == FileProgressStatus.Completed)
            System.Console.WriteLine($"  ✓ 完成: {info.DownloadedBytes} B");
    });

    using var downloader = new Downloader(builder => builder
        .WithMaxConcurrency(64)
        .WithRetry(3, TimeSpan.FromSeconds(1))
        .WithProgress(null, fileProgress, logProgress));

    var task = new DownloadTask
    {
        Url = "https://gh-proxy.com/https://github.com/Qomicex-Public/Qomicex.Tauri/releases/download/v0.1.0-beta4.0/latest.json",
        SavePath = savePath,
        MirrorUrls = new[] { "https://github.com/Qomicex-Public/Qomicex.Tauri/releases/download/v0.1.0-beta4.0/latest.json" },
    };

    var result = await downloader.DownloadAsync(task);

    if (result.IsSuccess)
    {
        var expected = "b6d3d9fee7eac46d80e2ee71d02aac56e21d810ac6145651729b6b19e1fb239a";
        var sha = await ComputeSha256Async(savePath);
        var match = string.Equals(sha, expected, StringComparison.OrdinalIgnoreCase);

        System.Console.WriteLine($"  最终镜像: {(result.FinalMirror?.Contains("gh-proxy") == true ? "gh-proxy" : "direct")}");
        System.Console.WriteLine($"  重试: {result.TotalRetries}次");
        System.Console.WriteLine($"  SHA256: {(match ? "✓" : "✗")}");
        if (logEntries.Count > 0)
        {
            System.Console.WriteLine($"  日志 ({logEntries.Count}条):");
            foreach (var l in logEntries.Take(5))
                System.Console.WriteLine($"    {l}");
        }
    }
    else
    {
        System.Console.WriteLine($"  失败: {result.ErrorMessage}");
    }
}

async Task TestDynamicAdd()
{
    System.Console.WriteLine("--- 测试4: 动态追加任务 ---");

    int completed = 0;
    var fileProgress = new Progress<FileProgressInfo>(info =>
    {
        if (info.Status == FileProgressStatus.Completed)
        {
            var n = Interlocked.Increment(ref completed);
            System.Console.WriteLine($"  ✓ [{n}] {info.FileName}: {info.DownloadedBytes} B");
        }
    });

    using var downloader = new Downloader(builder => builder
        .WithMaxConcurrency(64)
        .WithProgress(null, fileProgress, null));

    var task1 = downloader.DownloadAsync(new DownloadTask
    {
        Url = "https://github.com/Qomicex-Public/Qomicex.Tauri/releases/download/v0.1.0-beta4.0/latest.json",
        SavePath = Path.Combine(downloadDir, "dyn_latest1.json"),
        MirrorUrls = new[] { "https://gh-proxy.com/https://github.com/Qomicex-Public/Qomicex.Tauri/releases/download/v0.1.0-beta4.0/latest.json" },
    });

    await Task.Delay(300);
    System.Console.WriteLine("  追加任务2...");
    var task2 = downloader.DownloadAsync(new DownloadTask
    {
        Url = "https://gh-proxy.com/https://github.com/Qomicex-Public/Qomicex.Tauri/releases/download/v0.1.0-beta4.0/latest.json",
        SavePath = Path.Combine(downloadDir, "dyn_latest2.json"),
    });

    System.Console.WriteLine("  追加任务3...");
    var task3 = downloader.DownloadAsync(new DownloadTask
    {
        Url = "https://github.com/Qomicex-Public/Qomicex.Tauri/releases/download/v0.1.0-beta4.0/qomicex-launcher_v0.1.0-beta4.0_x86_64.dmg.sig",
        SavePath = Path.Combine(downloadDir, "dyn_x86.sig"),
        MirrorUrls = new[] { "https://gh-proxy.com/https://github.com/Qomicex-Public/Qomicex.Tauri/releases/download/v0.1.0-beta4.0/qomicex-launcher_v0.1.0-beta4.0_x86_64.dmg.sig" },
    });

    var results = await Task.WhenAll(task1, task2, task3);
    var expectedJsonSha = "b6d3d9fee7eac46d80e2ee71d02aac56e21d810ac6145651729b6b19e1fb239a";
    var expectedSigSha = "10123b42e999aa33e6f43124dd355fd6d9918693946ea63d3c52b0c47de4fdef";

    for (int i = 0; i < results.Length; i++)
    {
        var r = results[i];
        var path = i == 0 ? Path.Combine(downloadDir, "dyn_latest1.json")
                 : i == 1 ? Path.Combine(downloadDir, "dyn_latest2.json")
                 : Path.Combine(downloadDir, "dyn_x86.sig");
        var name = Path.GetFileName(path);

        if (r.IsSuccess)
        {
            var sha = await ComputeSha256Async(path);
            var expected = name.Contains("sig") ? expectedSigSha : expectedJsonSha;
            var match = string.Equals(sha, expected, StringComparison.OrdinalIgnoreCase);
            System.Console.WriteLine($"  {name}: {(match ? "✓" : "✗")} | {new FileInfo(path).Length}B | {r.Elapsed.TotalSeconds:F1}s | 重试{r.TotalRetries}次");
        }
        else
        {
            System.Console.WriteLine($"  {name}: ✗ | {r.ErrorMessage}");
        }
    }
}

async Task TestBatchQQFiles()
{
    System.Console.WriteLine("--- 测试5: QQ 大文件批量下载 (2个) ---");

    var startTime = DateTime.UtcNow;

    var globalProgress = new Progress<GlobalProgressInfo>(info =>
    {
        var elapsed = (DateTime.UtcNow - startTime).TotalSeconds;
        System.Console.Write(
            $"\r  进度: {info.CompletedTasks}/{info.TotalTasks} | " +
            $"{info.ActiveDownloads}活跃 | " +
            $"{info.GlobalSpeedBytesPerSec / 1024 / 1024:F1} MB/s | " +
            $"已下载 {info.DownloadedBytes / 1024 / 1024}/{info.TotalBytes / 1024 / 1024} MB | " +
            $"{elapsed:F1}s   ");
    });

    var fileProgress = new Progress<FileProgressInfo>(info =>
    {
        if (info.Status == FileProgressStatus.Downloading && info.TotalBytes > 0 && info.ProgressPercent % 10 < 1)
            System.Console.WriteLine($"\n  → {info.FileName}: {info.ProgressPercent:F0}% ({info.SpeedBytesPerSec / 1024 / 1024:F1} MB/s)");
        else if (info.Status == FileProgressStatus.Completed)
            System.Console.WriteLine($"\n  ✓ {info.FileName}: {info.DownloadedBytes / 1024 / 1024} MB 完成");
    });

    var logProgress = new Progress<DownloadLogEntry>(entry =>
    {
        if (entry.Level >= LogLevel.Retry)
            System.Console.WriteLine($"  [{entry.Level}] {entry.Message}");
    });

    using var downloader = new Downloader(builder => builder
        .WithMaxConcurrency(64)
        .WithRetry(3, TimeSpan.FromSeconds(1))
        .WithProgress(globalProgress, fileProgress, logProgress));

    var tasks = new List<DownloadTask>();
    foreach (var (name, url, _) in qqFiles)
    {
        tasks.Add(new DownloadTask { Url = url, SavePath = Path.Combine(downloadDir, name) });
    }

    var results = await downloader.DownloadBatchAsync(tasks);
    var elapsed = DateTime.UtcNow - startTime;
    System.Console.WriteLine();

    for (int i = 0; i < tasks.Count; i++)
    {
        var r = results[i];
        var name = tasks[i].SavePath.Split(Path.DirectorySeparatorChar).Last();

        if (r.IsSuccess)
        {
            var fi = new FileInfo(tasks[i].SavePath);
            System.Console.WriteLine($"  {name}: ✓ | {fi.Length / 1024 / 1024} MB | {r.Elapsed.TotalSeconds:F1}s | {fi.Length / r.Elapsed.TotalSeconds / 1024 / 1024:F1} MB/s | 重试{r.TotalRetries}次");
        }
        else
        {
            System.Console.WriteLine($"  {name}: ✗ | {r.ErrorMessage}");
        }
    }

    var totalMB = tasks.Select(t => File.Exists(t.SavePath) ? new FileInfo(t.SavePath).Length : 0).Sum() / 1024.0 / 1024.0;
    System.Console.WriteLine($"  总计: {totalMB:F0} MB | 耗时: {elapsed.TotalSeconds:F1}s | 均速: {totalMB / elapsed.TotalSeconds:F1} MB/s");
}

async Task<string> ComputeSha256Async(string filePath)
{
    using var stream = File.OpenRead(filePath);
    using var sha = SHA256.Create();
    var hash = await sha.ComputeHashAsync(stream);
    return Convert.ToHexStringLower(hash);
}
