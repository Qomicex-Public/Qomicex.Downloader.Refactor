# Qomicex Downloader Refactor

面向 **Minecraft 启动器**的高性能多文件下载器类库。支持大量小文件下载、多线程高并发、动态切片、智能镜像选择、IP 直连、自定义 UA/Headers、自动重试与看门狗保护，AOT 全量编译兼容。

---

## 特性

| 特性 | 说明 |
|:---|:---|
| **全局任务队列** | `Channel<T>` 无界队列，64 Worker 并发消费，支持运行时动态追加任务 |
| **动态切片** | HEAD 探测文件大小，>10MB 按 8–16MB 智能分片并行下载，小文件单请求直传 |
| **智能镜像选择** | DNS 解析 → 按 IP 粒度 EMA 测速 → 最优节点排序 → `ConnectCallback` 直连，精准避免慢节点 |
| **自动重试** | 分片级重试（耗尽后降级为整文件重试），支持按 URL 独立配置重试次数与间隔 |
| **看门狗** | 2s 周期扫描，30s 无数据判定卡死，速度持续低于全局均速 ×0.3 判定龟速，自动 Cancel + 重建 |
| **自定义 UA / Headers** | 全局默认 + 任务级覆盖，完整支持 `User-Agent`、`Authorization`、`Accept` 等强类型 Header |
| **进度回调** | `IProgress<GlobalProgressInfo>` / `IProgress<FileProgressInfo>` / `IProgress<DownloadLogEntry>` 三级报告 |
| **AOT 兼容** | 零反射，手写 DI 容器，`System.Threading.Channels`（BLC 原生），完整支持 Native AOT 发布 |
| **灵活配置** | Fluent API（`DownloaderBuilder`）+ Options 对象双重支持 |

---

## 项目结构

```
Qomicex.Downloader.Refactor.slnx              # 解决方案
├── Qomicex.Downloader.Refactor/              # 核心类库（net10.0）
│   ├── Model/
│   │   ├── DownloadTask.cs                   # 下载任务（URL、路径、镜像、Headers）
│   │   ├── DownloadResult.cs                 # 下载结果
│   │   ├── DownloadChunk.cs                  # 切片信息
│   │   └── DownloadUnit.cs                   # 内部调度单元
│   ├── Configuration/
│   │   ├── DownloaderOptions.cs              # 配置选项（并发、切片、重试、看门狗、UA/Headers）
│   │   ├── DownloaderBuilder.cs              # Fluent 构建器
│   │   └── RetryPolicy.cs                   # 重试策略（含按 URL 独立规则）
│   ├── Core/
│   │   ├── DownloadEngine.cs                 # 核心调度器（Channel 管线 + 64 Worker）
│   │   ├── ChunkStrategy.cs                  # 动态切片策略
│   │   ├── MirrorSelector.cs                # DNS 解析 + IP 粒度智能镜像选择
│   │   ├── DnsResolver.cs                   # DNS 解析缓存层（2 分钟 TTL）
│   │   ├── SpeedCache.cs                    # IP 粒度测速缓存（EMA 平滑）
│   │   ├── SpeedTracker.cs                  # 实时速度追踪（滑动窗口 + EMA）
│   │   ├── Watchdog.cs                      # 看门狗（卡死/龟速检测 → Cancel）
│   │   └── WatchdogConfig.cs               # 看门狗配置
│   ├── Http/
│   │   └── HttpFileFetcher.cs              # HTTP Range 请求 + IP 直连 + Header 处理
│   ├── Progress/
│   │   ├── GlobalProgressInfo.cs            # 全局进度
│   │   ├── FileProgressInfo.cs             # 单文件进度
│   │   └── DownloadLogEntry.cs             # 日志条目
│   ├── Container.cs                         # 手写 DI 容器（AOT 兼容）
│   └── Downloader.cs                        # 公开门面（ILibrary 入口）
│
└── Qomicex.Downloader.Refactor.Console/     # 控制台测试项目（AOT 兼容）
    └── Program.cs                           # 5 组完整功能测试
```

---

## 快速开始

### 安装

直接引用项目或编译为 DLL：

```bash
dotnet build -c Release
```

### 基础用法

```csharp
using Qomicex.Downloader.Refactor;
using Qomicex.Downloader.Refactor.Model;
using Qomicex.Downloader.Refactor.Progress;

var downloader = new Downloader(builder => builder
    .WithMaxConcurrency(64)
    .WithRetry(maxRetries: 3, delay: TimeSpan.FromSeconds(1))
    .WithUserAgent("QomicexLauncher/1.0"));

var tasks = new List<DownloadTask>
{
    new()
    {
        Url = "https://bmclapi2.bangbang93.com/version/1.21/client.jar",
        SavePath = @"C:\Minecraft\versions\1.21\client.jar",
        MirrorUrls = new[]
        {
            "https://download.mcbbs.net/version/1.21/client.jar",
        }
    }
};

var results = await downloader.DownloadBatchAsync(tasks);

foreach (var r in results)
{
    Console.WriteLine(r.IsSuccess
        ? $"OK {r.TaskId} — {r.DownloadedBytes / 1024}KB, {r.Elapsed.TotalSeconds:F1}s"
        : $"FAIL {r.TaskId} — {r.ErrorMessage}");
}

downloader.Dispose();
```

### 带进度的批量下载

```csharp
var globalProgress = new Progress<GlobalProgressInfo>(info =>
{
    Console.WriteLine($"进度: {info.CompletedTasks}/{info.TotalTasks} "
        + $"({info.GlobalSpeedBytesPerSec / 1024:F0} KB/s) 活跃:{info.ActiveDownloads}");
});

var fileProgress = new Progress<FileProgressInfo>(info =>
{
    Console.WriteLine($"  {info.FileName}: {info.ProgressPercent:F1}% "
        + $"({info.SpeedBytesPerSec / 1024:F0} KB/s)");
});

var logProgress = new Progress<DownloadLogEntry>(entry =>
{
    Console.WriteLine($"[{entry.Level}] {entry.TaskId}: {entry.Message}");
});

var downloader = new Downloader(builder => builder
    .WithMaxConcurrency(64)
    .WithProgress(globalProgress, fileProgress, logProgress));

var results = await downloader.DownloadBatchAsync(tasks);
```

### 自定义 UA 与 Headers

```csharp
var downloader = new Downloader(builder => builder
    .WithUserAgent("QomicexLauncher/1.0")
    .WithDefaultHeaders(new()
    {
        ["X-Client-Version"] = "2.1.0",
    }));

var task = new DownloadTask
{
    Url = "https://api.example.com/private/resource",
    SavePath = "resource.bin",
    Headers = new()
    {
        ["Authorization"] = "Bearer sk-xxxxx",
        ["X-Request-Id"] = Guid.NewGuid().ToString(),
    },
};

// 实际发送的 Headers（任务级覆盖全局同名 Header）:
// User-Agent: QomicexLauncher/1.0
// X-Client-Version: 2.1.0
// Authorization: Bearer sk-xxxxx
// X-Request-Id: a1b2c3d4...

await downloader.DownloadAsync(task);
```

### 支持的强类型 Header

以下 Header 通过各自的类型化属性设置，确保与 .NET HTTP 栈完全兼容：

| Header | 设置方式 |
|:---|:---|
| `User-Agent` | `request.Headers.UserAgent.ParseAdd(value)` |
| `Authorization` | `request.Headers.Authorization = AuthenticationHeaderValue.Parse(value)` |
| `Accept` | `request.Headers.Accept.ParseAdd(value)` |
| `Referer` | `request.Headers.Referrer = new Uri(value)` |
| 其他 Header | `request.Headers.TryAddWithoutValidation(key, value)` |

受限 Header（`Range`、`Host`、`Connection`、`Transfer-Encoding`、`Keep-Alive`）会被自动过滤。

### 动态追加任务

```csharp
var task1 = downloader.DownloadAsync(new DownloadTask
{
    Url = "https://example.com/mod.jar",
    SavePath = @"C:\Minecraft\mods\mod.jar"
});

await Task.Delay(300);

var task2 = downloader.DownloadAsync(new DownloadTask
{
    Url = "https://example.com/resource.zip",
    SavePath = @"C:\Minecraft\resources.zip"
});

await Task.WhenAll(task1, task2);
```

### 暂停 / 恢复 / 停止

```csharp
downloader.Pause();              // 暂停：不再取新任务，活跃下载 Cancel 后回队等待
Console.WriteLine(downloader.IsPaused); // true

await Task.Delay(5000);
downloader.Resume();             // 恢复：Worker 继续消费队列，被中断的切片从头重试

await downloader.StopAsync();    // 停止：取消全部活跃下载，未完成任务标记失败，清空队列
                                 // 引擎可复用，再次 DownloadAsync 即可

// Dispose 会彻底销毁引擎（不可复用）
// downloader.Dispose();
```

| 操作 | 活跃下载 | 队列 | 引擎状态 | 可复用 |
|:---|:---|:---|:---|:---|
| `Pause()` | Cancel 回队 | 阻塞不消费 | 运行中 | 是 |
| `Resume()` | 继续从头下载 | 恢复消费 | 运行中 | 是 |
| `StopAsync()` | 全部 Cancel | 清空 | 运行中 | 是 |
| `Dispose()` | 全部 Cancel | 清空 | 销毁 | 否 |

### 按 URL 配置独立重试规则

```csharp
var downloader = new Downloader(builder => builder
    .WithRetry(maxRetries: 3, delay: TimeSpan.FromSeconds(1))
    .WithPerUrlRetry(new Dictionary<string, PerUrlRetryConfig>
    {
        ["bmclapi"] = new() { MaxRetries = 5, RetryDelay = TimeSpan.FromMilliseconds(500) },
        ["mojang"] = new() { MaxRetries = 10, RetryDelay = TimeSpan.FromSeconds(2) },
    }));
```

### Options 对象方式

```csharp
var options = new DownloaderOptions
{
    MaxConcurrency = 64,
    ChunkThresholdBytes = 10 * 1024 * 1024,
    MinChunkSize = 8 * 1024 * 1024,
    MaxChunkSize = 16 * 1024 * 1024,
    DefaultMaxRetries = 3,
    DefaultRetryDelay = TimeSpan.FromSeconds(1),
    DefaultUserAgent = "QomicexLauncher/1.0",
    DefaultHeaders = new() { ["X-Custom"] = "value" },
    LowSpeedFactor = 0.3,
    StuckTimeout = TimeSpan.FromSeconds(30),
};

var downloader = new Downloader(options);
```

### 使用 DI 容器

```csharp
var container = new Container();
container.Register(new DownloaderOptions { MaxConcurrency = 32 });
container.Register<Downloader>(new Downloader(container.Resolve<DownloaderOptions>()));

var downloader = container.Resolve<Downloader>();
```

---

## 核心算法

### 调度模型

```
启动器（生产者）                        下载器（消费者）
    │                                        │
    ├─ DownloadBatchAsync ──→ Channel<DownloadUnit> ──→ Worker ×64
    ├─ DownloadAsync ──→                       │              │
    │                                          │         ┌────┴────┐
    │                                          │         │ DNS→IP? │
    │                                          │         │ Mirror? │
    │                                          │         │ Chunk?  │
    │                                          │         │ Retry?  │
    │                                          │         └────┬────┘
    │                                          │              │
    │                                          │    Watchdog ─┤ 卡死/龟速
    │                                          │              │   → Cancel
    │                                          │              │
    │                                          │  TaskTracker ─ 合并/降级
    │                                          │              │
    │  ←── IProgress<T> ───────────────────── │              │
```

### 镜像选择（IP 粒度）

```
对每个 URL:
  1. DnsResolver.Resolve(hostname) → [IP₁, IP₂, ...]
  2. 对每个 IP 查 SpeedCache → 取最优速度
  3. 按最优 IP 速度降序排列 URL

下载时:
  4. HttpRequestMessage.Options["X-Connect-Ip"] = bestIp
  5. SocketsHttpHandler.ConnectCallback → 直连该 IP
  6. 下载完成 → SpeedCache.UpdateSpeed(ip, actualSpeed)
```

### 切片策略

| 文件大小 | 行为 |
|:---|:---|
| ≤ 10MB | 1 个分片，直写目标文件 |
| > 10MB | 按 8–16MB 动态分片（目标 4–16 块），各分片写 `.chunk_N.tmp` → 合并 → 删除临时文件 |

### 重试策略

```
分片级重试（最多 N 次）
  ├─ 看门狗 Cancel   → 无延迟重试（切换镜像）
  ├─ HTTP / 网络错误  → 延迟后重试（切换镜像）
  └─ 重试耗尽        → 标记分片失败

所有分片完成
  ├─ 全部成功        → 合并 → 任务完成
  └─ 有分片失败      → 降级为单文件整文件重试（重新走完整重试流程）
       ├─ 成功       → 任务完成
       └─ 失败       → 任务失败
```

### 看门狗

```
每 2s 扫描所有活跃下载单元

对每个单元：
  now - LastActivity > 30s     → 卡死   → Cancel Cts → 触发重试
  EMA速度 < 全局均速 × 0.3
    且持续 > 10s               → 龟速   → Cancel Cts → 触发重试
  否则正常                     → 继续监控
```

### Header 合并规则

```
全局 DefaultHeaders → 全局 DefaultUserAgent → 任务级 Headers（覆盖）
```

任务级 Header 始终覆盖全局同名 Header，实现全局默认 + 任务级细粒度控制。

---

## AOT 发布

```bash
dotnet publish -c Release -p:PublishAot=true
```

项目通过以下方式确保 AOT 兼容：
- 零 `System.Reflection` 使用
- 手写 `Container` 替代 `Microsoft.Extensions.DependencyInjection`
- `System.Threading.Channels` 为 BLC（Base Class Library）原生组件
- `HttpClient` + `SocketsHttpHandler` 在 .NET 8+ 完整 AOT 支持
- `ConnectCallback` + `HttpRequestOptions` 实现 IP 直连，无需反射

---

## 测试结果

Console 项目包含 5 组完整功能测试：

| 测试 | 说明 | 状态 |
|:---|:---|:---|
| 小文件批量下载 | 3 个 GitHub 文件（1–2KB），镜像 URL + SHA256 校验 | 通过 |
| 大文件切片下载 | 100MB AppImage，64 并发切片下载 + SHA256 校验 | 通过 |
| 智能镜像切换 | gh-proxy ↔ direct 自动择优 | 通过 |
| 动态追加任务 | 运行时追加 3 个任务，并发完成 | 通过 |
| QQ 高并发下载 | 2 文件共 516MB，64 并发，15.7s，32.8 MB/s，0 重试 | 通过 |

---

## 配置项速查

| 配置项 | 类型 | 默认值 | 说明 |
|:---|:---|:---|:---|
| `MaxConcurrency` | `int` | 64 | 最大并发 Worker 数 |
| `ChunkThresholdBytes` | `long` | 10MB | 切片阈值（超过即分片） |
| `MinChunkSize` | `int` | 8MB | 单片最小尺寸 |
| `MaxChunkSize` | `int` | 16MB | 单片最大尺寸 |
| `DefaultMaxRetries` | `int` | 3 | 默认重试次数 |
| `DefaultRetryDelay` | `TimeSpan` | 1s | 默认重试间隔 |
| `PerUrlRetryConfigs` | `Dictionary` | null | 按 URL 独立重试规则 |
| `DefaultUserAgent` | `string` | null | 全局默认 User-Agent |
| `DefaultHeaders` | `Dictionary` | null | 全局默认请求头 |
| `LowSpeedFactor` | `double` | 0.3 | 龟速判定因子（×全局均速） |
| `StuckTimeout` | `TimeSpan` | 30s | 卡死超时 |
| `MinSlowDuration` | `TimeSpan` | 10s | 持续龟速最短判定时长 |
| `WatchdogInterval` | `TimeSpan` | 2s | 看门狗扫描间隔 |
| `PooledConnectionLifetime` | `TimeSpan` | 5min | HTTP 连接池生命周期 |
| `ProgressReportIntervalMs` | `int` | 200ms | 进度上报间隔 |
| `GlobalProgress` | `IProgress<T>` | null | 全局进度回调 |
| `FileProgress` | `IProgress<T>` | null | 文件进度回调 |
| `LogProgress` | `IProgress<T>` | null | 日志回调 |

---

## 依赖

- **.NET 10.0**
- **System.Threading.Channels**（BLC 内置）
- **System.Net.Http**（BLC 内置）
- 零第三方 NuGet 包

---

## 许可

本项目仅供学习与个人使用。Minecraft 相关资源的下载请遵守 Mojang EULA 及各镜像源的使用条款。
