using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using BasaltCore.Native;

namespace BasaltCore;

public sealed class BasaltEngine : IDisposable
{
    private readonly BasaltDatabase _database;
    private readonly IntPtr _core;
    private readonly ConcurrentDictionary<ulong, NativeMethods.JobHandler> _handlers = new();
    private int _disposed;

    public BasaltEngine(BasaltDatabase database, uint workers = 1, long leaseDuration = 30)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        BasaltDatabase.Check(NativeMethods.basalt_core_create(database.Handle, workers, leaseDuration, out _core));
    }

    public BasaltEngine(BasaltDatabase database, BasaltOptions options)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        if (options == null) throw new ArgumentNullException(nameof(options));
        BasaltDatabase.Check(NativeMethods.basalt_core_create_ex(database.Handle, options.WorkerCount, options.LeaseDurationSeconds, options.StopGraceMilliseconds, out _core));
    }

    public void RegisterRawHandler(ulong jobType, Func<ReadOnlyMemory<byte>, uint, int> handler)
    {
        RegisterNativeHandler(jobType, (payload, version, context) => handler(payload, version));
    }

    private void RegisterNativeHandler(ulong jobType, Func<ReadOnlyMemory<byte>, uint, NativeMethods.ExecutionContext, int> handler)
    {
        if (jobType == 0 || handler == null) throw new ArgumentException("A job type and handler are required.");
        NativeMethods.JobHandler callback = (payload, size, version, context, data) =>
        {
            try
            {
                int length = checked((int)size.ToUInt64()); byte[] bytes = new byte[length];
                if (length != 0) Marshal.Copy(payload, bytes, 0, length);
                NativeMethods.ExecutionContext execution = context == IntPtr.Zero ? default : Marshal.PtrToStructure<NativeMethods.ExecutionContext>(context);
                return handler(bytes, version, execution);
            }
            catch { return -1; }
        };
        BasaltDatabase.Check(NativeMethods.basalt_core_register_handler(_core, jobType, callback, IntPtr.Zero));
        _handlers[jobType] = callback;
    }

    public void RegisterHandler<T>(string stableKey, Func<T, JobContext, CancellationToken, Task> handler, IJobSerializer<T>? serializer = null)
    {
        if (handler == null) throw new ArgumentNullException(nameof(handler)); serializer ??= new DataContractJobSerializer<T>();
        ulong type = JobKey.Hash(stableKey);
        RegisterNativeHandler(type, (payload, version, nativeContext) =>
        {
            try
            {
                T value = serializer.Deserialize(payload, version);
                var context = new JobContext(nativeContext.ExecutionId, stableKey, version, nativeContext.Attempt, nativeContext.FencingToken, CancellationToken.None);
                handler(value, context, context.CancellationToken).GetAwaiter().GetResult(); return 0;
            }
            catch { return -1; }
        });
    }

    public Task<ulong> EnqueueAsync<T>(string stableKey, T value, IJobSerializer<T>? serializer = null, uint maxAttempts = 1, CancellationToken cancellationToken = default)
    {
        if (value == null) throw new ArgumentNullException(nameof(value)); serializer ??= new DataContractJobSerializer<T>();
        return EnqueueAsync(JobKey.Hash(stableKey), serializer.Serialize(value), serializer.Version, maxAttempts, cancellationToken);
    }

    public void Start() { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_core_start(_core)); }
    public void Stop() { ThrowIfDisposed(); BasaltDatabase.Check(NativeMethods.basalt_core_stop(_core)); }
    public Task StartAsync(CancellationToken cancellationToken = default) { cancellationToken.ThrowIfCancellationRequested(); return Task.Run(Start, cancellationToken); }
    public Task StopAsync(CancellationToken cancellationToken = default) { cancellationToken.ThrowIfCancellationRequested(); return Task.Run(Stop, cancellationToken); }
    public Task<ulong> EnqueueAsync(ulong jobType, ReadOnlyMemory<byte> payload, uint payloadVersion = 1, uint maxAttempts = 1, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed(); cancellationToken.ThrowIfCancellationRequested(); byte[] bytes = payload.ToArray();
        return Task.Run(() => { cancellationToken.ThrowIfCancellationRequested(); BasaltDatabase.Check(NativeMethods.basalt_core_enqueue(_core, jobType, bytes, (uint)bytes.Length, payloadVersion, DateTimeOffset.UtcNow.ToUnixTimeSeconds(), maxAttempts, out ulong id)); return id; }, cancellationToken);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        NativeMethods.basalt_core_stop(_core); NativeMethods.basalt_core_destroy(_core); _handlers.Clear(); GC.SuppressFinalize(this);
    }
    private void ThrowIfDisposed() { if (Volatile.Read(ref _disposed) != 0) throw new ObjectDisposedException(nameof(BasaltEngine)); }
}
