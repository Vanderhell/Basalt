using System.Runtime.InteropServices;
using BasaltCore.Native;

namespace BasaltCore;

public sealed class BasaltCoreException(JobDbResult result) : Exception($"BasaltCore operation failed: {result}")
{
    public JobDbResult Result { get; } = result;
}

public sealed class BasaltCoreClient : IDisposable
{
    private nint _handle;

    internal BasaltCoreClient(nint jobDbHandle, uint workers = 1, long leaseDuration = 30)
    {
        var result = NativeMethods.basalt_core_create(jobDbHandle, workers, leaseDuration, out _handle);
        if (result != JobDbResult.Ok) throw new BasaltCoreException(result);
    }

    public BasaltCoreClient(BasaltDatabase database, uint workers = 1, long leaseDuration = 30)
        : this(database?.Handle ?? throw new ArgumentNullException(nameof(database)), workers, leaseDuration) { }

    public event EventHandler<ExecutionSubmittedEventArgs>? ExecutionSubmitted;

    public void Start() => Check(NativeMethods.basalt_core_start(_handle));
    public void Stop() => Check(NativeMethods.basalt_core_stop(_handle));

    public Task<ulong> EnqueueAsync(ulong jobType, ReadOnlyMemory<byte> payload,
        uint payloadVersion = 1, uint maxAttempts = 1,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            var bytes = payload.ToArray();
            Check(NativeMethods.basalt_core_enqueue(_handle, jobType, bytes, (uint)bytes.Length,
                payloadVersion, DateTimeOffset.UtcNow.ToUnixTimeSeconds(), maxAttempts,
                out var executionId));
            ExecutionSubmitted?.Invoke(this, new ExecutionSubmittedEventArgs(executionId, jobType));
            return executionId;
        }, cancellationToken);
    }

    public void Dispose()
    {
        if (_handle == 0) return;
        NativeMethods.basalt_core_stop(_handle);
        NativeMethods.basalt_core_destroy(_handle);
        _handle = 0;
        GC.SuppressFinalize(this);
    }

    private static void Check(JobDbResult result)
    {
        if (result != JobDbResult.Ok) throw new BasaltCoreException(result);
    }
}

public sealed class ExecutionSubmittedEventArgs(ulong executionId, ulong jobType) : EventArgs
{
    public ulong ExecutionId { get; } = executionId;
    public ulong JobType { get; } = jobType;
}
