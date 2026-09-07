using System.Runtime.InteropServices;
using BasaltCore.Native;

namespace BasaltCore;

public sealed class BasaltException : Exception
{
    public BasaltException(JobDbResult result) : base("Basalt operation failed: " + result) { Result = result; }
    public JobDbResult Result { get; }
}

internal sealed class BasaltDbHandle : SafeHandle
{
    public BasaltDbHandle(IntPtr value) : base(IntPtr.Zero, true) { SetHandle(value); }
    public override bool IsInvalid { get { return handle == IntPtr.Zero || handle == new IntPtr(-1); } }
    protected override bool ReleaseHandle() { NativeMethods.basalt_db_close(handle); return true; }
}

public sealed class BasaltDatabase : IDisposable
{
    private readonly BasaltDbHandle _handle;
    private int _disposed;

    private BasaltDatabase(BasaltDbHandle handle) { _handle = handle; }
    internal IntPtr Handle { get { ThrowIfDisposed(); return _handle.DangerousGetHandle(); } }

    public static BasaltDatabase Create(string path) { return OpenCore(path, false); }
    public static BasaltDatabase Open(string path) { return OpenCore(path, true); }
    public static BasaltDatabase OpenOrCreate(string path)
    {
        if (!Directory.Exists(path)) return Create(path);
        return Open(path);
    }

    private static BasaltDatabase OpenCore(string path, bool open)
    {
        if (string.IsNullOrWhiteSpace(path)) throw new ArgumentException("A database path is required.", nameof(path));
        if (NativeMethods.basalt_abi_version() != 1) throw new BasaltException(JobDbResult.Version);
        IntPtr value; JobDbResult result = open ? NativeMethods.basalt_db_open(path, out value) : NativeMethods.basalt_db_create(path, out value);
        if (result != JobDbResult.Ok) throw new BasaltException(result);
        return new BasaltDatabase(new BasaltDbHandle(value));
    }

    public static void Verify(string path) { Check(NativeMethods.basalt_db_verify(path)); }
    public void Dispose() { if (Interlocked.Exchange(ref _disposed, 1) == 0) _handle.Dispose(); GC.SuppressFinalize(this); }
    private void ThrowIfDisposed() { if (Volatile.Read(ref _disposed) != 0) throw new ObjectDisposedException(nameof(BasaltDatabase)); }
    internal static void Check(JobDbResult result) { if (result != JobDbResult.Ok) throw new BasaltException(result); }
}
