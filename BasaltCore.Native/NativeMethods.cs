using System.Runtime.InteropServices;

namespace BasaltCore.Native;

public enum JobDbResult
{
    Ok = 0,
    InvalidArgument = 1,
    NotFound = 2,
    Busy = 12,
    Timeout = 13,
    StaleLease = 15
}

public static class NativeMethods
{
    private const string Library = "basalt_core_shared";

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern JobDbResult jobcore_create(nint db, uint workers, long leaseDuration, out nint core);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern void jobcore_destroy(nint core);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern JobDbResult jobcore_start(nint core);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern JobDbResult jobcore_stop(nint core);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern JobDbResult jobcore_enqueue(nint core, ulong type,
        byte[]? payload, uint size, uint version, long now, uint maxAttempts,
        out ulong executionId);
}
