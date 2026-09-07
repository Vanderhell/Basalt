using System.Runtime.InteropServices;

namespace BasaltCore.Native;

public enum JobDbResult
{
    Ok = 0,
    InvalidArgument = 1,
    NotFound = 2,
    AlreadyExists = 3,
    Io = 4,
    Corrupt = 5,
    Version = 6,
    Limit = 7,
    Internal = 8,
    InvalidTransaction = 9,
    Conflict = 10,
    InvalidState = 11,
    Busy = 12,
    Timeout = 13,
    StaleLease = 14
}

public static class NativeMethods
{
    [StructLayout(LayoutKind.Sequential)]
    public struct ExecutionContext
    {
        public ulong ExecutionId;
        public ulong JobType;
        public ulong FencingToken;
        public uint PayloadVersion;
        public uint Attempt;
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = 16)] public byte[] Worker;
        public IntPtr CancellationRequested;
    }

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate int JobHandler(IntPtr payload, UIntPtr size, uint version, IntPtr context, IntPtr data);

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
    public static extern JobDbResult jobcore_register_handler(nint core, ulong type, JobHandler handler, IntPtr data);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern JobDbResult jobcore_enqueue(nint core, ulong type,
        byte[]? payload, uint size, uint version, long now, uint maxAttempts,
        out ulong executionId);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern uint basalt_abi_version();

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
    public static extern JobDbResult basalt_db_create(string path, out IntPtr db);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
    public static extern JobDbResult basalt_db_open(string path, out IntPtr db);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern void basalt_db_close(IntPtr db);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
    public static extern JobDbResult basalt_db_verify(string path);
}
