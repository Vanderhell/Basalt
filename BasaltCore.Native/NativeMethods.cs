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
    public struct ExecutionView
    {
        public ulong ExecutionId, JobDefinitionId, ScheduleId, WorkflowId;
        public uint State, PayloadVersion, Attempt, MaxAttempts;
        public long CreatedAt, EligibleAt, StartedAt, FinishedAt, LeaseExpiresAt;
        public int Priority;
        public ulong Revision, FencingToken;
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = 16)] public byte[] WorkerInstanceId;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct StatsView
    {
        public ulong SubmittedTotal, StartedTotal, CompletedTotal, FailedTotal;
        public ulong RetriedTotal, CancelledTotal, DeadTotal, RecoveredTotal;
    }

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
    public static extern JobDbResult basalt_core_create(nint db, uint workers, long leaseDuration, out nint core);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern JobDbResult basalt_core_create_ex(nint db, uint workers, long leaseDuration, uint graceMilliseconds, out nint core);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern void basalt_core_destroy(nint core);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern JobDbResult basalt_core_start(nint core);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern JobDbResult basalt_core_stop(nint core);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern JobDbResult basalt_core_register_handler(nint core, ulong type, JobHandler handler, IntPtr data);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern JobDbResult basalt_core_enqueue(nint core, ulong type,
        byte[]? payload, uint size, uint version, long now, uint maxAttempts,
        out ulong executionId);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
    public static extern JobDbResult basalt_core_enqueue_idempotent(nint core, string idempotencyKey, ulong type,
        byte[]? payload, uint size, uint version, long now, uint maxAttempts, out ulong executionId);
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

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
    public static extern JobDbResult basalt_db_backup(nint db, string targetPath);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
    public static extern JobDbResult basalt_db_restore(string sourcePath, string targetPath);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern JobDbResult basalt_execution_get(nint db, ulong executionId, out ExecutionView execution);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern JobDbResult basalt_execution_list(nint db, ulong[]? ids, UIntPtr capacity, out UIntPtr count);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern JobDbResult basalt_execution_cancel(nint db, ulong executionId, ulong expectedRevision);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern JobDbResult basalt_execution_requeue(nint db, ulong executionId, ulong expectedRevision);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern JobDbResult basalt_stats_get(nint db, out StatsView stats);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    public static extern JobDbResult basalt_db_health(nint db);
}
