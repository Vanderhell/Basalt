using System.Runtime.InteropServices;

namespace BasaltCore.Native;

public static class StorageInterop
{
    [StructLayout(LayoutKind.Sequential)] public struct Record { public uint Type,FormatVersion; public ulong Id,Generation,Revision,TransactionId; public uint Flags; public IntPtr Payload; public uint PayloadSize; }
    [StructLayout(LayoutKind.Sequential)] public struct Worker { [MarshalAs(UnmanagedType.ByValArray,SizeConst=16)] public byte[] Bytes; }
    [StructLayout(LayoutKind.Sequential)] public struct Execution { public ulong Id,JobType,ScheduleId,WorkflowId; public uint State; public long CreatedAt,EligibleAt,StartedAt,FinishedAt; public int Priority; public uint Attempt,MaxAttempts; public ulong Revision; [MarshalAs(UnmanagedType.ByValArray,SizeConst=16)] public byte[] WorkerId; public long LeaseExpiresAt; public ulong FencingToken; }
    [StructLayout(LayoutKind.Sequential)] public struct Schedule { public ulong Id,JobType; public uint Type,Enabled; public ulong TimezoneReference,Interval; public uint IntervalMode,CatchUpMax; public long StartAt,EndAt,LastFireAt,NextFireAt; public ulong OccurrenceCount,MaxOccurrences; public uint MisfirePolicy,OverlapPolicy; public ulong Revision; }
    [StructLayout(LayoutKind.Sequential)] public struct Stats { public ulong Submitted,Started,Completed,Failed,Retried,Cancelled,Dead,Recovered; }

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate void Lifetime(IntPtr context);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult Health(IntPtr context);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult Clock(IntPtr context,out long seconds);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult Allocate(IntPtr context,out ulong id);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult RecordCreate(IntPtr context,uint type,ulong id,IntPtr payload,uint size);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult RecordGet(IntPtr context,uint type,ulong id,out Record record);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult RecordUpdate(IntPtr context,uint type,ulong id,ulong revision,IntPtr payload,uint size,IntPtr outRevision);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate void RecordFree(IntPtr context,ref Record record);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult ListIds(IntPtr context,uint type,IntPtr ids,UIntPtr capacity,out UIntPtr count);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult Enqueue(IntPtr context,ref Execution execution,uint recordType,IntPtr payload,uint size);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult EnqueueExtra(IntPtr context,ref Execution execution,uint recordType,IntPtr payload,uint size,uint extraType,IntPtr extra,uint extraSize);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult EnqueueReceipt(IntPtr context,ref Execution execution,uint recordType,IntPtr payload,uint size,ulong receiptId,IntPtr receipt,uint receiptSize);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult ReceiptGet(IntPtr context,ulong receiptId,IntPtr payload,uint capacity,out uint size);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult ExecutionGet(IntPtr context,ulong id,out Execution execution);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult Transition(IntPtr context,ulong id,ulong revision,uint state,IntPtr outRevision);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult Start(IntPtr context,ulong id,ref Worker worker,ulong fencing,long now,IntPtr outRevision);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult Claim(IntPtr context,ref Worker worker,long now,long lease,out Execution execution);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult Renew(IntPtr context,ulong id,ref Worker worker,ulong fencing,long expires);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult Complete(IntPtr context,ulong id,ref Worker worker,ulong fencing);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult Finalize(IntPtr context,ulong id,ref Worker worker,ulong fencing,uint state,int result,int error);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult Park(IntPtr context,ulong id,ref Worker worker,ulong fencing,IntPtr revision);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult Retry(IntPtr context,ulong id,ref Worker worker,ulong fencing,long eligible,IntPtr revision);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult ScheduleCreate(IntPtr context,ref Schedule schedule,uint firstType,IntPtr first,uint firstSize,uint secondType,IntPtr second,uint secondSize);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult ScheduleGet(IntPtr context,ulong id,out Schedule schedule);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult ScheduleUpdate(IntPtr context,ref Schedule schedule,ulong revision);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult ScheduleCas(IntPtr context,ulong id,ulong revision);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult ScheduleFire(IntPtr context,ulong id,ulong revision,long expectedFire,ulong executionId,long fire,long next,uint state);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult StatsGet(IntPtr context,out Stats stats);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult TxBegin(IntPtr context,out IntPtr transaction);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult TxRecord(IntPtr transaction,uint type,ulong id,IntPtr payload,uint size);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult TxExecution(IntPtr transaction,ref Execution execution);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult TxStats(IntPtr transaction,ref Stats stats,ulong revision,int exists);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate JobDbResult TxCommit(IntPtr transaction);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] public delegate void TxRollback(IntPtr transaction);

    [StructLayout(LayoutKind.Sequential)] public struct VTable
    {
        public uint AbiVersion,StructSize; public ulong Capabilities; public IntPtr ProviderName;
        public IntPtr Retain,Release,Health,UtcNow,Allocate,RecordCreate,RecordGet,RecordUpdate,RecordFree,ListIds,Enqueue,EnqueueExtra,EnqueueReceipt,ReceiptGet,ExecutionGet,Transition,Start,Claim,Renew,Complete,Finalize,Park,Retry,ScheduleCreate,ScheduleGet,ScheduleUpdate,SchedulePause,ScheduleResume,ScheduleRemove,ScheduleFire,StatsGet,TxBegin,TxRecord,TxExecution,TxStats,TxCommit,TxRollback;
    }
    private const string Library="basalt_core_shared";
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)] public static extern JobDbResult basalt_storage_create_v1(ref VTable vtable,IntPtr context,out IntPtr storage);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)] public static extern void basalt_storage_destroy(IntPtr storage);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)] public static extern JobDbResult basalt_core_create_storage_v1(IntPtr storage,uint workers,long lease,uint grace,out IntPtr core);
}
