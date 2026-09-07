namespace BasaltCore;

public enum BasaltDependencyPolicy : uint { Block = 1, Cancel = 2, Continue = 3, FailWorkflow = 4 }
public sealed class BasaltWorkflowNode
{
    public ulong NodeId { get; set; } public ulong JobType { get; set; } public byte[] Payload { get; set; } = new byte[0]; public uint PayloadVersion { get; set; } = 1; public IReadOnlyList<ulong> Dependencies { get; set; } = new ulong[0];
}
public sealed class BasaltWorkflowStatus { public ulong WorkflowId { get; internal set; } public uint NodeCount { get; internal set; } public uint ReadyCount { get; internal set; } public uint BlockedCount { get; internal set; } public uint RunningCount { get; internal set; } public uint TerminalCount { get; internal set; } public uint FailedCount { get; internal set; } public uint CancelledCount { get; internal set; } public bool CancelRequested { get; internal set; } }
