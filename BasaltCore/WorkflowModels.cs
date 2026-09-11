namespace BasaltCore;

public enum BasaltDependencyPolicy : uint { Block = 1, Cancel = 2, Continue = 3, FailWorkflow = 4 }
public sealed class BasaltWorkflowNode
{
    public ulong NodeId { get; set; } public ulong JobType { get; set; } public byte[] Payload { get; set; } = new byte[0]; public uint PayloadVersion { get; set; } = 1; public IReadOnlyList<ulong> Dependencies { get; set; } = new ulong[0];
}
public sealed class BasaltWorkflowStatus { public ulong WorkflowId { get; internal set; } public string? WorkflowKey { get; internal set; } public uint NodeCount { get; internal set; } public uint ReadyCount { get; internal set; } public uint BlockedCount { get; internal set; } public uint RunningCount { get; internal set; } public uint TerminalCount { get; internal set; } public uint FailedCount { get; internal set; } public uint CancelledCount { get; internal set; } public bool CancelRequested { get; internal set; } }
/// <summary>Read-only node in a durable workflow DAG. Dependencies use stable workflow node identities.</summary>
public sealed class BasaltWorkflowNodeInfo
{
    public ulong NodeId { get; internal set; }
    public string? Name { get; internal set; }
    public ulong ExecutionId { get; internal set; }
    public ulong JobDefinitionId { get; internal set; }
    public string? JobKey { get; internal set; }
    public ExecutionState State { get; internal set; }
    public IReadOnlyList<ulong> Dependencies { get; internal set; } = Array.Empty<ulong>();
    public string DependenciesText => Dependencies.Count == 0 ? "None" : string.Join(", ", Dependencies);
}
