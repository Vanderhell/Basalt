namespace BasaltCore;

public enum BasaltDependencyPolicy : uint { Block = 1, Cancel = 2, Continue = 3, FailWorkflow = 4 }
public sealed class BasaltWorkflowNode
{
    public ulong NodeId { get; set; } public ulong JobType { get; set; } public byte[] Payload { get; set; } = new byte[0]; public uint PayloadVersion { get; set; } = 1; public IReadOnlyList<ulong> Dependencies { get; set; } = new ulong[0];
}
