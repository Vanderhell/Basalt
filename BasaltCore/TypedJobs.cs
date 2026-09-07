using System.Runtime.Serialization;
using System.Text;
using BasaltCore.Native;

namespace BasaltCore;

public interface IJobSerializer<T>
{
    uint Version { get; }
    byte[] Serialize(T value);
    T Deserialize(ReadOnlyMemory<byte> payload, uint version);
}

public sealed class DataContractJobSerializer<T> : IJobSerializer<T>
{
    public uint Version { get; }
    public DataContractJobSerializer(uint version = 1) { Version = version == 0 ? throw new ArgumentOutOfRangeException(nameof(version)) : version; }
    public byte[] Serialize(T value)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        using var stream = new MemoryStream(); new DataContractSerializer(typeof(T)).WriteObject(stream, value); return stream.ToArray();
    }
    public T Deserialize(ReadOnlyMemory<byte> payload, uint version)
    {
        if (version != Version) throw new BasaltException(JobDbResult.Version);
        using var stream = new MemoryStream(payload.ToArray()); return (T)new DataContractSerializer(typeof(T)).ReadObject(stream)!;
    }
}

public sealed class JobContext
{
    internal JobContext(ulong executionId, string jobKey, uint version, uint attempt, ulong fencingToken, CancellationToken cancellationToken)
    { ExecutionId = executionId; JobKey = jobKey; PayloadVersion = version; Attempt = attempt; FencingToken = fencingToken; CancellationToken = cancellationToken; }
    public ulong ExecutionId { get; }
    public string JobKey { get; }
    public uint PayloadVersion { get; }
    public uint Attempt { get; }
    public ulong FencingToken { get; }
    public CancellationToken CancellationToken { get; }
}

internal static class JobKey
{
    internal static ulong Hash(string value)
    {
        if (string.IsNullOrWhiteSpace(value)) throw new ArgumentException("A stable job key is required.", nameof(value));
        ulong hash = 1469598103934665603UL; foreach (byte b in Encoding.UTF8.GetBytes(value)) { hash ^= b; hash *= 1099511628211UL; } return hash == 0 ? 1 : hash;
    }
}
