namespace BasaltCore;

public sealed class BasaltOptions
{
    public uint WorkerCount { get; set; } = 1;
    public long LeaseDurationSeconds { get; set; } = 30;
    public uint StopGraceMilliseconds { get; set; } = 1000;
}
