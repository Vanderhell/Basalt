namespace BasaltCore.SqlServer;

internal static class SqlIdentifier
{
    internal static string Validate(string? value, string parameter)
    {
        if (string.IsNullOrWhiteSpace(value)) throw new ArgumentException("A SQL identifier of at most 128 characters is required.", parameter);
        string identifier = value!;
        if (identifier.Length > 128) throw new ArgumentException("A SQL identifier of at most 128 characters is required.", parameter);
        foreach (char c in identifier) if (!(char.IsLetterOrDigit(c) || c == '_')) throw new ArgumentException("SQL identifiers may contain only letters, digits, and underscore.", parameter);
        return identifier;
    }
    internal static string Quote(string value) => "[" + value.Replace("]", "]]") + "]";
}
