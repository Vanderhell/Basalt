# Contributing to Basalt.NET

Thank you for helping improve Basalt.NET. Please open an issue before a large change, especially one affecting the public API or durable formats.

## Development setup

Basalt's supported development path is Windows x64 with CMake, a C compiler, and the .NET SDK.

```powershell
cmake -S . -B build -A x64
cmake --build build --config Debug
ctest --test-dir build -C Debug --output-on-failure -R "^(BasaltDB_tests|BasaltCore_tests|BasaltCore_scheduler_policy_tests|BasaltCore_worker_test|BasaltStorage_contract_tests)$"

dotnet build BasaltCore/BasaltCore.csproj -c Debug
dotnet build BasaltCore.SqlServer/BasaltCore.SqlServer.csproj -c Debug
dotnet build consumer-smoke/Consumer.csproj -c Debug
dotnet run --project consumer-smoke/Consumer.csproj -c Debug --no-build
dotnet build consumer-wpf/ConsumerWpf.csproj -c Debug
```

SQL integration tests require a separately configured SQL Server instance and are not part of the credential-free default CI job.

## Pull requests

- Keep changes focused and preserve existing durable behavior unless the issue explicitly requires a versioned format change.
- Add or update focused tests for changed behavior.
- Update documentation when public behavior changes.
- Do not commit build outputs, credentials, connection strings, or local databases.
- Describe compatibility and persistence-format impact in the pull request template.

By contributing, you agree that your contribution is licensed under the MIT License.
