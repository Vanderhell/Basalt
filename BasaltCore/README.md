# Basalt

Basalt is a local durable background-job engine for .NET applications.

The v1 contract is at-least-once delivery: handlers must be idempotent. A
successful enqueue confirms durable submission, not handler completion. Native
ownership is internal to `BasaltDatabase` and `BasaltEngine`; applications do
not manage native handles.

This repository currently provides the managed lifecycle and enqueue surface
for the RC workstream. Native runtime assets and the complete typed scheduling
and workflow surface remain subject to the release validation gates.
