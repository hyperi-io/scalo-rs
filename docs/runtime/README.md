# Runtime

How a scalo service starts, learns where it is running, and regulates itself.

`ServiceRuntime` is the assembly point: it constructs the core pillars, detects
the environment, and wires the memory guard, scaling pressure and worker pool
together so an app does not have to know they exist.

| Doc | Covers |
| --- | --- |
| [service-runtime.md](service-runtime.md) | `ServiceRuntime`, the `ServiceApp` trait, `run_app` |
| [runtime-context.md](runtime-context.md) | K8s / Docker / bare-metal detection, pod metadata |
| [memory.md](memory.md) | `MemoryGuard`, cgroup-aware limits and backpressure |

Self-regulation is on by default. [../self-regulation.md](../self-regulation.md)
explains the three signals and how to observe or tune them.
