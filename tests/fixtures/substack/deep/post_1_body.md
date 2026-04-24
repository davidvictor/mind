# Why Benchmarks Lie

Benchmarks are seductive. They promise an objective answer to a subjective question: how fast is this system?

The problem is that benchmarks measure what is easy to measure, not what matters. A benchmark that runs for 30 seconds on a warm cache tells you almost nothing about cold-start latency in production. Real systems accumulate state, face contention, and interact with hardware in ways no isolated test captures.

Consider what happened at a major cloud provider in 2019. Their team ran a synthetic benchmark that showed a 40% throughput improvement after a kernel patch. They shipped the change. In production, p99 latency doubled. The benchmark had not accounted for cross-NUMA memory access patterns that only emerge under mixed workload conditions.

Trust is hard to build and easy to destroy. The same principle applies to benchmarks: a single benchmark that misleads undermines confidence in all future measurements. Engineering teams that have been burned by bad benchmarks often overcorrect, distrusting even valid signals.

The measurement problem runs deeper. Heisenbugs — bugs that disappear when you observe them — have a benchmark analogue. Instrumentation changes timing. Adding a profiler perturbs the JIT. Running a benchmark in isolation removes OS scheduling noise that is an intrinsic part of production behavior.

Three strategies help. First, benchmark in production using shadow traffic. Second, establish a budget of representative workloads, not just microbenchmarks. Third, treat benchmark regressions as signals for investigation, not verdicts.

Alan Kay once said: "The best way to predict the future is to invent it." For benchmarks, the corollary is: the best way to understand your system is to run it the way it actually runs.

Jeff Dean's work on tail latency distribution showed that at scale, the 99th percentile matters more than the mean. Any benchmark that reports only mean throughput is systematically hiding the tail.

The lesson: measure more, trust less. Every benchmark is a hypothesis. Your production system is the experiment.
