# Tail Latency Is a Product Problem

Most engineering teams treat latency as a pure infrastructure concern. It is not. Tail latency — the p99, p999, the slow requests — is a product experience problem that happens to live in the infrastructure layer.

When a user clicks a button and waits, they do not care whether the delay came from the database, the CDN, or a garbage collection pause. They care that the product feels slow. Tail latency is where the infrastructure problem becomes the user's problem.

The connection to benchmarks is direct. As argued in the earlier post on why benchmarks lie, mean throughput metrics systematically hide tail behavior. A service that completes 99% of requests in 10ms but 1% in 2 seconds will have an excellent mean — and will feel broken to a meaningful fraction of users.

Google showed in their 2013 research that at large fan-out — where a single user request fans out to hundreds of backend calls — even a 1% slow tail means almost every user request hits at least one slow backend. The math compounds: if each of 100 backends has a 1% chance of being slow, the probability that all of them are fast is 0.99^100 ≈ 37%.

The engineering response is usually to add hedging: send the same request to two backends, use whichever responds first. This works but doubles load. The product response is harder: reduce fan-out depth by rethinking what needs to be computed synchronously.

Engineers often resist this framing. "Latency is not my problem, it is ops." But the teams shipping the best-feeling products treat latency as a first-class product metric, owned by the feature team, not delegated to infrastructure.

The measurement discipline required here is different. You cannot rely on synthetic load tests to surface tail behavior. The tails emerge from real traffic distributions: bursty patterns, cache cold starts, thundering herd events. Only production traffic with representative user load reveals the true shape of the latency distribution.

Three things to track: p50, p95, p99. Not just mean. If your monitoring dashboard shows only the mean, you are flying blind.
