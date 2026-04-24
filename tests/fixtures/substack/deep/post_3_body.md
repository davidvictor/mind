# The Platform Engineering Trap

Platform engineering is one of the hottest disciplines in software right now. Companies like Spotify, Netflix, Airbnb, and Uber have published extensively about their internal developer platforms — Backstage, the Delivery Console, the Deployment Gateway. These platforms promise to abstract away infrastructure complexity and let product engineers ship faster.

The trap is this: platform teams optimize for features that showcase platform sophistication rather than developer productivity. They build elaborate service meshes using Istio and Linkerd. They deploy Kubernetes operators to manage stateful workloads. They integrate Argo CD and Flux for GitOps deployments. All of this is technically impressive. Most of it is not what the average product engineer needs to ship faster.

I have spoken with platform engineering leaders at Stripe, Shopify, and Datadog. A consistent theme emerges: the most impactful platform work is embarrassingly simple. Automated environment provisioning that takes one command instead of a Confluence page. A deployment pipeline where developers can see build status without navigating three dashboards. A reliable staging environment that mirrors production closely enough that bugs found there actually predict production bugs.

The sophistication trap is seductive because complex tools signal competence. A team that deploys Istio looks like a serious infrastructure team. A team that automates a one-command environment setup looks like they are doing glue work. But the product engineers — the people the platform team exists to serve — care about the second team's output, not the first's.

Three patterns characterize platform teams that fall into the trap. First, they hire for infrastructure expertise and deprioritize developer experience research. The result is platforms that are technically correct but ergonomically painful. Terraform modules that require understanding HCL. Helm charts that require Kubernetes knowledge to debug. Second, they measure platform adoption as a success metric, which rewards visibility of platform usage over actual developer productivity. Third, they build for the sophistication of their most demanding users — the infrastructure engineers, SREs — rather than the median product engineer.

Teams that escape the trap share a common practice: they embed platform engineers with product teams for two-week rotations. HashiCorp calls this the internal customer rotation. Thoughtworks has documented similar practices under the name Team Topologies, drawing on the work of Matthew Skelton and Manuel Pais. The rotation surfaces friction points that would never appear in an internal survey.

Backstage, Spotify's open-source developer portal, is a case study in both the trap and the escape. Early versions were sophisticated and unusable by non-infrastructure engineers. The team's shift toward plugin simplicity and opinionated defaults — driven by feedback from product engineering rotations — is what made it adoptable at scale.

The lesson extends beyond platform engineering. Any internal tooling team faces the same dynamic: they are building for users who cannot easily articulate what they need, who will adapt their workflows around tooling rather than complain about it, and who vote with their feet by going outside the official platform rather than filing tickets.

The test of a good internal platform is not whether it can run a service mesh. It is whether a junior product engineer on their second week can deploy a new service without asking for help.
