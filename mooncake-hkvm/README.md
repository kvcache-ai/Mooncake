# Hierarchical Arch for Intra/Inter Data Center Deployment

We have extended Mooncake for large scale deployment in more than 10,000 cards datacenter
by Hierarchical KVcache Management(HKVM for short).



# Design 

In AntGroup's practice with Theta KVPool in agentic coding services, where a single LLM service reaches 10,000 card(accordingly 10,000 dummy clients), Mooncake Master becomes an obvious bottleneck. It will fail with these issues:

Allocators management is too heavy;
RPC processing will exhaust the CPU threads;
The recovery time for HA is not acceptable;
Even with HA mode ON, multiple Masters are available, but only one Master instance is active for all clients' requests.



# Acknowgement

* AntGroup Theta and Super-Compute Team for the development and large-scale practice
* Alicloud System and Network Team
* Approaching.AI
* Tsinghua University
* Moonshot AI
