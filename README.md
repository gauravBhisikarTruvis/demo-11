Account-centric multi-data prompts

“Given this account number, return the full context including linked persons, devices, IP addresses, emails, phones, and all payment relationships.”

“Expand this account by two hops and return all connected accounts, people, and digital identifiers.”

“For this account, return outgoing payments, incoming payments, and the bank details of each connected account.”

“Show all entities connected to this account and classify them by type (person, device, IP, email, phone, account).”

B. Fraud / mule exploration prompts (multi-result)

“Find all accounts that share the same IP address or device as this account and return them as a group.”

“Identify potential mule networks connected to this account using shared IPs, devices, or payment paths.”

“Return all accounts indirectly connected to this account within two hops that belong to different banks.”

“Show all accounts that have paid or received money from this account and also share at least one digital identifier.”

C. Person-centric multi-data prompts

“Given this person, return all linked accounts, devices, IPs, emails, phones, and payment relationships.”

“Expand this person node and return a complete digital footprint and financial footprint.”

“Show all accounts associated with this person and how those accounts are connected to other people.”

D. Payment-network prompts

“Return the full payment network starting from this account up to two levels, including intermediary accounts.”

“Show where money flows after this account makes a payment and who ultimately receives it.”

“Find circular payment patterns starting from this account.”

E. Bank / category comparison prompts

“Return all HSBC accounts connected to non-HSBC accounts via payments or shared identifiers.”

“Group connected accounts by bank name and return each group separately.”

“Show all cross-bank payment relationships for this account.”

F. Attribute-filtered multi-data prompts

“Return all connected accounts where fraud or mule flags are true, along with their related persons and devices.”

“Find all accounts connected to this one that were first seen before July 2024 and return their full context.”

“Return all high-risk entities connected to this account and explain why they are risky.”

G. UI-style graph expansion prompts (what your screenshots show)

“Expand this account node and return all first-hop entities, then expand only accounts from those entities.”

“Simulate a graph expansion starting from this account and return nodes grouped by hop level.”

“Return a subgraph view of this account including all HAS_PAID and HAS_ACCOUNT relationships.”

H. Agent-friendly meta prompts (very MCP-ready)

“Based on available graph capabilities, retrieve all relevant data needed to assess fraud risk for this account.”

“Discover relevant graph queries and execute those required to fully explain this account’s network.”

“Fetch all data required to explain how this account is connected to other accounts and people.”
