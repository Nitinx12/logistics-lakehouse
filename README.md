# logistics-lakehouse

Batch first ELT platform for logistics data. Design authority is
`ARCHITECTURE.md`. The streaming path replays historical delivery
events at an accelerated rate, so the live tab simulates traffic
instead of consuming it. Batch gold stays authoritative.
