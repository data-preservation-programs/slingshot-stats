# OBSOLETE / DO NOT USE

Kept for historic purposes

## slingshot-stats
- a lightweight lotus-client used for the data and rollups feeding Slingshot V2

### Prerequisites

- Local-like access to a running filecoin node (we will pull ~500MiB from it)
- An eliglible project list like https://slingshot.filecoin.io/api/get-verified-clients

### Use
```
go run ./ rollup /tmp/rollup_results  https://slingshot.filecoin.io/api/get-verified-clients
```
