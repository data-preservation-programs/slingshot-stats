# slingshot-stats

This is a lightweight lotus-client providing the data and rollups feeding https://slingshot.filecoin.io/

### Prerequisites

- Local-like access to a running filecoin node (we will pull ~500MiB from it)
- An eliglible project list like https://slingshot.filecoin.io/api/get-verified-clients

### Use
```
go run ./ rollup /tmp/rollup_results  https://slingshot.filecoin.io/api/get-verified-clients
```