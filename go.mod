module github.com/filecoin-project/slingshot-stats

go 1.15

require (
	github.com/Jeffail/gabs v1.4.0
	github.com/filecoin-project/go-address v0.0.5
	github.com/filecoin-project/go-state-types v0.1.0
	github.com/filecoin-project/lotus v1.5.0-rc2
	github.com/filecoin-project/specs-actors/v3 v3.0.3
	github.com/ipfs/go-cid v0.0.7
	github.com/ipfs/go-log/v2 v2.1.2-0.20200626104915-0016c0b4b3e4
	github.com/urfave/cli/v2 v2.3.0
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
)

replace github.com/filecoin-project/filecoin-ffi => github.com/ribasushi/go-fil-devstubs/filecoin-ffi v0.0.0-20210222205315-52cb8970aef6
