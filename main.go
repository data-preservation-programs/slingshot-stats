package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/Jeffail/gabs"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

// Requested by @jbenet
// How many epochs back to look at for dealstats
var defaultEpochLookback = abi.ChainEpoch(10)

// perl -E 'say scalar gmtime ( XXX * 30 + 1598306400 )'
//
// 166560:  Wed Oct 21 18:00:00 2020
// 307680:  Wed Dec  9 18:00:00 2020
// 448800:  Wed Jan 27 18:00:00 2021
// 569760:  Wed Mar 10 18:00:00 2021
// 756960:  Fri May 14 18:00:00 2021
// 912480:  Wed Jul  7 18:00:00 2021
// 1099680: Fri Sep 10 18:00:00 2021
// 1275360: Wed Nov 10 18:00:00 2021
// 1623840: Fri Mar 11 18:00:00 2021
var currentPhaseStart = abi.ChainEpoch(1623840)

// 1381920: Fri Dec 17 18:00:00 2021
var recoveryStart = abi.ChainEpoch(1381920)

//
// contents of basic_stats.json
type competitionTotalOutput struct {
	Epoch    int64            `json:"epoch"`
	Endpoint string           `json:"endpoint"`
	Payload  competitionTotal `json:"payload"`
}
type competitionTotal struct {
	UniqueCids        int   `json:"total_unique_cids"`
	UniqueProviders   int   `json:"total_unique_providers"`
	UniqueProjects    int   `json:"total_unique_projects"`
	UniqueClients     int   `json:"total_unique_clients"`
	TotalDeals        int   `json:"total_num_deals"`
	TotalBytes        int64 `json:"total_stored_data_size"`
	FilplusTotalDeals int   `json:"filplus_total_num_deals"`
	FilplusTotalBytes int64 `json:"filplus_total_stored_data_size"`

	seenProject  map[string]bool
	seenClient   map[address.Address]bool
	seenProvider map[address.Address]bool
	seenPieceCid map[cid.Cid]bool
}

//
// contents of client_stats.json
type projectAggregateStatsOutput struct {
	Epoch    int64                             `json:"epoch"`
	Endpoint string                            `json:"endpoint"`
	Payload  map[string]*projectAggregateStats `json:"payload"`
}
type projectAggregateStats struct {
	ProjectID           string                           `json:"project_id"`
	DataSizeMaxProvider int64                            `json:"max_data_size_stored_with_single_provider"`
	HighestCidDealCount int                              `json:"max_same_cid_deals"`
	DataSize            int64                            `json:"total_data_size"`
	NumCids             int                              `json:"total_num_cids"`
	NumDeals            int                              `json:"total_num_deals"`
	NumProviders        int                              `json:"total_num_providers"`
	ClientStats         map[string]*clientAggregateStats `json:"clients"`

	dataPerProvider          map[address.Address]int64
	timesSeenPieceCid        map[cid.Cid]int
	timesSeenPieceCidAllTime map[cid.Cid]int
}
type clientAggregateStats struct {
	Client       string `json:"client"`
	DataSize     int64  `json:"total_data_size"`
	NumCids      int    `json:"total_num_cids"`
	NumDeals     int    `json:"total_num_deals"`
	NumProviders int    `json:"total_num_providers"`

	providers map[address.Address]bool
	cids      map[cid.Cid]bool
}

//
// contents of deals_list_{{projid}}.json
type dealListOutput struct {
	Epoch    int64             `json:"epoch"`
	Endpoint string            `json:"endpoint"`
	Payload  []*individualDeal `json:"payload"`
}
type individualDeal struct {
	ProjectID      string `json:"project_id"`
	Client         string `json:"client"`
	DealID         string `json:"deal_id"`
	DealStartEpoch int64  `json:"deal_start_epoch"`
	MinerID        string `json:"miner_id"`
	PayloadCID     string `json:"payload_cid"`
	PaddedSize     int64  `json:"data_size"`
}

//
// contents of recovery_deallist.json
type recoveryListOutput struct {
	Epoch    int64           `json:"epoch"`
	Endpoint string          `json:"endpoint"`
	Payload  []recoveredDeal `json:"payload"`
}
type recoveredDeal struct {
	DealID          string `json:"deal_id"`
	ClientAddress   string `json:"client_address"`
	MinerID         string `json:"miner_id"`
	PieceCID        string `json:"piece_cid"`
	Label           string `json:"label"`
	PayloadCIDb32   string `json:"payload_cid"`
	PaddedPieceSize uint64 `json:"padded_piece_size"`
	DataSize        uint64 `json:"data_size"`
	DealStartEpoch  int64  `json:"deal_start_epoch"`
	DealEndEpoch    int64  `json:"deal_end_epoch"`
	RecoveryType    int8   `json:"recovery"` // 1: restore, 2: repair
}

var log = logging.Logger("slingshot-stats")
var resolvedWallets = map[address.Address]address.Address{}

func main() {
	logging.SetLogLevel("*", "INFO") //nolint:errcheck

	app := &cli.App{
		Name:  "slingshot-stats",
		Usage: "Misc tooling for https://slingshot.filecoin.io/",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "repo",
				EnvVars: []string{"LOTUS_PATH"},
				Value:   "~/.lotus", // TODO: Consider XDG_DATA_HOME
			},
		},
		Commands: []*cli.Command{rollup},
	}

	if err := app.Run(os.Args); err != nil {
		log.Error(err)
		os.Exit(1)
		return
	}
}

var rollup = &cli.Command{
	Usage:     "Translating current lotus state into format and rollups as understood by https://slingshot.filecoin.io/",
	Name:      "rollup",
	ArgsUsage: "  <non-existent output directory name>  <eligible project list>",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:        "tipset",
			Usage:       "Current tipset either as comma separated array of cids, or @height",
			DefaultText: fmt.Sprintf("%d epochs behind current", defaultEpochLookback),
		},
		&cli.Int64Flag{
			Name:  "phasestart-epoch",
			Value: int64(currentPhaseStart),
		},
	},
	Action: func(cctx *cli.Context) error {

		if cctx.Args().Len() != 3 || cctx.Args().Get(0) == "" || cctx.Args().Get(1) == "" || cctx.Args().Get(2) == "" {
			return errors.New("must supply 3 arguments: a nonexistent target directory to write results to, a source of currently active projects and a source of recovery list clients")
		}
		ctx := lcli.ReqContext(cctx)

		if cctx.Int64("phasestart-epoch") > 0 {
			currentPhaseStart = abi.ChainEpoch(cctx.Int64("phasestart-epoch"))
		}

		outDirName := cctx.Args().Get(0)
		if _, err := os.Stat(outDirName); err == nil {
			return xerrors.Errorf("unable to proceed: supplied stat target '%s' already exists", outDirName)
		}

		if err := os.MkdirAll(outDirName, 0755); err != nil {
			return xerrors.Errorf("creation of destination '%s' failed: %s", outDirName, err)
		}

		knownAddrMap, err := getAndParseProjectList(ctx, outDirName, cctx.Args().Get(1))
		if err != nil {
			return xerrors.Errorf("determining registered project failed: %s", err)
		}

		knownRestoreClients, err := getAndParseRestore(ctx, outDirName, cctx.Args().Get(2))
		if err != nil {
			return xerrors.Errorf("determining restore clients failed: %s", err)
		}

		api, apiCloser, err := lcli.GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer apiCloser()

		outClientStatsFd, err := os.Create(outDirName + "/client_stats.json")
		if err != nil {
			return err
		}
		defer outClientStatsFd.Close() //nolint:errcheck

		outBasicStatsFd, err := os.Create(outDirName + "/basic_stats.json")
		if err != nil {
			return err
		}
		defer outBasicStatsFd.Close() //nolint:errcheck

		outRecoveryListFd, err := os.Create(outDirName + "/recovery_deallist.json")
		if err != nil {
			return err
		}
		defer outRecoveryListFd.Close() //nolint:errcheck

		var ts *types.TipSet
		if cctx.String("tipset") == "" {
			ts, err = api.ChainHead(ctx)
			if err != nil {
				return err
			}
			ts, err = api.ChainGetTipSetByHeight(ctx, ts.Height()-defaultEpochLookback, ts.Key())
			if err != nil {
				return err
			}
		} else {
			ts, err = lcli.ParseTipSetRef(ctx, api, cctx.String("tipset"))
			if err != nil {
				return err
			}
		}

		deals, err := api.StateMarketDeals(ctx, ts.Key())
		if err != nil {
			return err
		}

		recoveredDeals := make([]recoveredDeal, 0, 8192)

		projStats := make(map[string]*projectAggregateStats)
		projDealLists := make(map[string][]*individualDeal)
		grandTotals := competitionTotal{
			seenProject:  make(map[string]bool),
			seenClient:   make(map[address.Address]bool),
			seenProvider: make(map[address.Address]bool),
			seenPieceCid: make(map[cid.Cid]bool),
		}

		orderedDealList := make([]string, 0, len(deals))
		for dealID, dealInfo := range deals {
			// Only count deals whose sectors have properly started, not past/future ones
			// https://github.com/filecoin-project/specs-actors/blob/v0.9.9/actors/builtin/market/deal.go#L81-L85
			// Bail on 0 as well in case SectorStartEpoch is uninitialized due to some bug
			//
			// Additionally if the SlashEpoch is set this means the underlying sector is
			// terminated for whatever reason ( not just slashed ), and the deal record
			// will soon be removed from the state entirely
			if dealInfo.State.SectorStartEpoch <= 0 ||
				dealInfo.State.SectorStartEpoch > ts.Height() ||
				dealInfo.State.SlashEpoch > -1 {
				continue
			}

			orderedDealList = append(orderedDealList, dealID)
		}

		sort.Slice(orderedDealList, func(i, j int) bool {
			di, dj := deals[orderedDealList[i]], deals[orderedDealList[j]]
			switch {

			case di.State.SectorStartEpoch != dj.State.SectorStartEpoch:
				return di.State.SectorStartEpoch < dj.State.SectorStartEpoch

			case di.Proposal.StartEpoch != dj.Proposal.StartEpoch:
				return di.Proposal.StartEpoch < dj.Proposal.StartEpoch

			default:
				didi, _ := strconv.ParseInt(orderedDealList[i], 10, 64)
				didj, _ := strconv.ParseInt(orderedDealList[j], 10, 64)
				return didi < didj
			}
		})

		for _, dealID := range orderedDealList {

			dealInfo := deals[dealID]

			payloadCid := "unknown"
			payloadCidB32 := "unknown"
			if c, err := cid.Parse(dealInfo.Proposal.Label); err == nil {
				payloadCid = c.String()
				payloadCidB32 = cid.NewCidV1(c.Type(), c.Hash()).String()
			}

			clientAddr, found := resolvedWallets[dealInfo.Proposal.Client]
			if !found {
				var err error
				clientAddr, err = api.StateAccountKey(ctx, dealInfo.Proposal.Client, ts.Key())
				if err != nil {
					log.Warnf("failed to resolve id '%s' to wallet address: %s", dealInfo.Proposal.Client, err)
					continue
				}

				resolvedWallets[dealInfo.Proposal.Client] = clientAddr
			}

			if _, isRecover := knownRestoreClients[clientAddr]; isRecover &&
				dealInfo.State.SectorStartEpoch >= recoveryStart &&
				dealInfo.Proposal.EndEpoch-dealInfo.Proposal.StartEpoch > builtin.EpochsInDay*499 {
				recoveredDeals = append(recoveredDeals, recoveredDeal{
					DealID:          dealID,
					ClientAddress:   clientAddr.String(),
					MinerID:         dealInfo.Proposal.Provider.String(),
					PieceCID:        dealInfo.Proposal.PieceCID.String(),
					Label:           dealInfo.Proposal.Label,
					PayloadCIDb32:   payloadCidB32,
					PaddedPieceSize: uint64(dealInfo.Proposal.PieceSize),
					DataSize:        uint64(dealInfo.Proposal.PieceSize),
					DealStartEpoch:  int64(dealInfo.Proposal.StartEpoch),
					DealEndEpoch:    int64(dealInfo.Proposal.EndEpoch),
					RecoveryType:    1,
				})
			}

			// TEMP WORKAROUND
			if clientAddr.String() == "f17ia7m5mvizrdug3sqtevqw3tifiqvxqr3kdaeuq" && dealInfo.State.SectorStartEpoch >= recoveryStart {
				continue
			}

			projID, projKnown := knownAddrMap[clientAddr]
			if !projKnown {
				continue
			}

			projStatEntry, ok := projStats[projID]
			if !ok {
				projStatEntry = &projectAggregateStats{
					ProjectID:                projID,
					ClientStats:              make(map[string]*clientAggregateStats),
					timesSeenPieceCid:        make(map[cid.Cid]int),
					timesSeenPieceCidAllTime: make(map[cid.Cid]int),
					dataPerProvider:          make(map[address.Address]int64),
				}
				projStats[projID] = projStatEntry
			}

			projStatEntry.timesSeenPieceCidAllTime[dealInfo.Proposal.PieceCID]++

			if dealInfo.State.SectorStartEpoch < currentPhaseStart {
				continue
			}

			// anything under 360 days: not qualified
			if dealInfo.Proposal.EndEpoch-dealInfo.Proposal.StartEpoch < builtin.EpochsInDay*360 {
				continue
			}

			grandTotals.seenProject[projID] = true

			if projStatEntry.timesSeenPieceCidAllTime[dealInfo.Proposal.PieceCID] >= 10 {
				continue
			}

			grandTotals.seenClient[clientAddr] = true
			clientStatEntry, ok := projStatEntry.ClientStats[clientAddr.String()]
			if !ok {
				clientStatEntry = &clientAggregateStats{
					Client:    clientAddr.String(),
					cids:      make(map[cid.Cid]bool),
					providers: make(map[address.Address]bool),
				}
				projStatEntry.ClientStats[clientAddr.String()] = clientStatEntry
			}

			grandTotals.TotalBytes += int64(dealInfo.Proposal.PieceSize)
			projStatEntry.DataSize += int64(dealInfo.Proposal.PieceSize)
			clientStatEntry.DataSize += int64(dealInfo.Proposal.PieceSize)

			grandTotals.seenProvider[dealInfo.Proposal.Provider] = true
			projStatEntry.dataPerProvider[dealInfo.Proposal.Provider] += int64(dealInfo.Proposal.PieceSize)
			clientStatEntry.providers[dealInfo.Proposal.Provider] = true

			grandTotals.seenPieceCid[dealInfo.Proposal.PieceCID] = true
			projStatEntry.timesSeenPieceCid[dealInfo.Proposal.PieceCID]++
			clientStatEntry.cids[dealInfo.Proposal.PieceCID] = true

			grandTotals.TotalDeals++
			projStatEntry.NumDeals++
			clientStatEntry.NumDeals++

			if dealInfo.Proposal.VerifiedDeal {
				grandTotals.FilplusTotalDeals++
				grandTotals.FilplusTotalBytes += int64(dealInfo.Proposal.PieceSize)
			}

			projDealLists[projID] = append(projDealLists[projID], &individualDeal{
				DealID:         dealID,
				ProjectID:      projID,
				Client:         clientAddr.String(),
				MinerID:        dealInfo.Proposal.Provider.String(),
				PayloadCID:     payloadCid,
				PaddedSize:     int64(dealInfo.Proposal.PieceSize),
				DealStartEpoch: int64(dealInfo.State.SectorStartEpoch),
			})
		}

		//
		// Write out per-project deal lists
		for proj, dl := range projDealLists {
			err := func() error {
				outListFd, err := os.Create(fmt.Sprintf(outDirName+"/deals_list_%s.json", proj))
				if err != nil {
					return err
				}

				defer outListFd.Close() //nolint:errcheck

				sort.Slice(dl, func(i, j int) bool {
					return dl[j].PaddedSize < dl[i].PaddedSize
				})

				if err := json.NewEncoder(outListFd).Encode(
					dealListOutput{
						Epoch:    int64(ts.Height()),
						Endpoint: "DEAL_LIST",
						Payload:  dl,
					},
				); err != nil {
					return err
				}

				return nil
			}()

			if err != nil {
				return err
			}
		}

		//
		// write out basic_stats.json
		grandTotals.UniqueCids = len(grandTotals.seenPieceCid)
		grandTotals.UniqueClients = len(grandTotals.seenClient)
		grandTotals.UniqueProviders = len(grandTotals.seenProvider)
		grandTotals.UniqueProjects = len(grandTotals.seenProject)

		if err := json.NewEncoder(outBasicStatsFd).Encode(
			competitionTotalOutput{
				Epoch:    int64(ts.Height()),
				Endpoint: "COMPETITION_TOTALS",
				Payload:  grandTotals,
			},
		); err != nil {
			return err
		}

		//
		// write out recovery_deallist.json
		if err := json.NewEncoder(outRecoveryListFd).Encode(
			recoveryListOutput{
				Epoch:    int64(ts.Height()),
				Endpoint: "RECOVERED_DEALS_LIST",
				Payload:  recoveredDeals,
			},
		); err != nil {
			return err
		}

		//
		// write out client_stats.json
		for _, ps := range projStats {
			ps.NumCids = len(ps.timesSeenPieceCid)
			ps.NumProviders = len(ps.dataPerProvider)
			for _, dealsForCid := range ps.timesSeenPieceCid {
				if ps.HighestCidDealCount < dealsForCid {
					ps.HighestCidDealCount = dealsForCid
				}
			}
			for _, dataForProvider := range ps.dataPerProvider {
				if ps.DataSizeMaxProvider < dataForProvider {
					ps.DataSizeMaxProvider = dataForProvider
				}
			}

			for _, cs := range ps.ClientStats {
				cs.NumCids = len(cs.cids)
				cs.NumProviders = len(cs.providers)
			}
		}

		if err := json.NewEncoder(outClientStatsFd).Encode(
			projectAggregateStatsOutput{
				Epoch:    int64(ts.Height()),
				Endpoint: "PROJECT_DEAL_STATS",
				Payload:  projStats,
			},
		); err != nil {
			return err
		}

		return nil
	},
}

// Downloads and parses JSON input in the form:
// {
// 	"payload": [
// 		{
// 			"project": "5fb5f5b3ad3275e236287ce3",
// 			"address": "f3w3r2c6iukyh3u6f6kx62s5g6n2gf54aqp33ukqrqhje2y6xhf7k55przg4xqgahpcdal6laljz6zonma5pka"
// 		},
// 		{
// 			"project": "5fb608c4ad3275e236287ced",
// 			"address": "f3rs2khurnubol6ent27lpggidxxujqo2lg5aap5d5bmtam6yjb5wfla5cxxdgj45tqoaawgpzt5lofc3vpzfq"
// 		},
//  	...
//  ]
// }
func getAndParseProjectList(ctx context.Context, saveToDir, projListName string) (map[address.Address]string, error) {

	var projListSrc io.Reader

	if strings.HasPrefix(projListName, "http://") || strings.HasPrefix(projListName, "https://") {
		req, err := http.NewRequestWithContext(ctx, "GET", projListName, nil)
		if err != nil {
			return nil, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusOK {
			return nil, xerrors.Errorf("non-200 response: %d", resp.StatusCode)
		}

		projListSrc = resp.Body

	} else {
		inputFh, err := os.Open(projListName)
		if err != nil {
			return nil, xerrors.Errorf("failed to open '%s': %w", projListName, err)
		}
		defer inputFh.Close() //nolint:errcheck

		projListSrc = inputFh
	}

	projListCopy, err := os.Create(saveToDir + "/client_list.json")
	if err != nil {
		return nil, err
	}
	defer projListCopy.Close() //nolint:errcheck

	_, err = io.Copy(projListCopy, projListSrc)
	if err != nil {
		return nil, xerrors.Errorf("failed to copy from %s to %s: %w", projListName, saveToDir+"/client_list.json", err)
	}

	if _, err := projListCopy.Seek(0, 0); err != nil {
		return nil, err
	}

	projList, err := gabs.ParseJSONBuffer(projListCopy)
	if err != nil {
		return nil, err
	}
	proj, err := projList.Search("payload").Children()
	if err != nil {
		return nil, err
	}

	ret := make(map[address.Address]string, 64)

knownProject:
	for _, p := range proj {
		a, err := address.NewFromString(p.S("address").Data().(string))
		if err != nil {
			return nil, err
		}

		dsets, err := p.Search("curatedDataset").Children()
		if err != nil {
			return nil, err
		}

		// TEMP WORKAROUND
		// disqualify any project that has `landsat-8` registered
		for _, dset := range dsets {
			if dset.Data().(string) == "landsat-8" {
				continue knownProject
			}
		}

		ret[a] = p.S("project").Data().(string)
	}

	if len(ret) == 0 {
		return nil, xerrors.Errorf("no active projects/clients found in '%s': unable to continue", projListName)
	}

	return ret, nil
}

// Downloads and parses recovery list clients JSON:
func getAndParseRestore(ctx context.Context, saveToDir, restoreClientsListName string) (map[address.Address]struct{}, error) {

	var clientListSrc io.Reader

	if strings.HasPrefix(restoreClientsListName, "http://") || strings.HasPrefix(restoreClientsListName, "https://") {
		req, err := http.NewRequestWithContext(ctx, "GET", restoreClientsListName, nil)
		if err != nil {
			return nil, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close() //nolint:errcheck

		if resp.StatusCode != http.StatusOK {
			return nil, xerrors.Errorf("non-200 response: %d", resp.StatusCode)
		}

		clientListSrc = resp.Body

	} else {
		inputFh, err := os.Open(restoreClientsListName)
		if err != nil {
			return nil, xerrors.Errorf("failed to open '%s': %w", restoreClientsListName, err)
		}
		defer inputFh.Close() //nolint:errcheck

		clientListSrc = inputFh
	}

	clientListCopy, err := os.Create(saveToDir + "/restore_client_list.json")
	if err != nil {
		return nil, err
	}
	defer clientListCopy.Close() //nolint:errcheck

	_, err = io.Copy(clientListCopy, clientListSrc)
	if err != nil {
		return nil, xerrors.Errorf("failed to copy from %s to %s: %w", restoreClientsListName, saveToDir+"/restore_client_list.json", err)
	}

	if _, err := clientListCopy.Seek(0, 0); err != nil {
		return nil, err
	}

	fl := struct {
		Payload []address.Address `json:"payload"`
	}{}
	if err = json.NewDecoder(clientListCopy).Decode(&fl); err != nil {
		return nil, err
	}

	ret := make(map[address.Address]struct{})
	for _, a := range fl.Payload {
		ret[a] = struct{}{}
	}

	return ret, nil
}
