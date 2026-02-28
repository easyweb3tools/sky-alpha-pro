package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"sky-alpha-pro/internal/chain"
	"sky-alpha-pro/pkg/database"
)

func newChainCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "chain",
		Short: "Chain scanner commands",
	}
	cmd.AddCommand(newChainScanCmd())
	cmd.AddCommand(newChainBotsCmd())
	cmd.AddCommand(newChainWatchCmd())
	return cmd
}

func newChainScanCmd() *cobra.Command {
	var (
		lookback       uint64
		maxTx          int
		botMinTrades   int
		botMaxInterval float64
	)

	cmd := &cobra.Command{
		Use:   "scan",
		Short: "Scan Polygon events and refresh competitor stats",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}
			if dryRun {
				fmt.Fprintln(cmd.OutOrStdout(), "[dry-run] skip chain scan")
				return nil
			}

			svc, closeDB, err := newChainService()
			if err != nil {
				return err
			}
			defer closeDB()

			var result *chain.ScanResult
			err = runWithRetry(context.Background(), retryOptions{
				Attempts:       3,
				InitialBackoff: 500 * time.Millisecond,
				MaxBackoff:     4 * time.Second,
				IsRetryable:    isRetryableChainErr,
				OnRetry: func(attempt int, err error, wait time.Duration) {
					fmt.Fprintf(cmd.ErrOrStderr(), "chain scan attempt %d failed: %v; retry in %s\n", attempt, err, wait)
				},
			}, func(ctx context.Context) error {
				var runErr error
				result, runErr = svc.Scan(ctx, chain.ScanOptions{
					LookbackBlocks:       lookback,
					MaxTx:                maxTx,
					BotMinTrades:         botMinTrades,
					BotMaxAvgIntervalSec: botMaxInterval,
				})
				return runErr
			})
			if err != nil {
				return err
			}
			return writeChainScanResult(cmd, result)
		},
	}

	cmd.Flags().Uint64Var(&lookback, "lookback-blocks", 0, "scan lookback block count (0 uses config)")
	cmd.Flags().IntVar(&maxTx, "max-tx", 0, "max unique tx to process (0 uses config)")
	cmd.Flags().IntVar(&botMinTrades, "bot-min-trades", 0, "bot detection minimum trades (0 uses config)")
	cmd.Flags().Float64Var(&botMaxInterval, "bot-max-avg-interval-sec", 0, "bot detection max avg trade interval in seconds (0 uses config)")
	return cmd
}

func newChainBotsCmd() *cobra.Command {
	var (
		limit      int
		all        bool
		address    string
		trades     int
		showTrades bool
	)

	cmd := &cobra.Command{
		Use:   "bots",
		Short: "List suspicious bot competitors",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}

			svc, closeDB, err := newChainService()
			if err != nil {
				return err
			}
			defer closeDB()

			address = strings.TrimSpace(address)
			if address != "" {
				item, err := svc.GetCompetitor(context.Background(), address)
				if err != nil {
					return err
				}
				if showTrades {
					entries, err := svc.ListCompetitorTrades(context.Background(), address, trades)
					if err != nil {
						return err
					}
					if jsonOut {
						payload := map[string]any{
							"competitor": item,
							"trades":     entries,
						}
						enc := json.NewEncoder(cmd.OutOrStdout())
						enc.SetIndent("", "  ")
						return enc.Encode(payload)
					}
					writeCompetitorsTable(cmd, []chain.CompetitorView{*item})
					return writeCompetitorTradesTable(cmd, entries)
				}
				if jsonOut {
					enc := json.NewEncoder(cmd.OutOrStdout())
					enc.SetIndent("", "  ")
					return enc.Encode(item)
				}
				return writeCompetitorsTable(cmd, []chain.CompetitorView{*item})
			}

			items, err := svc.ListCompetitors(context.Background(), chain.ListCompetitorsOptions{
				OnlyBots: !all,
				Limit:    limit,
			})
			if err != nil {
				return err
			}

			if jsonOut {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(items)
			}
			return writeCompetitorsTable(cmd, items)
		},
	}

	cmd.Flags().IntVar(&limit, "limit", 20, "result size limit")
	cmd.Flags().BoolVar(&all, "all", false, "include non-bot addresses")
	cmd.Flags().StringVar(&address, "address", "", "show one competitor by address")
	cmd.Flags().BoolVar(&showTrades, "trades", false, "when --address is set, include recent competitor trades")
	cmd.Flags().IntVar(&trades, "trades-limit", 20, "recent trades limit when --trades is enabled")
	return cmd
}

func newChainWatchCmd() *cobra.Command {
	var (
		interval time.Duration
		lookback uint64
		maxTx    int
		limit    int
	)

	cmd := &cobra.Command{
		Use:   "watch",
		Short: "Periodically scan chain and print latest bot list",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}
			if dryRun {
				fmt.Fprintln(cmd.OutOrStdout(), "[dry-run] skip chain watch")
				return nil
			}

			svc, closeDB, err := newChainService()
			if err != nil {
				return err
			}
			defer closeDB()

			if interval <= 0 {
				interval = appConfig.Chain.WatchInterval
			}
			if interval <= 0 {
				interval = 30 * time.Second
			}

			stopCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			for {
				err := runWithRetry(stopCtx, retryOptions{
					Attempts:       3,
					InitialBackoff: 500 * time.Millisecond,
					MaxBackoff:     4 * time.Second,
					IsRetryable:    isRetryableChainErr,
					OnRetry: func(attempt int, err error, wait time.Duration) {
						fmt.Fprintf(cmd.ErrOrStderr(), "chain watch cycle attempt %d failed: %v; retry in %s\n", attempt, err, wait)
					},
				}, func(ctx context.Context) error {
					return runOneWatchCycle(ctx, interval, cmd, svc, lookback, maxTx, limit)
				})
				if err != nil {
					fmt.Fprintf(cmd.ErrOrStderr(), "chain watch cycle error: %v\n", err)
				}
				select {
				case <-stopCtx.Done():
					return nil
				case <-ticker.C:
				}
			}
		},
	}

	cmd.Flags().DurationVar(&interval, "interval", 0, "watch loop interval (0 uses config chain.watch_interval)")
	cmd.Flags().Uint64Var(&lookback, "lookback-blocks", 0, "scan lookback block count (0 uses config)")
	cmd.Flags().IntVar(&maxTx, "max-tx", 0, "max unique tx to process per cycle (0 uses config)")
	cmd.Flags().IntVar(&limit, "limit", 10, "bot list size per cycle")
	return cmd
}

func runOneWatchCycle(stopCtx context.Context, interval time.Duration, cmd *cobra.Command, svc *chain.Service, lookback uint64, maxTx int, limit int) error {
	timeout := interval * 2
	if timeout < 15*time.Second {
		timeout = 15 * time.Second
	}
	cycleCtx, cancel := context.WithTimeout(stopCtx, timeout)
	defer cancel()

	result, err := svc.Scan(cycleCtx, chain.ScanOptions{
		LookbackBlocks: lookback,
		MaxTx:          maxTx,
	})
	if err != nil {
		return err
	}
	bots, err := svc.ListCompetitors(cycleCtx, chain.ListCompetitorsOptions{
		OnlyBots: true,
		Limit:    limit,
	})
	if err != nil {
		return err
	}

	if jsonOut {
		payload := map[string]any{
			"result": result,
			"bots":   bots,
		}
		enc := json.NewEncoder(cmd.OutOrStdout())
		return enc.Encode(payload)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "\n[%s] scan finished\n", time.Now().UTC().Format(time.RFC3339))
	if err := writeChainScanResult(cmd, result); err != nil {
		return err
	}
	if len(bots) == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "no bot competitor detected")
		return nil
	}
	return writeCompetitorsTable(cmd, bots)
}

func isRetryableChainErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, chain.ErrChainUnavailable) || errors.Is(err, context.DeadlineExceeded)
}

func writeChainScanResult(cmd *cobra.Command, result *chain.ScanResult) error {
	if jsonOut {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "from_block:          %d\n", result.FromBlock)
	fmt.Fprintf(cmd.OutOrStdout(), "to_block:            %d\n", result.ToBlock)
	fmt.Fprintf(cmd.OutOrStdout(), "observed_logs:       %d\n", result.ObservedLogs)
	fmt.Fprintf(cmd.OutOrStdout(), "observed_tx:         %d\n", result.ObservedTx)
	fmt.Fprintf(cmd.OutOrStdout(), "inserted_trades:     %d\n", result.InsertedTrades)
	fmt.Fprintf(cmd.OutOrStdout(), "duplicate_trades:    %d\n", result.DuplicateTrades)
	fmt.Fprintf(cmd.OutOrStdout(), "updated_competitors: %d\n", result.UpdatedCompetitor)
	fmt.Fprintf(cmd.OutOrStdout(), "started_at:          %s\n", result.StartedAt.UTC().Format(time.RFC3339))
	fmt.Fprintf(cmd.OutOrStdout(), "finished_at:         %s\n", result.FinishedAt.UTC().Format(time.RFC3339))
	return nil
}

func writeCompetitorsTable(cmd *cobra.Command, items []chain.CompetitorView) error {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ADDRESS\tBOT\tCONF\tTRADES\tAVG_INTERVAL_SEC\tFIRST_SEEN\tLAST_SEEN")
	for _, it := range items {
		firstSeen := "-"
		lastSeen := "-"
		if it.FirstSeenAt != nil {
			firstSeen = it.FirstSeenAt.UTC().Format(time.RFC3339)
		}
		if it.LastSeenAt != nil {
			lastSeen = it.LastSeenAt.UTC().Format(time.RFC3339)
		}
		fmt.Fprintf(w, "%s\t%t\t%.2f\t%d\t%.2f\t%s\t%s\n",
			it.Address, it.IsBot, it.BotConfidence, it.TotalTrades, it.AvgTradeIntervalSec, firstSeen, lastSeen)
	}
	return w.Flush()
}

func writeCompetitorTradesTable(cmd *cobra.Command, items []chain.CompetitorTradeView) error {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "TX_HASH\tBLOCK\tMARKET_ID\tSIDE\tOUTCOME\tAMOUNT_USDC\tGAS_GWEI\tTIMESTAMP")
	for _, it := range items {
		amount := "-"
		if it.AmountUSDC != nil {
			amount = fmt.Sprintf("%.6f", *it.AmountUSDC)
		}
		gas := "-"
		if it.GasPriceGwei != nil {
			gas = fmt.Sprintf("%.4f", *it.GasPriceGwei)
		}
		fmt.Fprintf(w, "%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
			it.TxHash,
			it.BlockNumber,
			it.MarketID,
			it.Side,
			it.Outcome,
			amount,
			gas,
			it.Timestamp.UTC().Format(time.RFC3339),
		)
	}
	return w.Flush()
}

func newChainService() (*chain.Service, func(), error) {
	db, err := database.Open(appConfig.Database, appLogger)
	if err != nil {
		return nil, nil, err
	}
	svc := chain.NewService(appConfig.Chain, db, appLogger)
	closeFn := func() {
		svc.Close()
		database.Close(db)
	}
	return svc, closeFn, nil
}
