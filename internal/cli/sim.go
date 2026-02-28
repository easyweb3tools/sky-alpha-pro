package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os/signal"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"sky-alpha-pro/internal/market"
	"sky-alpha-pro/internal/sim"
	signalpkg "sky-alpha-pro/internal/signal"
	"sky-alpha-pro/internal/trade"
	"sky-alpha-pro/internal/weather"
	"sky-alpha-pro/pkg/database"
)

func newSimCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sim",
		Short: "Paper trading simulation commands",
	}
	cmd.AddCommand(newSimRunCmd())
	cmd.AddCommand(newSimReportCmd())
	return cmd
}

func newSimRunCmd() *cobra.Command {
	var (
		interval     time.Duration
		orderSize    float64
		minEdge      float64
		maxPositions int
		cycles       int
	)

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run automated paper trading simulation",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}

			// Safety: force paper mode
			appConfig.Trade.PaperMode = true
			appConfig.Trade.ConfirmationRequired = false

			if interval > 0 {
				appConfig.Sim.Interval = interval
			}
			if appConfig.Sim.Interval <= 0 {
				appConfig.Sim.Interval = 5 * time.Minute
			}
			if orderSize > 0 {
				appConfig.Sim.OrderSize = orderSize
			}
			if appConfig.Sim.OrderSize <= 0 {
				appConfig.Sim.OrderSize = 1.0
			}
			if minEdge > 0 {
				appConfig.Sim.MinEdgePct = minEdge
			}
			if appConfig.Sim.MinEdgePct <= 0 {
				appConfig.Sim.MinEdgePct = 5.0
			}
			if maxPositions > 0 {
				appConfig.Sim.MaxPositions = maxPositions
			}
			if appConfig.Sim.MaxPositions <= 0 {
				appConfig.Sim.MaxPositions = 20
			}

			db, err := database.Open(appConfig.Database, appLogger)
			if err != nil {
				return err
			}
			defer database.Close(db)

			marketSvc := market.NewService(appConfig.Market, db, appLogger)
			weatherSvc := weather.NewService(appConfig.Weather, db, appLogger)
			signalSvc := signalpkg.NewService(appConfig.Signal, db, appLogger)
			tradeSvc := trade.NewService(appConfig.Trade, appConfig.Market, db, appLogger, signalSvc)
			simSvc := sim.NewService(appConfig.Sim, db, appLogger, marketSvc, weatherSvc, signalSvc, tradeSvc)

			stopCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			ticker := time.NewTicker(appConfig.Sim.Interval)
			defer ticker.Stop()

			fmt.Fprintf(cmd.OutOrStdout(), "sim: starting paper trading (interval=%s, order_size=%.2f, min_edge=%.1f%%, max_positions=%d",
				appConfig.Sim.Interval, appConfig.Sim.OrderSize, appConfig.Sim.MinEdgePct, appConfig.Sim.MaxPositions)
			if cycles > 0 {
				fmt.Fprintf(cmd.OutOrStdout(), ", cycles=%d", cycles)
			}
			fmt.Fprintln(cmd.OutOrStdout(), ")")

			for cycleNum := 1; cycles <= 0 || cycleNum <= cycles; cycleNum++ {
				err := runWithRetry(stopCtx, retryOptions{
					Attempts:       3,
					InitialBackoff: 1 * time.Second,
					MaxBackoff:     8 * time.Second,
					OnRetry: func(attempt int, err error, wait time.Duration) {
						fmt.Fprintf(cmd.ErrOrStderr(), "sim cycle %d attempt %d failed: %v; retry in %s\n", cycleNum, attempt, err, wait)
					},
				}, func(ctx context.Context) error {
					timeout := appConfig.Sim.Interval * 2
					if timeout < 30*time.Second {
						timeout = 30 * time.Second
					}
					cycleCtx, cancel := context.WithTimeout(ctx, timeout)
					defer cancel()

					result, err := simSvc.RunCycle(cycleCtx, cycleNum)
					if err != nil {
						return err
					}
					printCycleResult(cmd, result)
					return nil
				})
				if err != nil {
					fmt.Fprintf(cmd.ErrOrStderr(), "sim cycle %d error: %v\n", cycleNum, err)
				}

				if cycles > 0 && cycleNum >= cycles {
					break
				}

				select {
				case <-stopCtx.Done():
					fmt.Fprintln(cmd.OutOrStdout(), "\nsim: stopped")
					return nil
				case <-ticker.C:
				}
			}

			fmt.Fprintln(cmd.OutOrStdout(), "sim: completed")
			return nil
		},
	}

	cmd.Flags().DurationVar(&interval, "interval", 0, "cycle interval (0 uses config sim.interval)")
	cmd.Flags().Float64Var(&orderSize, "order-size", 0, "USDC per trade (0 uses config)")
	cmd.Flags().Float64Var(&minEdge, "min-edge", 0, "minimum edge %% (0 uses config)")
	cmd.Flags().IntVar(&maxPositions, "max-positions", 0, "max simultaneous positions (0 uses config)")
	cmd.Flags().IntVar(&cycles, "cycles", 0, "number of cycles (0 = infinite)")
	return cmd
}

func printCycleResult(cmd *cobra.Command, r *sim.CycleResult) {
	if jsonOut {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		_ = enc.Encode(r)
		return
	}
	fmt.Fprintf(cmd.OutOrStdout(), "[cycle %d] synced=%d signals=%d trades=%d settled=%d errors=%d\n",
		r.Cycle, r.MarketsSynced, r.SignalsGenerated, r.TradesPlaced, r.TradesSettled, len(r.Errors))
	for _, e := range r.Errors {
		fmt.Fprintf(cmd.ErrOrStderr(), "  err: %s\n", e)
	}
}

func newSimReportCmd() *cobra.Command {
	var (
		fromStr string
		toStr   string
	)

	cmd := &cobra.Command{
		Use:   "report",
		Short: "Show paper trading simulation report",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}

			var fromTime, toTime time.Time
			var err error
			if fromStr != "" {
				fromTime, err = time.Parse("2006-01-02", fromStr)
				if err != nil {
					return fmt.Errorf("invalid --from: %w", err)
				}
			}
			if toStr != "" {
				toTime, err = time.Parse("2006-01-02", toStr)
				if err != nil {
					return fmt.Errorf("invalid --to: %w", err)
				}
			}

			db, err := database.Open(appConfig.Database, appLogger)
			if err != nil {
				return err
			}
			defer database.Close(db)

			signalSvc := signalpkg.NewService(appConfig.Signal, db, appLogger)
			tradeSvc := trade.NewService(appConfig.Trade, appConfig.Market, db, appLogger, signalSvc)
			simSvc := sim.NewService(appConfig.Sim, db, appLogger, nil, nil, nil, tradeSvc)

			report, err := simSvc.GetReport(context.Background(), sim.ReportOptions{
				From: fromTime,
				To:   toTime,
			})
			if err != nil {
				return err
			}

			if jsonOut {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(report)
			}

			printSimReport(cmd, report)
			return nil
		},
	}

	cmd.Flags().StringVar(&fromStr, "from", "", "start date (YYYY-MM-DD)")
	cmd.Flags().StringVar(&toStr, "to", "", "end date (YYYY-MM-DD)")
	return cmd
}

func printSimReport(cmd *cobra.Command, r *sim.SimReport) {
	fmt.Fprintf(cmd.OutOrStdout(), "Paper Trading Report (%s to %s)\n", r.From.Format("2006-01-02"), r.To.Format("2006-01-02"))
	fmt.Fprintln(cmd.OutOrStdout(), "─────────────────────────────────────────")
	fmt.Fprintf(cmd.OutOrStdout(), "Total Trades:     %d\n", r.TotalTrades)
	fmt.Fprintf(cmd.OutOrStdout(), "Filled Trades:    %d\n", r.FilledTrades)
	fmt.Fprintf(cmd.OutOrStdout(), "Win / Loss:       %d / %d\n", r.WinTrades, r.LossTrades)
	fmt.Fprintf(cmd.OutOrStdout(), "Win Rate:         %.1f%%\n", r.WinRate)
	fmt.Fprintf(cmd.OutOrStdout(), "Realized P&L:     %.4f USDC\n", r.RealizedPnLUSDC)
	fmt.Fprintf(cmd.OutOrStdout(), "Unrealized P&L:   %.4f USDC\n", r.UnrealizedPnL)
	fmt.Fprintf(cmd.OutOrStdout(), "Pending Trades:   %d\n", r.PendingTrades)
	fmt.Fprintf(cmd.OutOrStdout(), "Avg Edge (Win):   %.2f%%\n", r.AvgEdgeWinning)
	fmt.Fprintf(cmd.OutOrStdout(), "Avg Edge (Loss):  %.2f%%\n", r.AvgEdgeLosing)
	fmt.Fprintf(cmd.OutOrStdout(), "Gross Volume:     %.4f USDC\n", r.GrossVolumeUSDC)
	fmt.Fprintf(cmd.OutOrStdout(), "Open Exposure:    %.4f USDC\n", r.OpenExposureUSDC)

	if len(r.Daily) > 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "\nDaily Breakdown:")
		tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 2, 2, ' ', 0)
		fmt.Fprintln(tw, "DATE\tPNL\tVOLUME\tTRADES")
		for _, d := range r.Daily {
			fmt.Fprintf(tw, "%s\t%.4f\t%.4f\t%d\n", d.Date, d.RealizedPnL, d.GrossVolume, d.FilledTrades)
		}
		tw.Flush()
	}
}
