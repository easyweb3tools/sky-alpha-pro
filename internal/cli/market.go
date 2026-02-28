package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/signal"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"sky-alpha-pro/internal/market"
	"sky-alpha-pro/pkg/database"
)

func newMarketCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "market",
		Short: "Market data commands",
	}
	cmd.AddCommand(newMarketSyncCmd())
	cmd.AddCommand(newMarketListCmd())
	return cmd
}

func newMarketSyncCmd() *cobra.Command {
	var interval time.Duration

	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Sync weather markets from Gamma/CLOB",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}
			if dryRun {
				fmt.Fprintln(cmd.OutOrStdout(), "[dry-run] skip market sync")
				return nil
			}

			db, err := database.Open(appConfig.Database, appLogger)
			if err != nil {
				return err
			}
			defer database.Close(db)

			svc := market.NewService(appConfig.Market, db, appLogger)
			if interval <= 0 {
				var result *market.SyncResult
				err := runWithRetry(context.Background(), retryOptions{
					Attempts:       3,
					InitialBackoff: 500 * time.Millisecond,
					MaxBackoff:     4 * time.Second,
					IsRetryable:    isRetryableRuntimeErr,
					OnRetry: func(attempt int, err error, wait time.Duration) {
						fmt.Fprintf(cmd.ErrOrStderr(), "market sync attempt %d failed: %v; retry in %s\n", attempt, err, wait)
					},
				}, func(ctx context.Context) error {
					var runErr error
					result, runErr = svc.SyncMarkets(ctx)
					return runErr
				})
				if err != nil {
					return err
				}
				return writeMarketSyncResult(cmd, result)
			}

			stopCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			for {
				var result *market.SyncResult
				err := runWithRetry(stopCtx, retryOptions{
					Attempts:       3,
					InitialBackoff: 500 * time.Millisecond,
					MaxBackoff:     4 * time.Second,
					IsRetryable:    isRetryableRuntimeErr,
					OnRetry: func(attempt int, err error, wait time.Duration) {
						fmt.Fprintf(cmd.ErrOrStderr(), "market sync attempt %d failed: %v; retry in %s\n", attempt, err, wait)
					},
				}, func(ctx context.Context) error {
					var runErr error
					result, runErr = svc.SyncMarkets(ctx)
					return runErr
				})
				if err != nil {
					fmt.Fprintf(cmd.ErrOrStderr(), "market sync error: %v\n", err)
				} else if err := writeMarketSyncResult(cmd, result); err != nil {
					return err
				}

				select {
				case <-stopCtx.Done():
					return nil
				case <-ticker.C:
				}
			}
		},
	}

	cmd.Flags().DurationVar(&interval, "interval", 0, "run periodic sync (e.g. 5m); 0 means run once")
	return cmd
}

func newMarketListCmd() *cobra.Command {
	var (
		limit      int
		city       string
		marketType string
		active     bool
	)

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List synced markets",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}

			db, err := database.Open(appConfig.Database, appLogger)
			if err != nil {
				return err
			}
			defer database.Close(db)

			svc := market.NewService(appConfig.Market, db, appLogger)
			items, err := svc.ListMarketSnapshots(context.Background(), market.ListOptions{
				ActiveOnly: active,
				City:       city,
				MarketType: marketType,
				Limit:      limit,
			})
			if err != nil {
				return err
			}

			if jsonOut {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(items)
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			fmt.Fprintln(w, "POLYMARKET_ID\tCITY\tACTIVE\tPRICE_YES\tPRICE_NO\tCAPTURED_AT\tQUESTION")
			for _, item := range items {
				capturedAt := "-"
				if item.CapturedAt != nil {
					capturedAt = item.CapturedAt.UTC().Format(time.RFC3339)
				}
				fmt.Fprintf(
					w,
					"%s\t%s\t%t\t%.4f\t%.4f\t%s\t%s\n",
					item.PolymarketID,
					item.City,
					item.IsActive,
					item.PriceYes,
					item.PriceNo,
					capturedAt,
					item.Question,
				)
			}
			return w.Flush()
		},
	}

	cmd.Flags().IntVar(&limit, "limit", 20, "result size limit")
	cmd.Flags().StringVar(&city, "city", "", "city filter")
	cmd.Flags().StringVar(&marketType, "type", "", "market type filter")
	cmd.Flags().BoolVar(&active, "active", true, "show active markets only")
	return cmd
}

func writeMarketSyncResult(cmd *cobra.Command, result *market.SyncResult) error {
	if jsonOut {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "markets fetched:  %d\n", result.MarketsFetched)
	fmt.Fprintf(cmd.OutOrStdout(), "markets upserted: %d\n", result.MarketsUpserted)
	fmt.Fprintf(cmd.OutOrStdout(), "price snapshots:  %d\n", result.PriceSnapshots)
	if len(result.Errors) > 0 {
		fmt.Fprintf(cmd.OutOrStdout(), "errors:           %d\n", len(result.Errors))
		for i, msg := range result.Errors {
			fmt.Fprintf(cmd.OutOrStdout(), "%d. %s\n", i+1, msg)
		}
	}
	return nil
}

func isRetryableRuntimeErr(err error) bool {
	if err == nil {
		return false
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}
