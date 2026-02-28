package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"sky-alpha-pro/internal/signal"
	"sky-alpha-pro/pkg/database"
)

func newSignalCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "signal",
		Short: "Signal generation commands",
	}
	cmd.AddCommand(newSignalGenerateCmd())
	cmd.AddCommand(newSignalListCmd())
	return cmd
}

func newSignalGenerateCmd() *cobra.Command {
	var limit int
	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate trading signals in batch",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}
			if dryRun {
				fmt.Fprintln(cmd.OutOrStdout(), "[dry-run] skip signal generate")
				return nil
			}

			db, err := database.Open(appConfig.Database, appLogger)
			if err != nil {
				return err
			}
			defer database.Close(db)

			svc := signal.NewService(appConfig.Signal, db, appLogger)
			result, err := svc.GenerateSignals(context.Background(), signal.GenerateOptions{Limit: limit})
			if err != nil {
				return err
			}
			if jsonOut {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(result)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "processed: %d\n", result.Processed)
			fmt.Fprintf(cmd.OutOrStdout(), "generated: %d\n", result.Generated)
			fmt.Fprintf(cmd.OutOrStdout(), "skipped:   %d\n", result.Skipped)
			if len(result.Errors) > 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "errors:    %d\n", len(result.Errors))
			}
			return nil
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 0, "max markets to process")
	return cmd
}

func newSignalListCmd() *cobra.Command {
	var (
		limit   int
		minEdge float64
	)
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List generated signals",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}
			db, err := database.Open(appConfig.Database, appLogger)
			if err != nil {
				return err
			}
			defer database.Close(db)

			svc := signal.NewService(appConfig.Signal, db, appLogger)
			items, err := svc.ListSignals(context.Background(), signal.ListOptions{
				Limit:   limit,
				MinEdge: minEdge,
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
			fmt.Fprintln(w, "ID\tMARKET_ID\tDATE\tDIR\tEDGE_PCT\tCONF\tPRICE\tESTIMATE\tCREATED_AT")
			for _, it := range items {
				fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%.2f\t%.1f\t%.4f\t%.4f\t%s\n",
					it.ID, it.MarketID, it.SignalDate.UTC().Format("2006-01-02"), it.Direction, it.EdgePct, it.Confidence,
					it.MarketPrice, it.OurEstimate, it.CreatedAt.UTC().Format(time.RFC3339))
			}
			return w.Flush()
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 20, "result size limit")
	cmd.Flags().Float64Var(&minEdge, "min-edge", 0, "minimum absolute edge percent")
	return cmd
}
