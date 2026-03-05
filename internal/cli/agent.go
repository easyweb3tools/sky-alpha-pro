package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"sky-alpha-pro/internal/agent"
	"sky-alpha-pro/internal/signal"
	"sky-alpha-pro/internal/weather"
	"sky-alpha-pro/pkg/database"
)

func newAgentCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "agent",
		Short: "AI agent analysis commands",
	}
	cmd.AddCommand(newAgentCycleCmd())
	cmd.AddCommand(newAgentAnalyzeCmd())
	cmd.AddCommand(newAgentSignalsCmd())
	return cmd
}

func newAgentCycleCmd() *cobra.Command {
	var (
		runMode             string
		maxToolCalls        int
		maxExternalRequests int
		memoryWindow        int
		marketLimit         int
	)
	cmd := &cobra.Command{
		Use:   "cycle",
		Short: "Run one agent cycle (plan + tools)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}

			db, err := database.Open(appConfig.Database, appLogger)
			if err != nil {
				return err
			}
			defer database.Close(db)

			weatherSvc := weather.NewService(appConfig.Weather, db, appLogger)
			signalSvc := signal.NewService(appConfig.Signal, db, appLogger)
			agentSvc := agent.NewService(appConfig.Agent, db, appLogger, weatherSvc, signalSvc)

			result, err := agentSvc.RunCycle(context.Background(), agent.CycleOptions{
				RunMode:             runMode,
				TradeEnabled:        false,
				MaxToolCalls:        maxToolCalls,
				MaxExternalRequests: maxExternalRequests,
				MemoryWindow:        memoryWindow,
				MarketLimit:         marketLimit,
			})
			if err != nil {
				return err
			}
			if jsonOut {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(result)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "session:   %s\n", result.SessionID)
			fmt.Fprintf(cmd.OutOrStdout(), "cycle:     %s\n", result.CycleID)
			fmt.Fprintf(cmd.OutOrStdout(), "status:    %s\n", result.Status)
			fmt.Fprintf(cmd.OutOrStdout(), "decision:  %s\n", result.Decision)
			fmt.Fprintf(cmd.OutOrStdout(), "llm_calls: %d\n", result.LLMCalls)
			fmt.Fprintf(cmd.OutOrStdout(), "llm_tokens:%d\n", result.LLMTokens)
			fmt.Fprintf(cmd.OutOrStdout(), "tools:     %d\n", result.ToolCalls)
			return nil
		},
	}
	cmd.Flags().StringVar(&runMode, "run-mode", "observe", "run mode: observe|simulate|trade")
	cmd.Flags().IntVar(&maxToolCalls, "max-tool-calls", 0, "max tool calls per cycle")
	cmd.Flags().IntVar(&maxExternalRequests, "max-external-requests", 0, "max external requests per cycle")
	cmd.Flags().IntVar(&memoryWindow, "memory-window", 0, "recent memory windows for context")
	cmd.Flags().IntVar(&marketLimit, "market-limit", 0, "max markets for batch tools")
	return cmd
}

func newAgentAnalyzeCmd() *cobra.Command {
	var (
		all   bool
		limit int
		depth string
	)

	cmd := &cobra.Command{
		Use:   "analyze [market_id]",
		Short: "Run agent analysis for one market or active markets",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}

			marketID := ""
			if len(args) > 0 {
				marketID = strings.TrimSpace(args[0])
			}
			if marketID == "" && !all {
				return fmt.Errorf("provide market_id or use --all")
			}

			db, err := database.Open(appConfig.Database, appLogger)
			if err != nil {
				return err
			}
			defer database.Close(db)

			weatherSvc := weather.NewService(appConfig.Weather, db, appLogger)
			signalSvc := signal.NewService(appConfig.Signal, db, appLogger)
			agentSvc := agent.NewService(appConfig.Agent, db, appLogger, weatherSvc, signalSvc)

			resp, err := agentSvc.Analyze(context.Background(), agent.AnalyzeRequest{
				MarketID: marketID,
				All:      all,
				Limit:    limit,
				Depth:    depth,
			})
			if err != nil {
				return err
			}

			if jsonOut {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(resp)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "session: %s\n", resp.SessionID)
			fmt.Fprintf(cmd.OutOrStdout(), "analyzed: %d\n", resp.Count)
			if len(resp.Errors) > 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "errors:   %d\n", len(resp.Errors))
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			fmt.Fprintln(w, "POLYMARKET_ID\tEDGE_PCT\tCONF\tMARKET_PRICE\tOUR_PROB\tRECOMMENDATION")
			for _, item := range resp.Items {
				fmt.Fprintf(w, "%s\t%.2f\t%.1f\t%.4f\t%.4f\t%s\n",
					item.PolymarketID,
					item.Analysis.EdgePct,
					item.Analysis.Confidence,
					item.Analysis.MarketPriceYes,
					item.Analysis.OurProbability,
					item.Analysis.Recommendation,
				)
			}
			if err := w.Flush(); err != nil {
				return err
			}
			if len(resp.Errors) > 0 {
				for i, msg := range resp.Errors {
					fmt.Fprintf(cmd.OutOrStdout(), "%d. %s\n", i+1, msg)
				}
			}
			return nil
		},
	}

	cmd.Flags().BoolVar(&all, "all", false, "analyze active markets")
	cmd.Flags().IntVar(&limit, "limit", 0, "max active markets when --all")
	cmd.Flags().StringVar(&depth, "depth", "summary", "analysis depth: summary|full")
	return cmd
}

func newAgentSignalsCmd() *cobra.Command {
	var (
		limit   int
		minEdge float64
	)

	cmd := &cobra.Command{
		Use:   "signals",
		Short: "List generated agent signals",
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
