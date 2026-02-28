package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"sky-alpha-pro/internal/signal"
	"sky-alpha-pro/internal/trade"
	"sky-alpha-pro/pkg/database"
)

func newTradeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "trade",
		Short: "Trade execution commands",
	}
	cmd.AddCommand(newTradeBuyCmd())
	cmd.AddCommand(newTradeSellCmd())
	cmd.AddCommand(newTradeCancelCmd())
	cmd.AddCommand(newTradeListCmd())
	cmd.AddCommand(newTradePositionsCmd())
	cmd.AddCommand(newTradePnLCmd())
	return cmd
}

func newTradeBuyCmd() *cobra.Command {
	return newTradeSubmitCmd("buy", "BUY")
}

func newTradeSellCmd() *cobra.Command {
	return newTradeSubmitCmd("sell", "SELL")
}

func newTradeSubmitCmd(use string, side string) *cobra.Command {
	var (
		outcome string
		price   float64
		size    float64
		confirm bool
	)
	cmd := &cobra.Command{
		Use:   use + " <market_id>",
		Short: side + " order",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}
			if appConfig.Trade.ConfirmationRequired && !confirm {
				ok, err := promptTradeConfirmation(cmd, side, args[0], outcome, price, size)
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("order aborted by user")
				}
				confirm = true
			}
			db, err := database.Open(appConfig.Database, appLogger)
			if err != nil {
				return err
			}
			defer database.Close(db)

			signalSvc := signal.NewService(appConfig.Signal, db, appLogger)
			svc := trade.NewService(appConfig.Trade, appConfig.Market, db, appLogger, signalSvc)
			resp, err := svc.SubmitOrder(context.Background(), trade.SubmitOrderRequest{
				MarketRef: args[0],
				Side:      side,
				Outcome:   outcome,
				Price:     price,
				Size:      size,
				Confirm:   confirm,
			})
			if err != nil {
				return err
			}
			if jsonOut {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(resp)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "trade_id: %d\n", resp.TradeID)
			fmt.Fprintf(cmd.OutOrStdout(), "order_id: %s\n", resp.OrderID)
			fmt.Fprintf(cmd.OutOrStdout(), "status:   %s\n", resp.Status)
			fmt.Fprintf(cmd.OutOrStdout(), "cost:     %.4f USDC\n", resp.CostUSDC)
			return nil
		},
	}
	cmd.Flags().StringVar(&outcome, "outcome", "", "YES or NO")
	cmd.Flags().Float64Var(&price, "price", 0, "limit price (0-1)")
	cmd.Flags().Float64Var(&size, "size", 0, "order size in shares")
	cmd.Flags().BoolVar(&confirm, "confirm", false, "confirm order placement")
	_ = cmd.MarkFlagRequired("outcome")
	_ = cmd.MarkFlagRequired("price")
	_ = cmd.MarkFlagRequired("size")
	return cmd
}

func newTradeCancelCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cancel <trade_id>",
		Short: "Cancel a placed trade",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}
			tradeID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil || tradeID == 0 {
				return fmt.Errorf("invalid trade id")
			}

			db, err := database.Open(appConfig.Database, appLogger)
			if err != nil {
				return err
			}
			defer database.Close(db)

			signalSvc := signal.NewService(appConfig.Signal, db, appLogger)
			svc := trade.NewService(appConfig.Trade, appConfig.Market, db, appLogger, signalSvc)
			resp, err := svc.CancelOrder(context.Background(), tradeID)
			if err != nil {
				return err
			}
			if jsonOut {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(resp)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "trade_id: %d\n", resp.TradeID)
			fmt.Fprintf(cmd.OutOrStdout(), "order_id: %s\n", resp.OrderID)
			fmt.Fprintf(cmd.OutOrStdout(), "status:   %s\n", resp.Status)
			return nil
		},
	}
	return cmd
}

func newTradeListCmd() *cobra.Command {
	var (
		limit    int
		status   string
		marketID string
	)
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List trade records",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}
			db, err := database.Open(appConfig.Database, appLogger)
			if err != nil {
				return err
			}
			defer database.Close(db)

			signalSvc := signal.NewService(appConfig.Signal, db, appLogger)
			svc := trade.NewService(appConfig.Trade, appConfig.Market, db, appLogger, signalSvc)
			items, err := svc.ListTrades(context.Background(), trade.ListTradesOptions{
				Limit:    limit,
				Status:   status,
				MarketID: marketID,
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
			fmt.Fprintln(w, "ID\tMARKET_ID\tSIDE\tOUTCOME\tPRICE\tSIZE\tCOST\tSTATUS\tCREATED_AT")
			for _, it := range items {
				fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%.4f\t%.4f\t%.4f\t%s\t%s\n",
					it.ID, it.MarketID, it.Side, it.Outcome, it.Price, it.Size, it.CostUSDC, it.Status,
					it.CreatedAt.UTC().Format(time.RFC3339))
			}
			return w.Flush()
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 20, "result size limit")
	cmd.Flags().StringVar(&status, "status", "", "status filter (pending/placed/partially_filled/filled/cancelled/closed/failed)")
	cmd.Flags().StringVar(&marketID, "market-id", "", "market id filter")
	return cmd
}

func newTradePositionsCmd() *cobra.Command {
	var marketID string
	cmd := &cobra.Command{
		Use:   "positions",
		Short: "List current positions",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}
			db, err := database.Open(appConfig.Database, appLogger)
			if err != nil {
				return err
			}
			defer database.Close(db)

			signalSvc := signal.NewService(appConfig.Signal, db, appLogger)
			svc := trade.NewService(appConfig.Trade, appConfig.Market, db, appLogger, signalSvc)
			items, err := svc.ListPositions(context.Background(), trade.ListPositionsOptions{MarketID: marketID})
			if err != nil {
				return err
			}
			if jsonOut {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(items)
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			fmt.Fprintln(w, "MARKET_ID\tOUTCOME\tNET_SIZE\tAVG_ENTRY\tMARK_PRICE\tUNREALIZED_PNL\tREALIZED_PNL\tUPDATED_AT")
			for _, it := range items {
				mark := "-"
				if it.MarkPrice != nil {
					mark = fmt.Sprintf("%.4f", *it.MarkPrice)
				}
				unrealized := "-"
				if it.UnrealizedPnL != nil {
					unrealized = fmt.Sprintf("%.4f", *it.UnrealizedPnL)
				}
				fmt.Fprintf(w, "%s\t%s\t%.4f\t%.4f\t%s\t%s\t%.4f\t%s\n",
					it.MarketID, it.Outcome, it.NetSize, it.AvgEntryPrice, mark, unrealized, it.RealizedPnL,
					it.UpdatedAt.UTC().Format(time.RFC3339))
			}
			return w.Flush()
		},
	}
	cmd.Flags().StringVar(&marketID, "market-id", "", "market id filter")
	return cmd
}

func newTradePnLCmd() *cobra.Command {
	var (
		from string
		to   string
	)
	cmd := &cobra.Command{
		Use:   "pnl",
		Short: "Get PnL report",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}
			var fromTime time.Time
			if strings.TrimSpace(from) != "" {
				parsed, err := time.Parse("2006-01-02", strings.TrimSpace(from))
				if err != nil {
					return fmt.Errorf("invalid --from, expected YYYY-MM-DD")
				}
				fromTime = parsed
			}
			var toTime time.Time
			if strings.TrimSpace(to) != "" {
				parsed, err := time.Parse("2006-01-02", strings.TrimSpace(to))
				if err != nil {
					return fmt.Errorf("invalid --to, expected YYYY-MM-DD")
				}
				toTime = parsed
			}

			db, err := database.Open(appConfig.Database, appLogger)
			if err != nil {
				return err
			}
			defer database.Close(db)

			signalSvc := signal.NewService(appConfig.Signal, db, appLogger)
			svc := trade.NewService(appConfig.Trade, appConfig.Market, db, appLogger, signalSvc)
			report, err := svc.GetPnLReport(context.Background(), trade.PnLReportOptions{
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

			fmt.Fprintf(cmd.OutOrStdout(), "from:              %s\n", report.From.UTC().Format("2006-01-02"))
			fmt.Fprintf(cmd.OutOrStdout(), "to:                %s\n", report.To.UTC().Format("2006-01-02"))
			fmt.Fprintf(cmd.OutOrStdout(), "total_trades:      %d\n", report.TotalTrades)
			fmt.Fprintf(cmd.OutOrStdout(), "filled_trades:     %d\n", report.FilledTrades)
			fmt.Fprintf(cmd.OutOrStdout(), "win_trades:        %d\n", report.WinTrades)
			fmt.Fprintf(cmd.OutOrStdout(), "loss_trades:       %d\n", report.LossTrades)
			fmt.Fprintf(cmd.OutOrStdout(), "win_rate:          %.2f%%\n", report.WinRate)
			fmt.Fprintf(cmd.OutOrStdout(), "gross_volume_usdc: %.4f\n", report.GrossVolumeUSDC)
			fmt.Fprintf(cmd.OutOrStdout(), "realized_pnl_usdc: %.4f\n", report.RealizedPnLUSDC)
			fmt.Fprintf(cmd.OutOrStdout(), "open_exposure:     %.4f\n", report.OpenExposureUSDC)

			if len(report.Daily) > 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "")
				w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
				fmt.Fprintln(w, "DATE\tREALIZED_PNL\tGROSS_VOLUME\tFILLED_TRADES")
				for _, day := range report.Daily {
					fmt.Fprintf(w, "%s\t%.4f\t%.4f\t%d\n", day.Date, day.RealizedPnL, day.GrossVolume, day.FilledTrades)
				}
				if err := w.Flush(); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&from, "from", "", "start date (YYYY-MM-DD)")
	cmd.Flags().StringVar(&to, "to", "", "end date (YYYY-MM-DD)")
	return cmd
}

func promptTradeConfirmation(cmd *cobra.Command, side, marketID, outcome string, price, size float64) (bool, error) {
	out := cmd.ErrOrStderr()
	_, _ = fmt.Fprintf(out, "confirm order %s market=%s outcome=%s price=%.4f size=%.4f ? [y/N]: ",
		side, marketID, strings.ToUpper(strings.TrimSpace(outcome)), price, size)
	reader := bufio.NewReader(cmd.InOrStdin())
	line, err := reader.ReadString('\n')
	if err != nil {
		return false, err
	}
	answer := strings.ToLower(strings.TrimSpace(line))
	return answer == "y" || answer == "yes", nil
}
