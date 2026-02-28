package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
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
