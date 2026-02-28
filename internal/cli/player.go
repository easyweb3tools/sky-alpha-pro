package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"sky-alpha-pro/internal/player"
	"sky-alpha-pro/pkg/database"
)

func newPlayerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "player",
		Short: "Player tracking commands",
	}
	cmd.AddCommand(newPlayerSyncCmd())
	cmd.AddCommand(newPlayerListCmd())
	cmd.AddCommand(newPlayerShowCmd())
	cmd.AddCommand(newPlayerLeaderboardCmd())
	cmd.AddCommand(newPlayerCompareCmd())
	return cmd
}

func newPlayerSyncCmd() *cobra.Command {
	var limit int
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Build player ranking and positions from tracked activity",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}
			if dryRun {
				fmt.Fprintln(cmd.OutOrStdout(), "[dry-run] skip player sync")
				return nil
			}
			svc, closeDB, err := newPlayerService()
			if err != nil {
				return err
			}
			defer closeDB()
			res, err := svc.RefreshFromCompetitors(context.Background(), player.RefreshOptions{Limit: limit})
			if err != nil {
				return err
			}
			if jsonOut {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(res)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "players_upserted:   %d\n", res.PlayersUpserted)
			fmt.Fprintf(cmd.OutOrStdout(), "positions_upserted: %d\n", res.PositionsUpserted)
			fmt.Fprintf(cmd.OutOrStdout(), "started_at:         %s\n", res.StartedAt.UTC())
			fmt.Fprintf(cmd.OutOrStdout(), "finished_at:        %s\n", res.FinishedAt.UTC())
			return nil
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 50, "maximum players to refresh")
	return cmd
}

func newPlayerListCmd() *cobra.Command {
	var limit int
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List tracked players",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}
			svc, closeDB, err := newPlayerService()
			if err != nil {
				return err
			}
			defer closeDB()
			items, err := svc.ListPlayers(context.Background(), player.ListOptions{Limit: limit})
			if err != nil {
				return err
			}
			if jsonOut {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(items)
			}
			return writePlayerTable(cmd, items)
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 20, "result size limit")
	return cmd
}

func newPlayerShowCmd() *cobra.Command {
	var positionsLimit int
	cmd := &cobra.Command{
		Use:   "show <address>",
		Short: "Show one player and recent positions",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}
			svc, closeDB, err := newPlayerService()
			if err != nil {
				return err
			}
			defer closeDB()
			item, err := svc.GetPlayer(context.Background(), args[0])
			if err != nil {
				return err
			}
			positions, err := svc.ListPlayerPositions(context.Background(), args[0], player.PositionOptions{Limit: positionsLimit})
			if err != nil {
				return err
			}
			if jsonOut {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(map[string]any{"player": item, "positions": positions})
			}
			if err := writePlayerTable(cmd, []player.PlayerView{*item}); err != nil {
				return err
			}
			return writePlayerPositions(cmd, positions)
		},
	}
	cmd.Flags().IntVar(&positionsLimit, "positions-limit", 20, "max positions to show")
	return cmd
}

func newPlayerLeaderboardCmd() *cobra.Command {
	var (
		limit int
		typeV string
	)
	cmd := &cobra.Command{
		Use:   "leaderboard",
		Short: "Show player leaderboard",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}
			svc, closeDB, err := newPlayerService()
			if err != nil {
				return err
			}
			defer closeDB()
			items, err := svc.GetLeaderboard(context.Background(), player.LeaderboardOptions{Limit: limit, Type: typeV})
			if err != nil {
				return err
			}
			if jsonOut {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(items)
			}
			return writePlayerTable(cmd, items)
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 20, "result size limit")
	cmd.Flags().StringVar(&typeV, "type", "weather", "leaderboard type: weather|overall")
	return cmd
}

func newPlayerCompareCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compare <address>",
		Short: "Compare one player with current account strategy outcomes",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}
			svc, closeDB, err := newPlayerService()
			if err != nil {
				return err
			}
			defer closeDB()
			item, err := svc.CompareWithMyStrategy(context.Background(), args[0])
			if err != nil {
				return err
			}
			if jsonOut {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(item)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "player:             %s\n", item.PlayerAddress)
			if item.PlayerWinRateReady {
				fmt.Fprintf(cmd.OutOrStdout(), "player_win_rate:    %.2f%%\n", item.PlayerWinRate)
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "player_win_rate:    N/A (not computed yet)\n")
			}
			if item.PlayerTotalPnL != nil {
				fmt.Fprintf(cmd.OutOrStdout(), "player_total_pnl:   %.4f\n", *item.PlayerTotalPnL)
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "player_total_pnl:   N/A\n")
			}
			fmt.Fprintf(cmd.OutOrStdout(), "player_total_volume:%.4f\n", item.PlayerTotalVolume)
			fmt.Fprintf(cmd.OutOrStdout(), "my_win_rate:        %.2f%%\n", item.MyWinRate)
			fmt.Fprintf(cmd.OutOrStdout(), "my_realized_pnl:    %.4f\n", item.MyRealizedPnL)
			fmt.Fprintf(cmd.OutOrStdout(), "my_filled_trades:   %d\n", item.MyFilledTrades)
			if item.WinRateDiff != nil {
				fmt.Fprintf(cmd.OutOrStdout(), "win_rate_diff:      %.2f%%\n", *item.WinRateDiff)
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "win_rate_diff:      N/A\n")
			}
			if item.RealizedPnLDiff != nil {
				fmt.Fprintf(cmd.OutOrStdout(), "realized_pnl_diff:  %.4f\n", *item.RealizedPnLDiff)
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "realized_pnl_diff:  N/A (%s)\n", item.RealizedPnLDiffStatus)
			}
			return nil
		},
	}
	return cmd
}

func writePlayerTable(cmd *cobra.Command, items []player.PlayerView) error {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ADDRESS\tWEATHER_RANK\tOVERALL_RANK\tWEATHER_MARKETS\tTOTAL_MARKETS\tWIN_RATE\tLAST_ACTIVE")
	for _, it := range items {
		last := "-"
		if it.LastActiveAt != nil {
			last = it.LastActiveAt.UTC().Format("2006-01-02T15:04:05Z")
		}
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%.2f\t%s\n",
			it.Address, it.RankWeather, it.RankOverall, it.WeatherMarkets, it.TotalMarkets, it.WinRate, last)
	}
	return w.Flush()
}

func writePlayerPositions(cmd *cobra.Command, items []player.PlayerPositionView) error {
	fmt.Fprintln(cmd.OutOrStdout(), "")
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "MARKET_ID\tOUTCOME\tSIZE\tLAST_UPDATE")
	for _, it := range items {
		fmt.Fprintf(w, "%s\t%s\t%.4f\t%s\n", it.MarketID, it.Outcome, it.Size, it.LastUpdateAt.UTC().Format("2006-01-02T15:04:05Z"))
	}
	return w.Flush()
}

func newPlayerService() (*player.Service, func(), error) {
	db, err := database.Open(appConfig.Database, appLogger)
	if err != nil {
		return nil, nil, err
	}
	svc := player.NewService(db, appLogger)
	closeFn := func() { database.Close(db) }
	return svc, closeFn, nil
}
