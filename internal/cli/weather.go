package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"sky-alpha-pro/internal/weather"
	"sky-alpha-pro/pkg/database"
)

func newWeatherCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "weather",
		Short: "Weather data commands",
	}
	cmd.AddCommand(newWeatherForecastCmd())
	cmd.AddCommand(newWeatherObserveCmd())
	return cmd
}

func newWeatherForecastCmd() *cobra.Command {
	var (
		source string
		days   int
	)

	cmd := &cobra.Command{
		Use:   "forecast <location>",
		Short: "Get weather forecast",
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

			svc := weather.NewService(appConfig.Weather, db, appLogger)
			resp, err := svc.GetForecast(context.Background(), weather.ForecastRequest{
				Location: args[0],
				Source:   source,
				Days:     days,
			})
			if err != nil {
				return err
			}

			if jsonOut {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(resp)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "location: %s (%.4f, %.4f)\n", resp.Location, resp.Latitude, resp.Longitude)
			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			fmt.Fprintln(w, "SOURCE\tDATE\tHIGH_F\tLOW_F\tPRECIP_IN\tWIND_MPH")
			for _, item := range resp.Forecasts {
				fmt.Fprintf(w, "%s\t%s\t%.1f\t%.1f\t%.2f\t%.1f\n",
					item.Source,
					item.ForecastDate.UTC().Format("2006-01-02"),
					item.TempHighF,
					item.TempLowF,
					item.PrecipIn,
					item.WindSpeedMPH,
				)
			}
			_ = w.Flush()
			if len(resp.Errors) > 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "errors:")
				for _, e := range resp.Errors {
					fmt.Fprintf(cmd.OutOrStdout(), "- %s\n", e)
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&source, "source", "all", "forecast source: all|nws|openmeteo|visualcrossing")
	cmd.Flags().IntVar(&days, "days", 7, "forecast days")
	return cmd
}

func newWeatherObserveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "observe <station>",
		Short: "Get latest station observation",
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

			svc := weather.NewService(appConfig.Weather, db, appLogger)
			obs, err := svc.GetLatestObservation(context.Background(), args[0])
			if err != nil {
				return err
			}

			if jsonOut {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(obs)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "station: %s\n", strings.ToUpper(obs.StationID))
			fmt.Fprintf(cmd.OutOrStdout(), "observed_at: %s\n", obs.ObservedAt.UTC().Format(time.RFC3339))
			fmt.Fprintf(cmd.OutOrStdout(), "temp_f: %.1f\n", obs.TempF)
			fmt.Fprintf(cmd.OutOrStdout(), "humidity_pct: %d\n", obs.HumidityPct)
			fmt.Fprintf(cmd.OutOrStdout(), "wind_speed_mph: %.1f\n", obs.WindSpeedMPH)
			fmt.Fprintf(cmd.OutOrStdout(), "wind_dir: %s\n", obs.WindDir)
			fmt.Fprintf(cmd.OutOrStdout(), "description: %s\n", obs.Description)
			return nil
		},
	}
	return cmd
}
