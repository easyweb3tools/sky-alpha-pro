package cli

import (
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"sky-alpha-pro/pkg/config"
	"sky-alpha-pro/pkg/logger"
)

var (
	cfgFile string
	verbose bool
	jsonOut bool
	dryRun  bool

	appConfig *config.Config
	appLogger *zap.Logger
)

var rootCmd = &cobra.Command{
	Use:   "sky-alpha-pro",
	Short: "Sky Alpha Pro CLI",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load(cfgFile)
		if err != nil {
			return err
		}
		if verbose {
			cfg.Log.Level = "debug"
		}
		if jsonOut {
			cfg.Log.Encoding = "json"
		}

		log, err := logger.New(cfg.Log.Level, cfg.Log.Encoding)
		if err != nil {
			return err
		}

		appConfig = cfg
		appLogger = log
		return nil
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		if appLogger != nil {
			_ = appLogger.Sync()
		}
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file path")
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "enable verbose logs")
	rootCmd.PersistentFlags().BoolVar(&jsonOut, "json", false, "json output where available")
	rootCmd.PersistentFlags().BoolVar(&dryRun, "dry-run", false, "simulate command execution")

	rootCmd.AddCommand(newVersionCmd())
	rootCmd.AddCommand(newDBCmd())
	rootCmd.AddCommand(newServeCmd())
	rootCmd.AddCommand(newMarketCmd())
	rootCmd.AddCommand(newWeatherCmd())
	rootCmd.AddCommand(newSignalCmd())
	rootCmd.AddCommand(newAgentCmd())
	rootCmd.AddCommand(newTradeCmd())
	rootCmd.AddCommand(newChainCmd())
	rootCmd.AddCommand(newPlayerCmd())
}

func Execute() error {
	return rootCmd.Execute()
}

func requireRuntime() error {
	if appConfig == nil || appLogger == nil {
		return fmt.Errorf("runtime not initialized")
	}
	return nil
}
