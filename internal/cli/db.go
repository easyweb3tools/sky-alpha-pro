package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"sky-alpha-pro/pkg/database"
)

func newDBCmd() *cobra.Command {
	dbCmd := &cobra.Command{
		Use:   "db",
		Short: "Database utilities",
	}
	dbCmd.AddCommand(newDBMigrateCmd())
	return dbCmd
}

func newDBMigrateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "migrate",
		Short: "Apply GORM AutoMigrate schema changes",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}
			if dryRun {
				fmt.Fprintln(cmd.OutOrStdout(), "[dry-run] skip db migrate")
				return nil
			}

			db, err := database.Open(appConfig.Database, appLogger)
			if err != nil {
				return err
			}
			defer database.Close(db)

			if err := database.AutoMigrate(db); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), "auto-migrate applied")
			return nil
		},
	}
}
