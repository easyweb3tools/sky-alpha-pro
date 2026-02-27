package cli

import (
	"context"
	"fmt"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"sky-alpha-pro/internal/server"
	"sky-alpha-pro/pkg/database"
)

func newServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Start HTTP API server",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireRuntime(); err != nil {
				return err
			}

			db, err := database.Open(appConfig.Database, appLogger)
			if err != nil {
				return err
			}
			defer database.Close(db)
			if err := database.AutoMigrate(db); err != nil {
				return err
			}

			handler := server.NewRouter(appConfig, appLogger, db)
			httpServer := &http.Server{
				Addr:              fmt.Sprintf("%s:%d", appConfig.Server.Host, appConfig.Server.Port),
				Handler:           handler,
				ReadHeaderTimeout: appConfig.Server.ReadHeaderTimeout,
				ReadTimeout:       appConfig.Server.ReadTimeout,
				WriteTimeout:      appConfig.Server.WriteTimeout,
			}

			errCh := make(chan error, 1)
			go func() {
				appLogger.Info("http server started",
					zap.String("host", appConfig.Server.Host),
					zap.Int("port", appConfig.Server.Port),
				)
				if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					errCh <- err
				}
			}()

			sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			select {
			case <-sigCtx.Done():
				ctx, cancel := context.WithTimeout(context.Background(), appConfig.Server.ShutdownTimeout)
				defer cancel()
				appLogger.Info("shutting down http server")
				return httpServer.Shutdown(ctx)
			case err := <-errCh:
				return err
			}
		},
	}
}
