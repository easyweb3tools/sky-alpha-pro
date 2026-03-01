package cli

import (
	"context"
	"fmt"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"sky-alpha-pro/internal/scheduler"
	"sky-alpha-pro/internal/server"
	"sky-alpha-pro/internal/sim"
	"sky-alpha-pro/internal/trade"
	"sky-alpha-pro/pkg/database"
	"sky-alpha-pro/pkg/metrics"
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

			metricReg := metrics.New(appConfig.Metrics)
			services := server.NewServicesWithMetrics(appConfig, appLogger, db, metricReg)
			if services.Chain != nil {
				defer services.Chain.Close()
			}

			sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			// Build dedicated sim service for scheduler paper trading.
			// Reuse shared market/weather/signal services to avoid duplicated upstream state.
			simTradeCfg := appConfig.Trade
			simTradeCfg.PaperMode = true
			simTradeCfg.ConfirmationRequired = false
			simTradeSvc := trade.NewService(simTradeCfg, appConfig.Market, db, appLogger, services.Signal)
			simSvc := sim.NewService(appConfig.Sim, db, appLogger, services.Market, services.Weather, services.Signal, simTradeSvc)

			schedulerMgr := scheduler.NewManager(appConfig.Scheduler, appLogger, metricReg)
			schedulerMgr.SetRunRecorder(scheduler.NewGormRunRecorder(db))
			scheduler.RegisterDefaultJobs(schedulerMgr, appConfig, db, services.Market, services.Weather, services.Chain, simSvc, appLogger)

			handler := server.NewRouterWithServices(appConfig, appLogger, db, metricReg, services, schedulerMgr)
			httpServer := &http.Server{
				Addr:              fmt.Sprintf("%s:%d", appConfig.Server.Host, appConfig.Server.Port),
				Handler:           handler,
				ReadHeaderTimeout: appConfig.Server.ReadHeaderTimeout,
				ReadTimeout:       appConfig.Server.ReadTimeout,
				WriteTimeout:      appConfig.Server.WriteTimeout,
			}

			schedulerMgr.Start(sigCtx)

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

			select {
			case <-sigCtx.Done():
				schedulerMgr.Wait()
				ctx, cancel := context.WithTimeout(context.Background(), appConfig.Server.ShutdownTimeout)
				defer cancel()
				appLogger.Info("shutting down http server")
				return httpServer.Shutdown(ctx)
			case err := <-errCh:
				stop()
				schedulerMgr.Wait()
				return err
			}
		},
	}
}
