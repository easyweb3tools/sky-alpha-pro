package server

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"

	"sky-alpha-pro/internal/chain"
)

var chainScanMu sync.Mutex

type chainScanRequest struct {
	LookbackBlocks       uint64  `json:"lookback_blocks"`
	MaxTx                int     `json:"max_tx"`
	BotMinTrades         int     `json:"bot_min_trades"`
	BotMaxAvgIntervalSec float64 `json:"bot_max_avg_interval_sec"`
}

func ScanChainHandler(svc *chain.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !chainScanMu.TryLock() {
			c.JSON(http.StatusConflict, gin.H{
				"error": gin.H{"code": "CHAIN_SCAN_RUNNING", "message": "chain scan already running"},
			})
			return
		}
		defer chainScanMu.Unlock()

		var body chainScanRequest
		if c.Request.ContentLength > 0 {
			if err := c.ShouldBindJSON(&body); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": gin.H{"code": "BAD_REQUEST", "message": "invalid json body"},
				})
				return
			}
		}

		result, err := svc.Scan(c.Request.Context(), chain.ScanOptions{
			LookbackBlocks:       body.LookbackBlocks,
			MaxTx:                body.MaxTx,
			BotMinTrades:         body.BotMinTrades,
			BotMaxAvgIntervalSec: body.BotMaxAvgIntervalSec,
		})
		if err != nil {
			status, code := mapChainError(err)
			c.JSON(status, gin.H{"error": gin.H{"code": code, "message": err.Error()}})
			return
		}
		c.JSON(http.StatusOK, gin.H{"result": result})
	}
}

func ListCompetitorsHandler(svc *chain.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		onlyBots := false
		if raw := strings.TrimSpace(c.Query("only_bots")); raw != "" {
			parsed, err := strconv.ParseBool(raw)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": gin.H{"code": "BAD_REQUEST", "message": "invalid only_bots query parameter"},
				})
				return
			}
			onlyBots = parsed
		}

		limit := 20
		if parsed, err := parsePositiveIntQuery(c, "limit", 20); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "BAD_REQUEST", "message": err.Error()},
			})
			return
		} else {
			limit = parsed
		}

		items, err := svc.ListCompetitors(c.Request.Context(), chain.ListCompetitorsOptions{
			OnlyBots: onlyBots,
			Limit:    limit,
		})
		if err != nil {
			status, code := mapChainError(err)
			c.JSON(status, gin.H{"error": gin.H{"code": code, "message": err.Error()}})
			return
		}
		c.JSON(http.StatusOK, gin.H{"items": items, "count": len(items)})
	}
}

func GetCompetitorHandler(svc *chain.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		address := strings.TrimSpace(c.Param("address"))
		if address == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "BAD_REQUEST", "message": "address is required"},
			})
			return
		}

		item, err := svc.GetCompetitor(c.Request.Context(), address)
		if err != nil {
			status, code := mapChainError(err)
			c.JSON(status, gin.H{"error": gin.H{"code": code, "message": err.Error()}})
			return
		}
		c.JSON(http.StatusOK, item)
	}
}

func ListCompetitorTradesHandler(svc *chain.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		address := strings.TrimSpace(c.Param("address"))
		if address == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "BAD_REQUEST", "message": "address is required"},
			})
			return
		}

		limit := 50
		if parsed, err := parsePositiveIntQuery(c, "limit", 50); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "BAD_REQUEST", "message": err.Error()},
			})
			return
		} else {
			limit = parsed
		}

		items, err := svc.ListCompetitorTrades(c.Request.Context(), address, limit)
		if err != nil {
			status, code := mapChainError(err)
			c.JSON(status, gin.H{"error": gin.H{"code": code, "message": err.Error()}})
			return
		}
		c.JSON(http.StatusOK, gin.H{"items": items, "count": len(items)})
	}
}

func mapChainError(err error) (int, string) {
	switch {
	case errors.Is(err, chain.ErrChainBadRequest):
		return http.StatusBadRequest, "BAD_REQUEST"
	case errors.Is(err, chain.ErrChainNotFound):
		return http.StatusNotFound, "NOT_FOUND"
	case errors.Is(err, chain.ErrChainUnavailable):
		return http.StatusBadGateway, "CHAIN_UNAVAILABLE"
	default:
		return http.StatusInternalServerError, "CHAIN_ERROR"
	}
}
