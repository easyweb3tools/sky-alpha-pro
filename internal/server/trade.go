package server

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"sky-alpha-pro/internal/trade"
)

type submitTradeRequest struct {
	MarketID string  `json:"market_id"`
	Side     string  `json:"side"`
	Outcome  string  `json:"outcome"`
	Price    float64 `json:"price"`
	Size     float64 `json:"size"`
	Confirm  bool    `json:"confirm"`
}

func CreateTradeHandler(svc *trade.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req submitTradeRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "BAD_REQUEST", "message": "invalid json body"},
			})
			return
		}
		result, err := svc.SubmitOrder(c.Request.Context(), trade.SubmitOrderRequest{
			MarketRef: req.MarketID,
			Side:      req.Side,
			Outcome:   req.Outcome,
			Price:     req.Price,
			Size:      req.Size,
			Confirm:   req.Confirm,
		})
		if err != nil {
			status, code := mapTradeError(err)
			c.JSON(status, gin.H{"error": gin.H{"code": code, "message": err.Error()}})
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

func CancelTradeHandler(svc *trade.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		tradeID, err := strconv.ParseUint(c.Param("id"), 10, 64)
		if err != nil || tradeID == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "BAD_REQUEST", "message": "invalid trade id"},
			})
			return
		}
		result, err := svc.CancelOrder(c.Request.Context(), tradeID)
		if err != nil {
			status, code := mapTradeError(err)
			c.JSON(status, gin.H{"error": gin.H{"code": code, "message": err.Error()}})
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

func ListTradesHandler(svc *trade.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		limit := 20
		if raw := c.Query("limit"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": gin.H{"code": "BAD_REQUEST", "message": "invalid limit query parameter"},
				})
				return
			}
			limit = n
		}
		statusFilter := strings.TrimSpace(c.Query("status"))
		marketIDFilter := strings.TrimSpace(c.Query("market_id"))
		items, err := svc.ListTrades(c.Request.Context(), trade.ListTradesOptions{
			Limit:    limit,
			Status:   statusFilter,
			MarketID: marketIDFilter,
		})
		if err != nil {
			if errors.Is(err, trade.ErrTradeInvalidRequest) {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": gin.H{"code": "BAD_REQUEST", "message": err.Error()},
				})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": gin.H{"code": "INTERNAL_ERROR", "message": err.Error()},
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{"items": items, "count": len(items)})
	}
}

func GetTradeHandler(svc *trade.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		tradeID, err := strconv.ParseUint(c.Param("id"), 10, 64)
		if err != nil || tradeID == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "BAD_REQUEST", "message": "invalid trade id"},
			})
			return
		}
		item, err := svc.GetTradeByID(c.Request.Context(), tradeID)
		if err != nil {
			status, code := mapTradeError(err)
			c.JSON(status, gin.H{"error": gin.H{"code": code, "message": err.Error()}})
			return
		}
		c.JSON(http.StatusOK, item)
	}
}

func ListPositionsHandler(svc *trade.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		marketID := strings.TrimSpace(c.Query("market_id"))
		items, err := svc.ListPositions(c.Request.Context(), trade.ListPositionsOptions{MarketID: marketID})
		if err != nil {
			status, code := mapTradeError(err)
			c.JSON(status, gin.H{"error": gin.H{"code": code, "message": err.Error()}})
			return
		}
		c.JSON(http.StatusOK, gin.H{"items": items, "count": len(items)})
	}
}

func GetPnLReportHandler(svc *trade.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		var (
			from time.Time
			to   time.Time
			err  error
		)
		if raw := strings.TrimSpace(c.Query("from")); raw != "" {
			from, err = time.Parse("2006-01-02", raw)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": gin.H{"code": "BAD_REQUEST", "message": "invalid from query parameter, expected YYYY-MM-DD"},
				})
				return
			}
		}
		if raw := strings.TrimSpace(c.Query("to")); raw != "" {
			to, err = time.Parse("2006-01-02", raw)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": gin.H{"code": "BAD_REQUEST", "message": "invalid to query parameter, expected YYYY-MM-DD"},
				})
				return
			}
		}

		report, err := svc.GetPnLReport(c.Request.Context(), trade.PnLReportOptions{
			From: from,
			To:   to,
		})
		if err != nil {
			status, code := mapTradeError(err)
			c.JSON(status, gin.H{"error": gin.H{"code": code, "message": err.Error()}})
			return
		}
		c.JSON(http.StatusOK, report)
	}
}

func mapTradeError(err error) (int, string) {
	switch {
	case errors.Is(err, trade.ErrTradeInvalidRequest):
		return http.StatusBadRequest, "BAD_REQUEST"
	case errors.Is(err, trade.ErrTradeRiskRejected):
		return http.StatusUnprocessableEntity, "RISK_REJECTED"
	case errors.Is(err, trade.ErrTradeNotFound):
		return http.StatusNotFound, "NOT_FOUND"
	case errors.Is(err, trade.ErrTradeNotCancellable):
		return http.StatusConflict, "TRADE_NOT_CANCELLABLE"
	case errors.Is(err, trade.ErrTradeCLOB):
		return http.StatusBadGateway, "CLOB_REQUEST_FAILED"
	case errors.Is(err, trade.ErrTradePersistence):
		return http.StatusInternalServerError, "TRADE_PERSISTENCE_FAILED"
	case errors.Is(err, trade.ErrTradeConfig):
		return http.StatusInternalServerError, "TRADE_CONFIG_ERROR"
	default:
		return http.StatusInternalServerError, "TRADE_ERROR"
	}
}
