package server

import (
	"net/http"
	"strconv"

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
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "TRADE_SUBMIT_FAILED", "message": err.Error()},
			})
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
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "TRADE_CANCEL_FAILED", "message": err.Error()},
			})
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
		items, err := svc.ListTrades(c.Request.Context(), trade.ListTradesOptions{Limit: limit})
		if err != nil {
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
			c.JSON(http.StatusNotFound, gin.H{
				"error": gin.H{"code": "NOT_FOUND", "message": err.Error()},
			})
			return
		}
		c.JSON(http.StatusOK, item)
	}
}
