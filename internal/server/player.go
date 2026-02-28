package server

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"sky-alpha-pro/internal/player"
)

func ListPlayersHandler(svc *player.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		limit, err := parseLimitQuery(c, 20)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"code": "BAD_REQUEST", "message": err.Error()}})
			return
		}
		minWeather := 0
		if raw := strings.TrimSpace(c.Query("min_weather_market")); raw != "" {
			v, convErr := strconv.Atoi(raw)
			if convErr != nil || v < 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"code": "BAD_REQUEST", "message": "invalid min_weather_market query parameter"}})
				return
			}
			minWeather = v
		}

		items, err := svc.ListPlayers(c.Request.Context(), player.ListOptions{Limit: limit, MinWeatherMarket: minWeather})
		if err != nil {
			status, code := mapPlayerError(err)
			c.JSON(status, gin.H{"error": gin.H{"code": code, "message": err.Error()}})
			return
		}
		c.JSON(http.StatusOK, gin.H{"items": items, "count": len(items)})
	}
}

func GetPlayerHandler(svc *player.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		addr := strings.TrimSpace(c.Param("address"))
		item, err := svc.GetPlayer(c.Request.Context(), addr)
		if err != nil {
			status, code := mapPlayerError(err)
			c.JSON(status, gin.H{"error": gin.H{"code": code, "message": err.Error()}})
			return
		}
		c.JSON(http.StatusOK, item)
	}
}

func ListPlayerPositionsHandler(svc *player.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		addr := strings.TrimSpace(c.Param("address"))
		limit, err := parseLimitQuery(c, 50)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"code": "BAD_REQUEST", "message": err.Error()}})
			return
		}
		items, listErr := svc.ListPlayerPositions(c.Request.Context(), addr, player.PositionOptions{Limit: limit})
		if listErr != nil {
			status, code := mapPlayerError(listErr)
			c.JSON(status, gin.H{"error": gin.H{"code": code, "message": listErr.Error()}})
			return
		}
		c.JSON(http.StatusOK, gin.H{"items": items, "count": len(items)})
	}
}

func GetPlayerLeaderboardHandler(svc *player.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		limit, err := parseLimitQuery(c, 20)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"code": "BAD_REQUEST", "message": err.Error()}})
			return
		}
		items, listErr := svc.GetLeaderboard(c.Request.Context(), player.LeaderboardOptions{
			Limit: limit,
			Type:  c.Query("type"),
		})
		if listErr != nil {
			status, code := mapPlayerError(listErr)
			c.JSON(status, gin.H{"error": gin.H{"code": code, "message": listErr.Error()}})
			return
		}
		c.JSON(http.StatusOK, gin.H{"items": items, "count": len(items)})
	}
}

func ComparePlayerHandler(svc *player.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		addr := strings.TrimSpace(c.Param("address"))
		item, err := svc.CompareWithMyStrategy(c.Request.Context(), addr)
		if err != nil {
			status, code := mapPlayerError(err)
			c.JSON(status, gin.H{"error": gin.H{"code": code, "message": err.Error()}})
			return
		}
		c.JSON(http.StatusOK, item)
	}
}

func SyncPlayersHandler(svc *player.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		limit, err := parseLimitQuery(c, 50)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": gin.H{"code": "BAD_REQUEST", "message": err.Error()}})
			return
		}
		res, syncErr := svc.RefreshFromCompetitors(c.Request.Context(), player.RefreshOptions{Limit: limit})
		if syncErr != nil {
			status, code := mapPlayerError(syncErr)
			c.JSON(status, gin.H{"error": gin.H{"code": code, "message": syncErr.Error()}})
			return
		}
		c.JSON(http.StatusOK, gin.H{"result": res})
	}
}

func mapPlayerError(err error) (int, string) {
	switch {
	case errors.Is(err, player.ErrPlayerBadRequest):
		return http.StatusBadRequest, "BAD_REQUEST"
	case errors.Is(err, player.ErrPlayerNotFound):
		return http.StatusNotFound, "NOT_FOUND"
	default:
		return http.StatusInternalServerError, "PLAYER_ERROR"
	}
}

func parseLimitQuery(c *gin.Context, dft int) (int, error) {
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			return 0, errors.New("invalid limit query parameter")
		}
		return limit, nil
	}
	return dft, nil
}
