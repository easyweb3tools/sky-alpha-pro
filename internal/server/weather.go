package server

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"sky-alpha-pro/internal/weather"
)

func GetForecastHandler(svc *weather.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		location := c.Query("location")
		if location == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "BAD_REQUEST", "message": "location is required"},
			})
			return
		}

		days := 7
		if raw := c.Query("days"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": gin.H{"code": "BAD_REQUEST", "message": "days must be positive integer"},
				})
				return
			}
			days = n
		}

		resp, err := svc.GetForecast(c.Request.Context(), weather.ForecastRequest{
			Location: location,
			Source:   c.Query("source"),
			Days:     days,
		})
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{
				"error": gin.H{"code": "FORECAST_FAILED", "message": err.Error()},
			})
			return
		}
		c.JSON(http.StatusOK, resp)
	}
}

func GetObservationHandler(svc *weather.Service) gin.HandlerFunc {
	return func(c *gin.Context) {
		station := c.Param("station")
		if station == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{"code": "BAD_REQUEST", "message": "station is required"},
			})
			return
		}

		obs, err := svc.GetLatestObservation(c.Request.Context(), station)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{
				"error": gin.H{"code": "OBSERVATION_FAILED", "message": err.Error()},
			})
			return
		}
		c.JSON(http.StatusOK, obs)
	}
}
