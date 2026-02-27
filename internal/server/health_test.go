package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"sky-alpha-pro/pkg/config"
)

func TestHealthHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	cfg := &config.Config{
		App: config.AppConfig{
			Name:    "sky-alpha-pro",
			Env:     "test",
			Version: "0.1.0-test",
		},
	}

	r := gin.New()
	r.GET("/health", HealthHandler(cfg, db))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: got=%d want=%d", w.Code, http.StatusOK)
	}
	if body := w.Body.String(); body == "" {
		t.Fatal("empty response body")
	}
}
