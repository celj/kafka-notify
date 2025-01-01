package middleware

import (
	"fmt"
	"net/http"
	"time"
)

func Logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		next.ServeHTTP(w, r)

		duration := time.Since(start)
		fmt.Printf("[%s] | %s | %s | %v\n",
			time.Now().Format("2006/01/02 - 15:04:05"),
			r.Method,
			r.URL.Path,
			duration,
		)
	})
}
