package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	fmt.Println("Starting ForkGuard Admin API...")

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	})

	server := &http.Server{Addr: ":8080"}

	go func() {
		fmt.Println("Admin API listening on :8080")
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			fmt.Printf("API server error: %v\n", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	<-stop

	fmt.Println("Shutting down Admin API...")

	server.Shutdown(nil)
	fmt.Println("Admin API stopped.")
}
