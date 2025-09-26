package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	fmt.Println("Starting ForkGuard Dispatcher...")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Dispatcher running. Press Ctrl+C to exit.")

	<-stop

	fmt.Println("Shutting down Dispatcher...")

	time.Sleep(1 * time.Second)
	fmt.Println("Dispatcher stopped.")
}
