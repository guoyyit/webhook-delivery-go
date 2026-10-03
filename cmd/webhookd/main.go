// Command webhookd runs the webhook delivery HTTP service.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/guoyyit/webhook-delivery-go/internal/api"
	"github.com/guoyyit/webhook-delivery-go/internal/breaker"
	"github.com/guoyyit/webhook-delivery-go/internal/engine"
	"github.com/guoyyit/webhook-delivery-go/internal/retry"
)

func main() {
	var (
		addr       = flag.String("addr", ":8080", "HTTP listen address")
		storeDir   = flag.String("store-dir", "./data", "persistence directory")
		reqTimeout = flag.Duration("request-timeout", 10*time.Second, "single delivery attempt timeout")
	)
	flag.Parse()

	cfg := engine.Config{
		StoreDir:       *storeDir,
		RequestTimeout: *reqTimeout,
		Retry:          retry.DefaultPolicy(),
		Breaker:        breaker.DefaultConfig(),
	}
	if v := os.Getenv("WEBHOOKD_ADDR"); v != "" {
		*addr = v
	}

	eng, err := engine.New(cfg)
	if err != nil {
		log.Fatalf("engine init: %v", err)
	}
	if err := eng.Start(nil); err != nil {
		log.Fatalf("engine start: %v", err)
	}
	defer eng.Close()

	h := api.New(eng)
	log.Printf("webhookd listening on %s", *addr)
	if err := http.ListenAndServe(*addr, h.Routes()); err != nil {
		log.Fatal(err)
	}
}
