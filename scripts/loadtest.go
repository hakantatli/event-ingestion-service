package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hakantatli/event-ingestion-service/internal/domain"
)

var (
	baseURL         string
	totalEvents     int
	concurrency     int
	batchSize       int
	reportIntervalS int
	mode            string
)

func init() {
	flag.StringVar(&baseURL, "url", "http://localhost:8080", "Base URL of the ingestion service")
	flag.IntVar(&totalEvents, "total", 1_000_000, "Total number of events to send")
	flag.IntVar(&concurrency, "concurrency", 100, "Number of concurrent workers")
	flag.IntVar(&batchSize, "batch", 1000, "Number of events per bulk request (bulk mode only)")
	flag.IntVar(&reportIntervalS, "report", 1, "Interval in seconds to report progress")
	flag.StringVar(&mode, "mode", "single", "Mode: 'single' or 'bulk'")
}

var eventNames = []string{"product_view", "add_to_cart", "purchase", "page_visit", "signup"}
var channels = []string{"web", "mobile", "api"}

func generateEvent(i int) domain.Event {
	return domain.Event{
		EventName:  eventNames[rand.Intn(len(eventNames))],
		UserID:     fmt.Sprintf("user_%d", rand.Intn(10000)),
		Timestamp:  time.Now().Unix() - int64(rand.Intn(86400*30)),
		Channel:    channels[rand.Intn(len(channels))],
		CampaignID: fmt.Sprintf("cmp_%d", rand.Intn(100)),
		Tags:       []string{"load_test"},
		Metadata:   map[string]any{"test_id": i},
	}
}

func runTest(client *http.Client) {
	var sentCount atomic.Int64
	var errCount atomic.Int64
	var errorLogCount atomic.Int32
	var wg sync.WaitGroup

	// Reporter goroutine
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Duration(reportIntervalS) * time.Second)
		defer ticker.Stop()
		var lastCount int64
		for {
			select {
			case <-ticker.C:
				current := sentCount.Load()
				rate := current - lastCount
				lastCount = current
				fmt.Printf("  [%s] Sent: %6d | Rate: %5d events/s | Errors: %d\n",
					time.Now().Format("15:04:05"), current, rate, errCount.Load())
			case <-done:
				return
			}
		}
	}()

	start := time.Now()

	if mode == "single" {
		targetURL := baseURL + "/events"
		// Using slightly smaller channel to prevent extreme memory blowout if testing millions
		eventCh := make(chan domain.Event, concurrency*2)
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for ev := range eventCh {
					body, _ := json.Marshal(ev)
					resp, err := client.Post(targetURL, "application/json", bytes.NewReader(body))
					if err != nil {
						if errorLogCount.Add(1) <= 10 {
							fmt.Printf("\n[Error] Network Failure: %v\n", err)
						}
						errCount.Add(1)
						continue
					}

					if resp.StatusCode >= 400 {
						if errorLogCount.Add(1) <= 10 {
							bodyBytes, _ := io.ReadAll(resp.Body)
							fmt.Printf("\n[Error] HTTP %d: %s\n", resp.StatusCode, string(bodyBytes))
						}
						errCount.Add(1)
					} else {
						// CRITICAL: Must drain body completely for Keep-Alive connection re-use!
						io.Copy(io.Discard, resp.Body)
						sentCount.Add(1)
					}
					resp.Body.Close()
				}
			}()
		}
		for i := 0; i < totalEvents; i++ {
			eventCh <- generateEvent(i)
		}
		close(eventCh)

	} else {
		targetURL := baseURL + "/events/bulk"
		batchCh := make(chan []domain.Event, concurrency)
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for batch := range batchCh {
					body, _ := json.Marshal(batch)
					resp, err := client.Post(targetURL, "application/json", bytes.NewReader(body))
					if err != nil {
						if errorLogCount.Add(1) <= 10 {
							fmt.Printf("\n[Error] Network Failure (Bulk): %v\n", err)
						}
						errCount.Add(int64(len(batch)))
						continue
					}
					if resp.StatusCode >= 400 {
						if errorLogCount.Add(1) <= 10 {
							bodyBytes, _ := io.ReadAll(resp.Body)
							fmt.Printf("\n[Error] HTTP %d (Bulk): %s\n", resp.StatusCode, string(bodyBytes))
						}
						errCount.Add(int64(len(batch)))
					} else {
						// CRITICAL: Must drain body completely for Keep-Alive connection re-use!
						io.Copy(io.Discard, resp.Body)
						sentCount.Add(int64(len(batch)))
					}
					resp.Body.Close()
				}
			}()
		}
		batch := make([]domain.Event, 0, batchSize)
		for i := 0; i < totalEvents; i++ {
			batch = append(batch, generateEvent(i))
			if len(batch) >= batchSize {
				batchCh <- batch
				batch = make([]domain.Event, 0, batchSize)
			}
		}
		if len(batch) > 0 {
			batchCh <- batch
		}
		close(batchCh)
	}

	wg.Wait()
	close(done)

	elapsed := time.Since(start)
	total := sentCount.Load()
	fmt.Println("\n=== Results ===")
	fmt.Printf("Total Sent:     %d\n", total)
	fmt.Printf("Total Errors:   %d\n", errCount.Load())
	fmt.Printf("Duration:       %s\n", elapsed.Round(time.Millisecond))

	if elapsed.Seconds() > 0 {
		fmt.Printf("Avg Throughput: %.0f events/sec\n", float64(total)/elapsed.Seconds())
	}
}

func main() {
	flag.Parse()

	// Maintain previous behavior: implicitly capture "go run scripts/loadtest.go bulk"
	args := flag.Args()
	if len(args) > 0 && args[0] == "bulk" {
		mode = "bulk"
	}

	client := &http.Client{
		Timeout: 20 * time.Second, // Provide a slightly more generous timeout
		Transport: &http.Transport{
			MaxConnsPerHost:     concurrency * 2, // Helps prevent connection bottlenecks
			MaxIdleConns:        concurrency,
			MaxIdleConnsPerHost: concurrency,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	fmt.Println("=== Event Ingestion Load Test ===")
	fmt.Printf("URL:         %s\n", baseURL)
	fmt.Printf("Mode:        %s\n", mode)
	fmt.Printf("Total:       %d events\n", totalEvents)
	fmt.Printf("Concurrency: %d goroutines\n", concurrency)
	if mode == "bulk" {
		fmt.Printf("Batch Size:  %d events/request\n", batchSize)
	}
	fmt.Println()

	runTest(client)
}
