package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	baseURL     = flag.String("base_url", "http://localhost:9998", "kv http server url")
	numRequests = flag.Int("num_requests", 10000, "number of requests")
	concurrency = flag.Int("concurrency", 100, "number of concurrent requests")
)

func randomString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func setKey(key, value string) error {
	url := fmt.Sprintf("%s/keys/%s", *baseURL, key)
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(value))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to set key '%s', status code: %d", key, resp.StatusCode)
	}

	return nil
}

func getKey(key string, expectedValue string) error {
	url := fmt.Sprintf("%s/keys/%s", *baseURL, key)
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get key '%s', status code: %d", key, resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	body := string(bodyBytes)
	if body != expectedValue {
		return fmt.Errorf("expected value for key '%s' is '%s', but received '%s'", key, expectedValue, body)
	}

	return nil
}

func main() {
	flag.Parse()

	start := time.Now()

	var wg errgroup.Group
	wg.SetLimit(*concurrency)

	for i := 0; i < *numRequests; i++ {
		wg.Go(func() error {
			id := randomString(8)
			key := fmt.Sprintf("key-%s", id)
			value := fmt.Sprintf("value-%s", id)

			if err := setKey(key, value); err != nil {
				return err
			}

			if err := getKey(key, value); err != nil {
				return err
			}

			return nil
		})
	}

	if err := wg.Wait(); err != nil {
		log.Fatalf("Benchmark failed: %v", err)
	}

	elapsed := time.Since(start)
	fmt.Printf("Benchmark completed in %s\n", elapsed)

	totalRequests := *numRequests * 2 // Set and Get requests
	requestsPerSecond := float64(totalRequests) / elapsed.Seconds()
	fmt.Printf("Total Requests: %d\n", totalRequests)
	fmt.Printf("Requests per Second: %.2f\n", requestsPerSecond)
}
