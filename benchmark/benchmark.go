package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	baseURL     = "http://localhost:9998"
	numRequests = 10000
	concurrency = 100
)

type keyValue struct {
	key   string
	value string
}

func setKey(key, value string) error {
	url := fmt.Sprintf("%s/keys/%s", baseURL, key)
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
	url := fmt.Sprintf("%s/keys/%s", baseURL, key)
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get key '%s', status code: %d", key, resp.StatusCode)
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
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
	start := time.Now()

	dataSet := make([]keyValue, numRequests)
	for i := 0; i < numRequests; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		dataSet[i] = keyValue{key: key, value: value}
	}

	var wg errgroup.Group

	for i := 0; i < numRequests; i++ {
		keyValue := dataSet[i]
		key := keyValue.key
		value := keyValue.value

		wg.Go(func() error {
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

	totalRequests := numRequests * 2 // Set and Get requests
	requestsPerSecond := float64(totalRequests) / elapsed.Seconds()
	fmt.Printf("Total Requests: %d\n", totalRequests)
	fmt.Printf("Requests per Second: %.2f\n", requestsPerSecond)
}
