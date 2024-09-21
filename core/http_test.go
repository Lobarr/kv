package core

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/sync/errgroup"
)

func setupKvHttpServer(t *testing.T) (*KvHttpServer, *Engine) {
	kv, err := newKvHttpServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	kv.setupEndpoints()

	engine, err := makeEngineWithoutCompaction(t)
	if err != nil {
		t.Fatal(err)
	}

	return kv, engine
}

func TestHandleGetRequest(t *testing.T) {
	kv, engine := setupKvHttpServer(t)
	key := randomStringBetween(10, 20)
	value := randomString(1000)

	if err := engine.Set(key, value); err != nil {
		t.Fatal(err)
	}

	kv.store = engine
	defer kv.store.Close()
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/keys/%s", key), nil)

	resp, err := kv.server.Test(req)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	}

	if resp.Body == nil {
		t.Errorf("expected response body, got %v", resp.Body)
	}

	defer resp.Body.Close()

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	body := string(bodyBytes)
	if body != value {
		t.Errorf("expected body to be %s, got %s", value, body)
	}
}

func TestConcurrentHandleGetRequest(t *testing.T) {
	kv, engine := setupKvHttpServer(t)

	// Generate test data
	numKeys := 5000
	maxKeyLength := 20
	maxValueLength := 500

	// Generate random keys and values
	keys := make([]string, numKeys)
	values := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = randomStringBetween(10, maxKeyLength)
		values[i] = randomString(maxValueLength)
		if err := engine.Set(keys[i], values[i]); err != nil {
			t.Fatal(err)
		}
	}

	// Set the engine in the KvHttpServer
	kv.store = engine
	defer kv.store.Close()

	// Define the number of concurrent goroutines
	numGoroutines := 100000

	// Create a WaitGroup to wait for all goroutines to finish
	wg := new(errgroup.Group)
	wg.SetLimit(1000)

	// Run the concurrent goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Go(func() error {
			// Choose a random key to retrieve
			n := rand.Intn(numKeys)
			key := keys[n]
			value := values[n]

			// Create a new HTTP request
			req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/keys/%s", key), nil)

			// Send the request to the server and get the response
			resp, err := kv.server.Test(req)
			if err != nil {
				return fmt.Errorf("failed to send request: %v", err)
			}

			if resp == nil {
				return errors.New("expected response but got nil")
			}

			if resp.Body == nil {
				return errors.New("expected response body but got nil")
			}
			defer resp.Body.Close()

			// Read and validate the response
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("expected status code %d, got %d", http.StatusOK, resp.StatusCode)
			}

			bodyBytes, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("failed to read response body: %v", err)
			}

			body := string(bodyBytes)
			if body != value {
				return fmt.Errorf("for key %s expected body to be %s, got %s", key, value, body)
			}

			return nil
		})
	}

	// Wait for all goroutines to finish
	if err := wg.Wait(); err != nil {
		t.Fatal(err)
	}
}
