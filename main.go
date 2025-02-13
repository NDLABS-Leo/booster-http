package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	chunkSize = 8 * 1024 * 1024 // Read 8MB per request
)

// HttpServer struct
type HttpServer struct{}

// getPieceContent handles content retrieval requests
func (s *HttpServer) getPieceContent(ctx context.Context, rootCid string, w http.ResponseWriter, dagScope string) error {
	// Construct the target CAR file URL
	url := fmt.Sprintf("http://202.77.20.108:51375/piece/%s", rootCid)

	log.Printf("[INFO] Processing request: CID=%s, dag-scope=%s", rootCid, dagScope)

	// If dag-scope is "block", return only the root block
	//if dagScope == "block" {
	//	log.Printf("[INFO] Returning only root block for CID: %s", rootCid)
	//	http.Error(w, "Root block retrieval not implemented", http.StatusNotImplemented)
	//	return nil
	//}

	client := &http.Client{}
	var start int64 = 0
	var end int64 = chunkSize - 1

	for {
		// Create HTTP request
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			log.Printf("[ERROR] Failed to create HTTP request: %v", err)
			return fmt.Errorf("failed to create HTTP request: %w", err)
		}

		// Set Range header
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

		log.Printf("[INFO] Sending request: URL=%s, Range=bytes=%d-%d", url, start, end)

		// Perform HTTP request
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("[ERROR] Failed to fetch content from remote server: %v", err)
			return fmt.Errorf("failed to fetch piece content from %s: %w", url, err)
		}
		defer resp.Body.Close()

		// Check response status code
		if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
			log.Printf("[ERROR] Unexpected status code: %d from %s", resp.StatusCode, url)
			return fmt.Errorf("unexpected status code %d from %s", resp.StatusCode, url)
		}

		log.Printf("[INFO] Successfully received response: Status=%d, Content-Length=%d", resp.StatusCode, resp.ContentLength)

		// Stream data to client
		startTime := time.Now()
		bytesWritten, err := io.Copy(w, resp.Body)
		elapsedTime := time.Since(startTime)

		if err != nil {
			log.Printf("[ERROR] Failed to stream data to client: %v", err)
			return fmt.Errorf("failed to stream data to client: %w", err)
		}

		log.Printf("[INFO] Successfully streamed %d bytes in %.2f seconds", bytesWritten, elapsedTime.Seconds())

		// Handle Range request
		contentRange := resp.Header.Get("Content-Range")
		if contentRange == "" || resp.ContentLength < chunkSize {
			log.Printf("[INFO] File transfer completed: CID=%s", rootCid)
			break
		}

		// Update Range for the next chunk
		start = end + 1
		end = start + chunkSize - 1
	}

	return nil
}

// handleRequest processes incoming HTTP requests
func handleRequest(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	log.Printf("[INFO] Received HTTP request: %s %s", r.Method, r.URL.Path)

	// Extract CID from the URL
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 3 {
		log.Printf("[ERROR] Invalid request path: %s", r.URL.Path)
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	rootCid := pathParts[2] // Extract CID as string

	// Get dag-scope parameter
	queryParams := r.URL.Query()
	dagScope := queryParams.Get("dag-scope")

	log.Printf("[INFO] Processing request: CID=%s, dag-scope=%s", rootCid, dagScope)

	server := &HttpServer{}
	ctx := r.Context()
	err := server.getPieceContent(ctx, rootCid, w, dagScope)
	if err != nil {
		log.Printf("[ERROR] Failed to retrieve CID: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	log.Printf("[INFO] Request completed: CID=%s, Duration=%.2f seconds", rootCid, time.Since(startTime).Seconds())
}

func main() {
	// Read port from environment variable
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // Default port
	}

	http.HandleFunc("/ipfs/", handleRequest)

	log.Printf("[INFO] Starting HTTP server on port %s...", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
