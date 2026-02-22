package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"hdcf/internal/hdcf"
)

const cliAuthHeader = "X-API-Token"

func main() {
	if len(os.Args) < 2 {
		usage()
		return
	}
	switch os.Args[1] {
	case "submit":
		submit(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Println("hdcfctl submit --command <command> [--arg A --arg B] [--url=http://localhost:8080] [--token=dev-token]")
}

func submit(args []string) {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	url := fs.String("url", getenvCLI("HDCF_ADDR", "http://localhost:8080"), "control plane url")
	token := fs.String("token", getenvCLI("HDCF_API_TOKEN", "dev-token"), "api token")
	command := fs.String("command", "", "command")
	argsCSV := fs.String("args", "", "comma separated args")
	workingDir := fs.String("working-dir", "", "working directory")
	maxAttempts := fs.Int("max-attempts", 3, "max attempts")
	timeoutMs := fs.Int64("timeout-ms", 0, "command timeout in ms")
	fs.Parse(args)

	if strings.TrimSpace(*command) == "" {
		log.Fatal("command is required")
	}
	request := hdcf.CreateJobRequest{
		Command:     *command,
		Args:        splitArgs(*argsCSV),
		WorkingDir:  *workingDir,
		MaxAttempts: *maxAttempts,
		TimeoutMs:   *timeoutMs,
	}
	payload, err := json.Marshal(request)
	if err != nil {
		log.Fatalf("marshal payload: %v", err)
	}

	endpoint := strings.TrimRight(*url, "/") + "/jobs"
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBuffer(payload))
	if err != nil {
		log.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(cliAuthHeader, *token)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		log.Fatalf("status=%s body=%s", resp.Status, strings.TrimSpace(string(msg)))
	}
	out := hdcf.CreateJobResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Fatalf("invalid response: %v", err)
	}
	fmt.Printf("job_id=%s status=%s\n", out.JobID, out.Status)
}

func splitArgs(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func getenvCLI(name, fallback string) string {
	if strings.TrimSpace(os.Getenv(name)) != "" {
		return os.Getenv(name)
	}
	return fallback
}
