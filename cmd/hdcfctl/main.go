package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
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
	case "jobs":
		jobs(os.Args[2:])
	case "workers":
		workers(os.Args[2:])
	case "abort":
		abort(os.Args[2:])
	case "replay":
		replay(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Println("hdcfctl <command>")
	fmt.Println("  submit --command <command> [--arg A --arg B] [--url=http://localhost:8080] [--token=dev-token]")
	fmt.Println("  jobs list [--status=STATUS] [--worker-id=<id>] [--url=http://localhost:8080] [--token=dev-token]")
	fmt.Println("  jobs describe <job_id> [--url=http://localhost:8080] [--token=dev-token]")
	fmt.Println("  workers list [--url=http://localhost:8080] [--token=dev-token]")
	fmt.Println("  abort --job-id=<id> [--reason=<text>] [--url=http://localhost:8080] [--token=dev-token]")
	fmt.Println("  abort --worker-id=<id> [--url=http://localhost:8080] [--token=dev-token]")
	fmt.Println("  replay --worker-id=<id> [--current-job-id=<id>] [--completed-jobs-file=path] [--url=http://localhost:8080] [--token=dev-token]")
}

func submit(args []string) {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	urlStr := fs.String("url", getenvCLI("HDCF_ADDR", "http://localhost:8080"), "control plane url")
	token := fs.String("token", getenvCLI("HDCF_API_TOKEN", "dev-token"), "api token")
	command := fs.String("command", "", "command")
	argsCSV := fs.String("args", "", "comma separated args")
	workingDir := fs.String("working-dir", "", "working directory")
	maxAttempts := fs.Int("max-attempts", 3, "max attempts")
	priority := fs.Int("priority", 0, "job priority (higher values run first)")
	scheduledAt := fs.Int64("scheduled-at", 0, "when job is eligible to run (unix seconds), 0 means now")
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
		Priority:    *priority,
		ScheduledAt: *scheduledAt,
		TimeoutMs:   *timeoutMs,
	}

	resp := hdcf.CreateJobResponse{}
	doJSONRequest(http.MethodPost, trimURL(*urlStr)+"/jobs", *token, request, &resp, "submit")
	fmt.Printf("job_id=%s status=%s\n", resp.JobID, resp.Status)
}

func jobs(args []string) {
	if len(args) < 1 {
		log.Fatal("jobs command requires a subcommand: list, describe")
	}
	switch args[0] {
	case "list":
		listJobs(args[1:])
	case "describe":
		describeJob(args[1:])
	default:
		log.Fatalf("unknown jobs subcommand: %s (expected list or describe)", args[0])
	}
}

func listJobs(args []string) {
	fs := flag.NewFlagSet("jobs-list", flag.ExitOnError)
	urlStr := fs.String("url", getenvCLI("HDCF_ADDR", "http://localhost:8080"), "control plane url")
	token := fs.String("token", getenvCLI("HDCF_API_TOKEN", "dev-token"), "api token")
	status := fs.String("status", "", "filter by status")
	workerID := fs.String("worker-id", "", "filter by worker")
	fs.Parse(args)

	endpoint := trimURL(*urlStr) + "/jobs"
	q := url.Values{}
	if strings.TrimSpace(*status) != "" {
		q.Set("status", strings.TrimSpace(*status))
	}
	if strings.TrimSpace(*workerID) != "" {
		q.Set("worker_id", strings.TrimSpace(*workerID))
	}
	if encoded := q.Encode(); encoded != "" {
		endpoint = endpoint + "?" + encoded
	}

	var out []hdcf.JobRead
	doJSONRequest(http.MethodGet, endpoint, *token, nil, &out, "jobs list")
	printJSON(out)
}

func describeJob(args []string) {
	fs := flag.NewFlagSet("jobs-describe", flag.ExitOnError)
	urlStr := fs.String("url", getenvCLI("HDCF_ADDR", "http://localhost:8080"), "control plane url")
	token := fs.String("token", getenvCLI("HDCF_API_TOKEN", "dev-token"), "api token")
	fs.Parse(args)

	if fs.NArg() != 1 {
		log.Fatal("jobs describe requires exactly one positional argument: <job_id>")
	}
	jobID := strings.TrimSpace(fs.Arg(0))
	if jobID == "" {
		log.Fatal("job_id is required")
	}

	var out hdcf.JobRead
	doJSONRequest(http.MethodGet, trimURL(*urlStr)+"/jobs/"+url.PathEscape(jobID), *token, nil, &out, "jobs describe")
	printJSON(out)
}

func workers(args []string) {
	if len(args) < 1 {
		log.Fatal("workers command requires a subcommand: list")
	}
	switch args[0] {
	case "list":
		listWorkers(args[1:])
	default:
		log.Fatalf("unknown workers subcommand: %s (expected list)", args[0])
	}
}

func listWorkers(args []string) {
	fs := flag.NewFlagSet("workers-list", flag.ExitOnError)
	urlStr := fs.String("url", getenvCLI("HDCF_ADDR", "http://localhost:8080"), "control plane url")
	token := fs.String("token", getenvCLI("HDCF_API_TOKEN", "dev-token"), "api token")
	fs.Parse(args)

	var out []hdcf.WorkerRead
	doJSONRequest(http.MethodGet, trimURL(*urlStr)+"/workers", *token, nil, &out, "workers list")
	printJSON(out)
}

func abort(args []string) {
	fs := flag.NewFlagSet("abort", flag.ExitOnError)
	urlStr := fs.String("url", getenvCLI("HDCF_ADDR", "http://localhost:8080"), "control plane url")
	token := fs.String("token", getenvCLI("HDCF_API_TOKEN", "dev-token"), "api token")
	jobID := fs.String("job-id", "", "abort by job id")
	workerID := fs.String("worker-id", "", "abort jobs currently held by worker id")
	reason := fs.String("reason", "", "optional reason")
	fs.Parse(args)

	req := hdcf.AbortRequest{
		JobID:    strings.TrimSpace(*jobID),
		WorkerID: strings.TrimSpace(*workerID),
		Reason:   strings.TrimSpace(*reason),
	}
	if req.JobID == "" && req.WorkerID == "" {
		log.Fatal("abort requires --job-id or --worker-id")
	}

	var out map[string]interface{}
	doJSONRequest(http.MethodPost, trimURL(*urlStr)+"/abort", *token, req, &out, "abort")
	printJSON(out)
}

func replay(args []string) {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	urlStr := fs.String("url", getenvCLI("HDCF_ADDR", "http://localhost:8080"), "control plane url")
	token := fs.String("token", getenvCLI("HDCF_API_TOKEN", "dev-token"), "api token")
	workerID := fs.String("worker-id", "", "worker id")
	currentJobID := fs.String("current-job-id", "", "current in-flight job id")
	completedJobsPath := fs.String("completed-jobs-file", "", "path to JSON file with []ReconnectCompletedJob")
	fs.Parse(args)

	req := hdcf.WorkerReconnectRequest{
		WorkerID: strings.TrimSpace(*workerID),
	}
	if req.WorkerID == "" {
		log.Fatal("replay requires --worker-id")
	}
	if strings.TrimSpace(*currentJobID) != "" {
		value := strings.TrimSpace(*currentJobID)
		req.CurrentJobID = &value
	}
	if strings.TrimSpace(*completedJobsPath) != "" {
		var completedJobs []hdcf.ReconnectCompletedJob
		if err := readJSONFile(*completedJobsPath, &completedJobs); err != nil {
			log.Fatalf("read completed jobs file: %v", err)
		}
		req.CompletedJobs = completedJobs
	}

	var out struct {
		Status  string `json:"status"`
		Actions []hdcf.ReconnectAction `json:"actions"`
	}
	doJSONRequest(http.MethodPost, trimURL(*urlStr)+"/reconnect", *token, req, &out, "replay")
	printJSON(out)
}

func doJSONRequest(method, endpoint, token string, payload any, out any, action string) {
	var body io.Reader
	var err error
	if payload != nil {
		bodyBytes, err := json.Marshal(payload)
		if err != nil {
			log.Fatalf("marshal payload: %v", err)
		}
		body = bytes.NewBuffer(bodyBytes)
	}

	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		log.Fatalf("build request: %v", err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set(cliAuthHeader, token)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("%s request failed: %v", action, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		log.Fatalf("%s failed: status=%s body=%s", action, resp.Status, strings.TrimSpace(string(msg)))
	}

	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			log.Fatalf("invalid response: %v", err)
		}
	}
}

func printJSON(value any) {
	out, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		log.Fatalf("encode output: %v", err)
	}
	fmt.Println(string(out))
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

func trimURL(raw string) string {
	return strings.TrimRight(strings.TrimSpace(raw), "/")
}

func getenvCLI(name, fallback string) string {
	if strings.TrimSpace(os.Getenv(name)) != "" {
		return os.Getenv(name)
	}
	return fallback
}

func readJSONFile(path string, out any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}
