package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/internal/admin"
	"github.com/cuprite-io/flux/types"
)

// Client defines the communication bridge between the Flux TUI cockpit and the engine.
type Client interface {
	GetStatus(ctx context.Context) (*StatusResponse, error)
	GetCircuits(ctx context.Context) ([]CircuitSummary, error)
	GetCircuit(ctx context.Context, id string) (*CircuitSummary, error)
	GetAutopilot(ctx context.Context) ([]AutopilotSession, error)
	GetAlertHistory(ctx context.Context, circuitID string, limit int) ([]AlertRecord, error)
	StreamAlerts(ctx context.Context) (<-chan AlertEvent, <-chan error, error)
	SubmitFeedback(ctx context.Context, req FeedbackRequest) (*FeedbackResponse, error)
	DryRunEval(ctx context.Context, req EvalRequest) (*EvalResponse, error)
	Close() error
}

// HTTPClient communicates with a remote Flux Engine Admin control plane over HTTP and SSE.
type HTTPClient struct {
	baseURL    string
	token      string
	httpClient *http.Client
	closeMu    sync.Mutex
	cancelFuncs []context.CancelFunc
}

// NewHTTPClient creates and validates an HTTPClient connected to the specified Admin endpoint.
func NewHTTPClient(endpoint string, token string) (*HTTPClient, error) {
	if endpoint == "" {
		return nil, errors.New("flux client: endpoint required")
	}

	target := strings.TrimSpace(endpoint)
	if !strings.Contains(target, "://") {
		target = "http://" + target
	}

	parsed, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("flux client: invalid endpoint URL: %w", err)
	}

	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil, fmt.Errorf("flux client: unsupported scheme %q; only http:// and https:// are supported", scheme)
	}

	baseURL := fmt.Sprintf("%s://%s", scheme, parsed.Host)
	if parsed.Path != "" && parsed.Path != "/" {
		baseURL += strings.TrimSuffix(parsed.Path, "/")
	}

	return &HTTPClient{
		baseURL: baseURL,
		token:   token,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}, nil
}

// BaseURL returns the normalized target base URL.
func (c *HTTPClient) BaseURL() string {
	return c.baseURL
}

func (c *HTTPClient) newRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	reqURL := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, method, reqURL, body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", "flux-cli/"+flux.Version)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return req, nil
}

func (c *HTTPClient) doJSON(ctx context.Context, method, path string, sendData, recvData any) error {
	var bodyReader io.Reader
	if sendData != nil {
		b, err := json.Marshal(sendData)
		if err != nil {
			return fmt.Errorf("flux client: failed to marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := c.newRequest(ctx, method, path, bodyReader)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("flux client: request failed: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil {
		return fmt.Errorf("flux client: failed to read response: %w", err)
	}

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("authentication required: %s", string(bodyBytes))
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	if recvData != nil && len(bodyBytes) > 0 {
		if err := json.Unmarshal(bodyBytes, recvData); err != nil {
			return fmt.Errorf("flux client: failed to decode response: %w", err)
		}
	}

	return nil
}

// GetStatus queries the engine health and cluster membership via GET /api/v1/status.
func (c *HTTPClient) GetStatus(ctx context.Context) (*StatusResponse, error) {
	var resp StatusResponse
	if err := c.doJSON(ctx, http.MethodGet, "/api/v1/status", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// GetCircuits retrieves all deployed circuits via GET /api/v1/circuits.
func (c *HTTPClient) GetCircuits(ctx context.Context) ([]CircuitSummary, error) {
	var summaries []CircuitSummary
	if err := c.doJSON(ctx, http.MethodGet, "/api/v1/circuits", nil, &summaries); err != nil {
		return nil, err
	}
	return summaries, nil
}

// GetCircuit retrieves a single deployed circuit by ID via GET /api/v1/circuits?id=<id>.
func (c *HTTPClient) GetCircuit(ctx context.Context, id string) (*CircuitSummary, error) {
	var summary CircuitSummary
	path := fmt.Sprintf("/api/v1/circuits?id=%s", url.QueryEscape(id))
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &summary); err != nil {
		return nil, err
	}
	return &summary, nil
}

// GetAutopilot retrieves active and historical Autopilot sessions via GET /api/v1/autopilot.
func (c *HTTPClient) GetAutopilot(ctx context.Context) ([]AutopilotSession, error) {
	var sessions []AutopilotSession
	if err := c.doJSON(ctx, http.MethodGet, "/api/v1/autopilot", nil, &sessions); err != nil {
		return nil, err
	}
	return sessions, nil
}

// GetAlertHistory retrieves historical alerts from CacheBackend via GET /api/v1/alerts/history.
func (c *HTTPClient) GetAlertHistory(ctx context.Context, circuitID string, limit int) ([]AlertRecord, error) {
	path := fmt.Sprintf("/api/v1/alerts/history?limit=%d", limit)
	if circuitID != "" {
		path += "&circuit_id=" + url.QueryEscape(circuitID)
	}

	var rawRecords []struct {
		ID        string         `json:"id"`
		CircuitID string         `json:"circuit_id"`
		NodeName  string         `json:"node_name"`
		SinkName  string         `json:"sink_name"`
		Condition string         `json:"condition,omitempty"`
		Payload   map[string]any `json:"payload,omitempty"`
		Feedback  *struct {
			Classification string    `json:"classification"`
			Reason         string    `json:"reason,omitempty"`
			Author         string    `json:"author,omitempty"`
			UpdatedAt      time.Time `json:"updated_at"`
		} `json:"feedback,omitempty"`
	}

	if err := c.doJSON(ctx, http.MethodGet, path, nil, &rawRecords); err != nil {
		return nil, err
	}

	records := make([]AlertRecord, len(rawRecords))
	for i, r := range rawRecords {
		var fb *FeedbackInfo
		if r.Feedback != nil {
			fb = &FeedbackInfo{
				Classification: r.Feedback.Classification,
				Reason:         r.Feedback.Reason,
				Author:         r.Feedback.Author,
				UpdatedAt:      r.Feedback.UpdatedAt,
			}
		}
		records[i] = AlertRecord{
			ID:        r.ID,
			CircuitID: r.CircuitID,
			NodeName:  r.NodeName,
			SinkName:  r.SinkName,
			Condition: r.Condition,
			Payload:   r.Payload,
			Feedback:  fb,
		}
	}

	return records, nil
}

// StreamAlerts opens a Server-Sent Events (SSE) connection to /api/v1/alerts/stream.
func (c *HTTPClient) StreamAlerts(ctx context.Context) (<-chan AlertEvent, <-chan error, error) {
	req, err := c.newRequest(ctx, http.MethodGet, "/api/v1/alerts/stream", nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Accept", "text/event-stream")

	// Use an un-timed HTTP transport for streaming SSE
	streamClient := &http.Client{
		Timeout: 0,
	}

	resp, err := streamClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("flux client: failed to connect to alert stream: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		resp.Body.Close()
		return nil, nil, fmt.Errorf("flux client: alert stream HTTP %d: %s", resp.StatusCode, string(body))
	}

	eventCh := make(chan AlertEvent, 64)
	errCh := make(chan error, 1)

	streamCtx, cancel := context.WithCancel(ctx)
	c.closeMu.Lock()
	c.cancelFuncs = append(c.cancelFuncs, cancel)
	c.closeMu.Unlock()

	go func() {
		defer resp.Body.Close()
		defer close(eventCh)
		defer close(errCh)

		reader := bufio.NewReader(resp.Body)
		var currentEvent string
		var dataBuffer strings.Builder

		for {
			select {
			case <-streamCtx.Done():
				return
			default:
			}

			line, err := reader.ReadString('\n')
			if err != nil {
				if streamCtx.Err() == nil && err != io.EOF {
					errCh <- err
				}
				return
			}

			line = strings.TrimRight(line, "\r\n")

			if line == "" {
				// Event boundary
				if dataBuffer.Len() > 0 {
					var evt AlertEvent
					if err := json.Unmarshal([]byte(dataBuffer.String()), &evt); err == nil {
						select {
						case eventCh <- evt:
						case <-streamCtx.Done():
							return
						}
					}
					dataBuffer.Reset()
					currentEvent = ""
				}
				continue
			}

			if strings.HasPrefix(line, ":") {
				// SSE comment / ping -> ignore
				continue
			}

			if strings.HasPrefix(line, "event:") {
				currentEvent = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
				_ = currentEvent
				continue
			}

			if strings.HasPrefix(line, "data:") {
				dataBuffer.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
				continue
			}
		}
	}()

	return eventCh, errCh, nil
}

// SubmitFeedback submits operator classification on an alert via POST /api/v1/feedback.
func (c *HTTPClient) SubmitFeedback(ctx context.Context, req FeedbackRequest) (*FeedbackResponse, error) {
	var resp FeedbackResponse
	if err := c.doJSON(ctx, http.MethodPost, "/api/v1/feedback", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// DryRunEval executes a dry-run DAG evaluation via POST /api/v1/eval.
func (c *HTTPClient) DryRunEval(ctx context.Context, req EvalRequest) (*EvalResponse, error) {
	var resp EvalResponse
	if err := c.doJSON(ctx, http.MethodPost, "/api/v1/eval", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Close gracefully terminates all active SSE streams.
func (c *HTTPClient) Close() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()

	for _, cancel := range c.cancelFuncs {
		cancel()
	}
	c.cancelFuncs = nil
	return nil
}

// LocalClient provides an in-memory client implementation for Standalone Mode.
// In standalone mode, Flux runs completely offline with zero network calls.
type LocalClient struct {
	engine   *flux.Engine
	owned    bool
	closedMu sync.RWMutex
	closed   bool
}

// NewLocalClient creates a LocalClient backed by an existing or fresh in-memory Flux engine.
func NewLocalClient(eng ...*flux.Engine) (*LocalClient, error) {
	if len(eng) > 0 && eng[0] != nil {
		return &LocalClient{
			engine: eng[0],
			owned:  false,
		}, nil
	}

	// Create standalone in-memory engine
	newEng, err := flux.New(
		flux.WithHistory(true),
	)
	if err != nil {
		return nil, fmt.Errorf("flux client: failed to create local standalone engine: %w", err)
	}

	return &LocalClient{
		engine: newEng,
		owned:  true,
	}, nil
}

// Engine returns the underlying local Flux Engine.
func (l *LocalClient) Engine() *flux.Engine {
	return l.engine
}

// GetStatus returns the standalone status.
func (l *LocalClient) GetStatus(ctx context.Context) (*StatusResponse, error) {
	circuits, _ := l.engine.ListCircuits(ctx)
	autopilotSessions := l.engine.GetAutopilotSessions(ctx)

	return &StatusResponse{
		Service:         "flux-admin",
		Version:         l.engine.Version(),
		Status:          "standalone",
		NodeID:          "standalone",
		LeaderID:        "standalone",
		IsLeader:        true,
		ClusterNodes:    1,
		ActiveNodes:     []string{"standalone"},
		ActiveCircuits:  len(circuits),
		ActiveAutopilot: len(autopilotSessions),
		Timestamp:       time.Now().UTC(),
	}, nil
}

// GetCircuits retrieves circuits registered in the local engine.
func (l *LocalClient) GetCircuits(ctx context.Context) ([]CircuitSummary, error) {
	circuits, err := l.engine.ListCircuits(ctx)
	if err != nil {
		return nil, err
	}

	summaries := make([]CircuitSummary, len(circuits))
	for i, c := range circuits {
		var cond string
		if c.Root != nil {
			cond = c.Root.Condition
		}
		summaries[i] = CircuitSummary{
			ID:            c.ID,
			Tags:          c.Tags,
			RootCondition: cond,
			NodeCount:     countNodes(c.Root),
			Status:        "LIVE",
			Revision:      c.Version,
			CreatedAt:     c.CreatedAt,
			Definition:    c,
		}
	}
	return summaries, nil
}

// GetCircuit retrieves a single circuit by ID from the local engine.
func (l *LocalClient) GetCircuit(ctx context.Context, id string) (*CircuitSummary, error) {
	c, err := l.engine.GetCircuit(ctx, id)
	if err != nil {
		return nil, err
	}
	var cond string
	if c.Root != nil {
		cond = c.Root.Condition
	}
	return &CircuitSummary{
		ID:            c.ID,
		Tags:          c.Tags,
		RootCondition: cond,
		NodeCount:     countNodes(c.Root),
		Status:        "LIVE",
		Revision:      c.Version,
		CreatedAt:     c.CreatedAt,
		Definition:    c,
	}, nil
}

// GetAutopilot retrieves active sessions from the local engine.
func (l *LocalClient) GetAutopilot(ctx context.Context) ([]AutopilotSession, error) {
	sessions := l.engine.GetAutopilotSessions(ctx)
	result := make([]AutopilotSession, len(sessions))
	for i, s := range sessions {
		var stInfo *AutopilotStagingInfo
		if s.StagingResult != nil {
			stInfo = &AutopilotStagingInfo{
				TotalEvents:     s.StagingResult.TotalEvents,
				FiringEvents:    s.StagingResult.FiringEvents,
				ErrorEvents:     s.StagingResult.ErrorEvents,
				ObservedRate:    s.StagingResult.ObservedRate,
				Status:          s.StagingResult.Status,
				RejectionReason: s.StagingResult.RejectionReason,
				Promoted:        s.StagingResult.Promoted,
			}
		}
		result[i] = AutopilotSession{
			SessionID:      s.SessionID,
			Prompt:         s.Prompt,
			Status:         s.Status,
			Tags:           s.Tags,
			ActiveCircuits: s.ActiveCircuits,
			StagingResult:  stInfo,
			LastError:      s.LastError,
		}
	}
	return result, nil
}

// GetAlertHistory retrieves historical alerts from the local engine's history store.
func (l *LocalClient) GetAlertHistory(ctx context.Context, circuitID string, limit int) ([]AlertRecord, error) {
	records, err := l.engine.GetAlertHistory(ctx, circuitID, limit)
	if err != nil {
		return nil, err
	}

	result := make([]AlertRecord, len(records))
	for i, r := range records {
		var fb *FeedbackInfo
		if r.Feedback != nil {
			fb = &FeedbackInfo{
				Classification: r.Feedback.Classification,
				Reason:         r.Feedback.Reason,
				Author:         r.Feedback.Author,
				UpdatedAt:      r.Feedback.UpdatedAt,
			}
		}
		result[i] = AlertRecord{
			ID:        r.ID,
			CircuitID: r.CircuitID,
			NodeName:  r.NodeName,
			SinkName:  r.SinkName,
			Condition: r.Condition,
			Payload:   r.Payload,
			Feedback:  fb,
		}
	}
	return result, nil
}

// StreamAlerts hooks into the local engine's alert observer for real-time local streaming.
func (l *LocalClient) StreamAlerts(ctx context.Context) (<-chan AlertEvent, <-chan error, error) {
	eventCh := make(chan AlertEvent, 64)
	errCh := make(chan error, 1)

	cancelObs := l.engine.RegisterAlertObserver(func(evt admin.AlertEvent) {
		select {
		case eventCh <- AlertEvent{
			ID:        evt.ID,
			CircuitID: evt.CircuitID,
			NodeName:  evt.NodeName,
			SinkName:  evt.SinkName,
			Condition: evt.Condition,
			Payload:   evt.Payload,
			Timestamp: evt.Timestamp,
		}:
		default:
		}
	})

	go func() {
		<-ctx.Done()
		cancelObs()
		close(eventCh)
		close(errCh)
	}()

	return eventCh, errCh, nil
}

// SubmitFeedback records alert feedback in the local engine.
func (l *LocalClient) SubmitFeedback(ctx context.Context, req FeedbackRequest) (*FeedbackResponse, error) {
	fb := flux.AlertFeedback{
		Classification: req.FeedbackType,
		Reason:         req.Notes,
		Author:         req.Operator,
		UpdatedAt:      time.Now().UTC(),
	}

	if err := l.engine.RecordAlertFeedback(ctx, req.CircuitID, req.AlertID, fb); err != nil {
		return nil, err
	}

	return &FeedbackResponse{
		Status:    "recorded",
		AlertID:   req.AlertID,
		Timestamp: fb.UpdatedAt,
	}, nil
}

// DryRunEval evaluates a dry-run payload directly in the local engine VM.
func (l *LocalClient) DryRunEval(ctx context.Context, req EvalRequest) (*EvalResponse, error) {
	adminReq := admin.EvalRequest{
		Circuit:   req.Circuit,
		CircuitID: req.CircuitID,
		Tags:      req.Tags,
		Payload:   req.Payload,
		Context:   req.Context,
		EntityID:  req.EntityID,
	}

	res, err := l.engine.DryRunEval(ctx, adminReq)
	if err != nil {
		return nil, err
	}

	return &EvalResponse{
		Passed:           res.Passed,
		ExecutedCircuits: res.ExecutedCircuits,
		ReturnedData:     res.ReturnedData,
		ShadowFirings:    res.ShadowFirings,
		DurationMicro:    res.DurationMicro,
		Errors:           res.Errors,
	}, nil
}

// Close terminates the local engine if owned.
func (l *LocalClient) Close() error {
	l.closedMu.Lock()
	defer l.closedMu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true

	if l.owned && l.engine != nil {
		return l.engine.Close()
	}
	return nil
}

func countNodes(n *types.Node) int {
	if n == nil {
		return 0
	}
	total := 1
	for _, child := range n.Children {
		total += countNodes(child)
	}
	return total
}
