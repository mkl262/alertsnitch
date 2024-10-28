package db

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/yakshaving.art/alertsnitch/internal"
	"gitlab.com/yakshaving.art/alertsnitch/internal/metrics"
	"gitlab.com/yakshaving.art/alertsnitch/internal/middleware"
)

const (
	defaultTimeout = 5 * time.Second
	lokiAPIPath    = "loki/api/v1"
)

// loki entry 的格式
type stream struct {
	Stream map[string]string `json:"stream"`
	Values []row             `json:"values"`
}
type row struct {
	At  time.Time
	Val string
}

func (r row) MarshalJSON() ([]byte, error) {
	return json.Marshal([]string{fmt.Sprintf("%d", r.At.UnixNano()), r.Val})
}

type payload struct {
	Streams []stream `json:"streams"`
}

type Config interface {
	Validate() error
}

type LokiConfig struct {
	Url              *url.URL
	Auth             AuthConfig
	RequestTimeout   time.Duration
}

type AuthConfig struct {
	TenantID         string
	BasicAuthUser    string
	BasicAuthPassword string
}

func (c LokiConfig) Validate() error {
	if c.Url == nil {
		return errors.New("URL is required")
	}
	return nil
}

type lokiClient struct {
	client http.Client
	cfg    LokiConfig
}

func connectLoki(args ConnectionArgs) (*lokiClient, error) {
	// TODO Rename DSN to Url
	if args.DSN == "" {
		return nil, fmt.Errorf("empty Loki endpoint provided, can't connect to Loki database")
	}

	endpoint, err := url.Parse(args.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Loki endpoint: %s", err)
	}

	client := &lokiClient{
		cfg: LokiConfig{
			Url: endpoint,
			Auth: AuthConfig{
				TenantID:         args.Options["tenant_id"],
				BasicAuthUser:    args.Options["basic_auth_user"],
				BasicAuthPassword: args.Options["basic_auth_password"],
			},
			RequestTimeout: defaultTimeout,
		},
	}

	if err := client.Ping(); err != nil {
		return nil, err
	}

	return client, nil
}

// Ping implements Storer interface
func (c *lokiClient) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err := c.pingContext(ctx); err != nil {
		metrics.DatabaseUp.Set(0)
		logrus.Debugf("Failed to ping Loki: %s", err)
		return err
	}
	
	metrics.DatabaseUp.Set(1)
	logrus.Debugf("Pinged Loki successfully")
	return nil
}

func (c *lokiClient) pingContext(ctx context.Context) error {
	uri := c.cfg.Url.JoinPath(lokiAPIPath, "/labels")
	req, err := http.NewRequestWithContext(ctx, "GET", uri.String(), nil)
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	c.setAuthAndTenantHeaders(req)

	res, err := c.client.Do(req)
	if res != nil {
		defer func() {
			if err := res.Body.Close(); err != nil {
				logrus.Errorf("failed to close response body: %s", err)
			}
		}()
	}
	if err != nil || (res.StatusCode < 200 || res.StatusCode >= 300) {
		return fmt.Errorf("failed to ping Loki endpoint: %s", err)
	}

	return nil
}

func (c *lokiClient) setAuthAndTenantHeaders(req *http.Request) {
	if c.cfg.Auth.TenantID != "" {
		req.Header.Add("X-Scope-OrgID", c.cfg.Auth.TenantID)
		logrus.Debugf("Setting tenant ID: %s", c.cfg.Auth.TenantID)
	}

	if c.cfg.Auth.BasicAuthUser != "" && c.cfg.Auth.BasicAuthPassword != "" {
		req.SetBasicAuth(c.cfg.Auth.BasicAuthUser, c.cfg.Auth.BasicAuthPassword)
		logrus.Debugf("Setting basic auth user: %s, password: %s", c.cfg.Auth.BasicAuthUser, c.cfg.Auth.BasicAuthPassword)
	}

	req.Header.Set("Content-Type", "application/json")
}

// Save implements Storer interface
func (c *lokiClient) Save(ctx context.Context, data *internal.AlertGroup) error {
	queryParams := middleware.GetQueryParameters(ctx)
	logrus.Debugf("Query parameters: %v", queryParams)

	streams, err := c.dataToStream(data, queryParams)
	if err != nil {
		return fmt.Errorf("error converting data to stream: %w", err)
	}
	
	payload := payload{
		Streams: streams,
	}
	payloadBytes, err := json.Marshal(payload)
	logrus.Debugf("Loki request payload: %v", string(payloadBytes))

	if err != nil {
		return fmt.Errorf("error marshalling Loki request: %w", err)
	}

	uri := c.cfg.Url.JoinPath(lokiAPIPath, "push")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uri.String(), bytes.NewBuffer(payloadBytes))
	if err != nil {
		return fmt.Errorf("error creating Loki push request: %w", err)
	}

	c.setAuthAndTenantHeaders(req)

	res, err := c.client.Do(req)
	if res != nil {
		defer func() {
			if err := res.Body.Close(); err != nil {
				logrus.Errorf("failed to close response body: %s", err)
			}
		}()
	}
	if err != nil {
		return fmt.Errorf("error pushing data to Loki: %w", err)
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		byt, _ := io.ReadAll(res.Body)
		if len(byt) > 0 {
			logrus.Error("Error response from Loki ", "response", string(byt), "status", res.StatusCode)
		} else {
			logrus.Error("Error response from Loki with an empty body ", "status", res.StatusCode)
		}
		return fmt.Errorf("received a non-2XX response from loki, status: %d", res.StatusCode)
	}

	logrus.Debugf("Save data to Loki: %v", fmt.Sprintf("%+v", data))
	return nil
}

// CheckModel implements Storer interface
func (c *lokiClient) CheckModel() error {
	return nil
}

func cloneLabels(labels map[string]string) map[string]string {
	clone := make(map[string]string, len(labels))
	for k, v := range labels {
		clone[k] = v
	}
	return clone
}

func (c *lokiClient) dataToStream(data *internal.AlertGroup, extraLabels map[string]string) ([]stream, error) {
	alertsByStatus := make(map[string][]internal.Alert)
	for _, alert := range data.Alerts {
		alertsByStatus[alert.Status] = append(alertsByStatus[alert.Status], alert)
	}
	
	baseLabels := c.getStreamLabels(data, extraLabels)
	
	var streams []stream
	for status, alerts := range alertsByStatus {
		streamLabels := cloneLabels(baseLabels)
		streamLabels["alert_status"] = status
		
		s := stream{
			Stream: streamLabels,
			Values: make([]row, 0, len(alerts)),
		}
		
		for _, alert := range alerts {
			flattenGroup := internal.FlattenAlertGroup{
				Version:           data.Version,
				GroupKey:         data.GroupKey,
				Receiver:         data.Receiver,
				Status:           data.Status,
				Alert:            alert,
				GroupLabels:      data.GroupLabels,
				CommonLabels:     data.CommonLabels,
				CommonAnnotations: data.CommonAnnotations,
				ExternalURL:      data.ExternalURL,
			}
			
			jsonData, err := json.Marshal(flattenGroup)
			if err != nil {
				return nil, fmt.Errorf("error marshalling FlattenAlertGroup: %w", err)
			}
			
			s.Values = append(s.Values, row{
				At:  time.Now(),
				Val: string(jsonData),
			})
		}
		
		streams = append(streams, s)
	}
	
	return streams, nil
}

var additionalLabels = map[string]string{
	"severity":  "severity",
	"priority":  "priority",
	"level":     "level",
	"instance":  "instance",
	"job":       "job",
	"team":      "team",
	"env":       "env",
	"service":   "service",
	"pod":       "pod",
	"namespace": "namespace",
	"node":      "node",
	"container": "container",
	"cluster":   "cluster",
}

func (c *lokiClient) getStreamLabels(data *internal.AlertGroup, extraLabels map[string]string) map[string]string {
	streamLabels := stream{
		Stream: make(map[string]string),
		Values: make([]row, 0),
	}.Stream
	
	for extraKey, extraValue := range extraLabels {
		streamLabels[extraKey] = extraValue
	}

	for commonLabel, commonValue := range data.CommonLabels {
		if label, ok := additionalLabels[commonLabel]; ok {
			streamLabels[label] = commonValue
		}
	}

	for groupLabel, groupValue := range data.GroupLabels {
		if label, ok := additionalLabels[groupLabel]; ok {
			streamLabels[label] = groupValue
		}
	}


	// streamLabels["app"] = "alertsnitch" // TBD if we need use label "app"
	streamLabels["receiver"] = data.Receiver
	streamLabels["status"] = data.Status

	return streamLabels
}
