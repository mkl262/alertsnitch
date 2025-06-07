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
	defaultTimeout   = 5 * time.Second
	lokiAPIPath      = "loki/api/v1"
	minTimeout       = 1 * time.Second
	maxTimeout       = 30 * time.Second
	maxErrorBodySize = 64 * 1024 // 64KB 錯誤響應大小限制
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
	Url            *url.URL
	Auth           AuthConfig
	RequestTimeout time.Duration
}

type AuthConfig struct {
	TenantID          string
	BasicAuthUser     string
	BasicAuthPassword string
}

func (c *LokiConfig) Validate() error {
	if c.Url == nil {
		return errors.New("URL is required")
	}

	if c.Url.Scheme != "http" && c.Url.Scheme != "https" {
		return fmt.Errorf("unsupported URL scheme: %s, only http and https are supported", c.Url.Scheme)
	}

	if c.RequestTimeout == 0 {
		// 零值情況下使用預設值，不視為錯誤
		c.RequestTimeout = defaultTimeout
	} else if c.RequestTimeout < minTimeout {
		return fmt.Errorf("request timeout too short: %v, minimum is %v", c.RequestTimeout, minTimeout)
	} else if c.RequestTimeout > maxTimeout {
		return fmt.Errorf("request timeout too long: %v, maximum is %v", c.RequestTimeout, maxTimeout)
	}

	// 驗證認證配置的一致性
	if err := c.Auth.Validate(); err != nil {
		return fmt.Errorf("auth config validation failed: %w", err)
	}

	return nil
}

func (a AuthConfig) Validate() error {
	// 如果設置了基本認證用戶名，則密碼也必須設置
	if a.BasicAuthUser != "" && a.BasicAuthPassword == "" {
		return errors.New("basic auth password is required when basic auth user is set")
	}

	// 如果設置了基本認證密碼，則用戶名也必須設置
	if a.BasicAuthPassword != "" && a.BasicAuthUser == "" {
		return errors.New("basic auth user is required when basic auth password is set")
	}

	return nil
}

type lokiClient struct {
	client *http.Client // 使用指針，http.Client 本身是併行安全的
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

	// 簡單直接的配置創建
	cfg := LokiConfig{
		Url: endpoint,
		Auth: AuthConfig{
			TenantID:          args.Options["tenant_id"],
			BasicAuthUser:     args.Options["basic_auth_user"],
			BasicAuthPassword: args.Options["basic_auth_password"],
		},
		RequestTimeout: defaultTimeout,
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid Loki configuration: %w", err)
	}

	// 創建優化的 HTTP 客戶端
	httpClient := &http.Client{
		Timeout: cfg.RequestTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	client := &lokiClient{
		client: httpClient,
		cfg:    cfg,
	}

	if err := client.Ping(); err != nil {
		return nil, err
	}

	return client, nil
}

// Ping implements Storer interface
func (c *lokiClient) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.RequestTimeout)
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
		logrus.Debugf("Setting basic auth for user: %s", c.cfg.Auth.BasicAuthUser)
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
		// 限制錯誤響應大小以防止 OOM 攻擊
		limitedReader := io.LimitReader(res.Body, maxErrorBodySize)
		byt, readErr := io.ReadAll(limitedReader)
		if readErr != nil {
			logrus.Errorf("Failed to read error response body: %v", readErr)
			return fmt.Errorf("received non-2XX response from Loki (status: %d) and failed to read response body: %w", res.StatusCode, readErr)
		}

		if len(byt) > 0 {
			logrus.Errorf("Loki error response - Status: %d, Body: %s", res.StatusCode, string(byt))
			return fmt.Errorf("Loki returned error (status: %d): %s", res.StatusCode, string(byt))
		} else {
			logrus.Errorf("Loki error response - Status: %d, Empty body", res.StatusCode)
			return fmt.Errorf("Loki returned error with empty body (status: %d)", res.StatusCode)
		}
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

// 簡單的分組函數
func groupAlertsByStatus(alerts []internal.Alert) map[string][]internal.Alert {
	alertsByStatus := make(map[string][]internal.Alert)
	for _, alert := range alerts {
		alertsByStatus[alert.Status] = append(alertsByStatus[alert.Status], alert)
	}
	return alertsByStatus
}

func createStreamForStatus(status string, alerts []internal.Alert, data *internal.AlertGroup, baseLabels map[string]string) (stream, error) {
	streamLabels := cloneLabels(baseLabels)
	streamLabels["alert_status"] = status

	s := stream{
		Stream: streamLabels,
		Values: make([]row, 0, len(alerts)),
	}

	now := time.Now()
	for _, alert := range alerts {
		// 簡單直接的 JSON 序列化
		flattenGroup := internal.FlattenAlertGroup{
			Version:           data.Version,
			GroupKey:          data.GroupKey,
			Receiver:          data.Receiver,
			Status:            data.Status,
			Alert:             alert,
			GroupLabels:       data.GroupLabels,
			CommonLabels:      data.CommonLabels,
			CommonAnnotations: data.CommonAnnotations,
			ExternalURL:       data.ExternalURL,
		}

		jsonData, err := json.Marshal(flattenGroup)
		if err != nil {
			return stream{}, fmt.Errorf("error marshalling FlattenAlertGroup: %w", err)
		}

		// 使用警報的實際時間，如果沒有則使用當前時間
		timestamp := now
		if !alert.StartsAt.IsZero() {
			timestamp = alert.StartsAt
		}

		s.Values = append(s.Values, row{
			At:  timestamp,
			Val: string(jsonData),
		})
	}

	return s, nil
}

func (c *lokiClient) dataToStream(data *internal.AlertGroup, extraLabels map[string]string) ([]stream, error) {
	if len(data.Alerts) == 0 {
		return nil, fmt.Errorf("no alerts to process")
	}

	alertsByStatus := groupAlertsByStatus(data.Alerts)
	baseLabels := buildStreamLabels(data, extraLabels)

	streams := make([]stream, 0, len(alertsByStatus))

	for status, alerts := range alertsByStatus {
		s, err := createStreamForStatus(status, alerts, data, baseLabels)
		if err != nil {
			return nil, err
		}
		streams = append(streams, s)
	}

	return streams, nil
}

// 允許的標籤集合 - 避免無意義的恆等映射
var allowedLabels = map[string]bool{
	"severity":  true,
	"priority":  true,
	"level":     true,
	"instance":  true,
	"job":       true,
	"team":      true,
	"env":       true,
	"service":   true,
	"pod":       true,
	"namespace": true,
	"node":      true,
	"container": true,
	"cluster":   true,
}

// buildStreamLabels 構建流標籤 - 簡單直接的函數
func buildStreamLabels(data *internal.AlertGroup, extraLabels map[string]string) map[string]string {
	streamLabels := make(map[string]string, len(extraLabels)+len(data.CommonLabels)+len(data.GroupLabels)+2)

	// 添加額外標籤
	for key, value := range extraLabels {
		streamLabels[key] = value
	}

	// 添加通用標籤
	for commonLabel, commonValue := range data.CommonLabels {
		if allowedLabels[commonLabel] {
			streamLabels[commonLabel] = commonValue
		}
	}

	// 添加群組標籤
	for groupLabel, groupValue := range data.GroupLabels {
		if allowedLabels[groupLabel] {
			streamLabels[groupLabel] = groupValue
		}
	}

	// 添加基本標籤
	streamLabels["receiver"] = data.Receiver
	streamLabels["status"] = data.Status

	return streamLabels
}
