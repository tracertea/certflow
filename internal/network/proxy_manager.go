package network

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"time"

	"github.com/tracertea/certflow/internal/config"
)

// proxyState and Proxy structs remain the same
type proxyState int

const (
	healthy proxyState = iota
	unhealthy
	maxCooldown = 30 * time.Minute // Maximum cooldown to prevent infinite backoff
)

type Proxy struct {
	URL              *url.URL
	state            proxyState
	failures         int
	concurrencySlots chan struct{}
	client           *http.Client
}

type ProxyStatus struct {
	URL      string
	State    string // "Healthy", "Unhealthy"
	Failures int
}

type ProxyManager struct {
	sync.RWMutex
	proxies []*Proxy
	cfg     *config.Config
	logger  *slog.Logger
	logURL  string // <-- NEW: Store log URL for health checks
}

// NewProxyManager initializes the proxy pool.
// It now accepts the logURL for health checking.
func NewProxyManager(logURL string, concurrencyPerProxy int, cfg *config.Config, logger *slog.Logger) (*ProxyManager, error) {
	mgr := &ProxyManager{
		logURL: logURL, // <-- NEW
		cfg:    cfg,
		logger: logger.With("component", "proxy_manager"),
	}

	createSlots := func(size int) chan struct{} {
		slots := make(chan struct{}, size)
		for i := 0; i < size; i++ {
			slots <- struct{}{}
		}
		return slots
	}

	if len(cfg.Proxies) == 0 {
		logger.Info("No proxies provided. Running in direct mode.")
		proxy := &Proxy{
			URL:              nil,
			state:            healthy,
			concurrencySlots: createSlots(concurrencyPerProxy),
			client:           &http.Client{Timeout: 45 * time.Second},
		}
		mgr.proxies = append(mgr.proxies, proxy)
		return mgr, nil
	}

	logger.Info("Initializing proxy pool.", "count", len(cfg.Proxies))
	for _, proxyStr := range cfg.Proxies {
		if proxyStr == "" {
			continue
		}
		proxyURL, err := url.Parse(proxyStr)
		if err != nil {
			logger.Warn("Could not parse proxy URL, skipping.", "url", proxyStr, "error", err)
			continue
		}
		proxy := &Proxy{
			URL:              proxyURL,
			state:            healthy,
			concurrencySlots: createSlots(concurrencyPerProxy),
			client: &http.Client{
				Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)},
				Timeout:   45 * time.Second,
			},
		}
		mgr.proxies = append(mgr.proxies, proxy)
	}

	if len(mgr.proxies) == 0 {
		return nil, fmt.Errorf("no valid proxies could be initialized from the provided list")
	}

	return mgr, nil
}

// GetClient blocks until a concurrency slot from any healthy proxy is available.
func (m *ProxyManager) GetClient() (*Proxy, error) {
	m.RLock()
	cases := make([]reflect.SelectCase, 0)
	activeProxies := make([]*Proxy, 0)
	for _, p := range m.proxies {
		if p.state == healthy {
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(p.concurrencySlots),
			})
			activeProxies = append(activeProxies, p)
		}
	}
	m.RUnlock()

	if len(cases) == 0 {
		// This is a temporary state, so a short sleep is fine. The loop will retry.
		time.Sleep(2 * time.Second)
		return nil, fmt.Errorf("no healthy proxies available")
	}

	chosen, _, recvOK := reflect.Select(cases)
	if !recvOK {
		return nil, fmt.Errorf("proxy channel was unexpectedly closed")
	}
	return activeProxies[chosen], nil
}

// ReleaseClient returns a client to the pool and updates its health.
func (m *ProxyManager) ReleaseClient(p *Proxy, success bool) {
	defer func() {
		p.concurrencySlots <- struct{}{}
	}()

	if success {
		m.Lock()
		p.failures = 0 // Reset failures on success
		m.Unlock()
		return
	}

	// Handle failure
	m.Lock()
	defer m.Unlock()

	p.failures++
	m.logger.Warn("Proxy reported failure.", "proxy", p.URL, "failures", p.failures)

	if p.state == healthy && p.failures >= m.cfg.ProxyFailures {
		p.state = unhealthy
		m.logger.Error("Proxy has been marked as UNHEALTHY.", "proxy", p.URL, "cooldown", m.cfg.ProxyCooldown)
		// Start the health-checking process with the initial cooldown.
		go m.probeProxy(p, m.cfg.ProxyCooldown)
	}
}

// probeProxy periodically checks an unhealthy proxy and attempts to revive it.
// It implements an exponential backoff strategy.
func (m *ProxyManager) probeProxy(p *Proxy, currentCooldown time.Duration) {
	// Wait for the cooldown period.
	time.Sleep(currentCooldown)

	m.logger.Info("Probing unhealthy proxy.", "proxy", p.URL)

	// Build the health check URL (get-sth is a good, lightweight choice).
	healthCheckURL, err := url.JoinPath(m.logURL, "ct/v1/get-sth")
	if err != nil {
		m.logger.Error("Could not create health check URL, proxy remains unhealthy.", "error", err)
		// This is a permanent config error, so we just leave the proxy as unhealthy.
		return
	}

	// Perform the health check using the proxy's own client.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", healthCheckURL, nil)
	resp, err := p.client.Do(req)

	probeSuccess := err == nil && resp.StatusCode == http.StatusOK
	if err == nil {
		resp.Body.Close()
	}

	m.Lock()
	defer m.Unlock()

	if probeSuccess {
		// It's alive! Return it to the pool.
		m.logger.Info("Proxy probe successful. Returning to healthy pool.", "proxy", p.URL)
		p.state = healthy
		p.failures = 0
	} else {
		// Probe failed. Increase cooldown and schedule the next probe.
		nextCooldown := currentCooldown * 2
		if nextCooldown > maxCooldown {
			nextCooldown = maxCooldown
		}
		m.logger.Warn("Proxy probe failed. Retrying after longer cooldown.", "proxy", p.URL, "next_cooldown", nextCooldown)
		go m.probeProxy(p, nextCooldown) // Recursively call with longer backoff
	}
}

// GetStatuses remains the same
func (m *ProxyManager) GetStatuses() []ProxyStatus {
	m.RLock()
	defer m.RUnlock()

	statuses := make([]ProxyStatus, len(m.proxies))
	for i, p := range m.proxies {
		status := ProxyStatus{
			Failures: p.failures,
		}
		if p.URL != nil {
			status.URL = p.URL.String()
		} else {
			status.URL = "Direct Connection"
		}

		switch p.state {
		case healthy:
			status.State = "Healthy"
		case unhealthy:
			status.State = "Unhealthy (Cooldown)"
		}
		statuses[i] = status
	}
	return statuses
}
