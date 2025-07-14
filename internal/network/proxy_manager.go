package network

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tracertea/certflow/internal/config"
)

type proxyState int

const (
	healthy proxyState = iota
	unhealthy
	maxCooldown = 30 * time.Minute
)

// Proxy now tracks the number of certificates it has downloaded.
type Proxy struct {
	URL              *url.URL
	state            proxyState
	failures         int
	concurrencySlots chan struct{}
	client           *http.Client
	CertsDownloaded  atomic.Uint64 // <-- NEW: Tracks certs per proxy.
}

// ProxyStatus includes the total cert count for UI display.
type ProxyStatus struct {
	URL             string
	State           string // "Healthy", "Unhealthy"
	Failures        int
	CertsDownloaded uint64 // <-- NEW: To pass total to UI.
}

type ProxyManager struct {
	sync.RWMutex
	proxies []*Proxy
	cfg     *config.Config
	logger  *slog.Logger
	logURL  string
}

// NewProxyManager remains the same structurally.
func NewProxyManager(logURL string, concurrencyPerProxy int, cfg *config.Config, logger *slog.Logger) (*ProxyManager, error) {
	mgr := &ProxyManager{
		logURL: logURL,
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

// GetClient remains the same.
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
		time.Sleep(2 * time.Second)
		return nil, fmt.Errorf("no healthy proxies available")
	}

	chosen, _, recvOK := reflect.Select(cases)
	if !recvOK {
		return nil, fmt.Errorf("proxy channel was unexpectedly closed")
	}
	return activeProxies[chosen], nil
}

// ReleaseClient now accepts a certCount to update proxy stats on success.
func (m *ProxyManager) ReleaseClient(p *Proxy, success bool, certCount uint64) {
	defer func() {
		p.concurrencySlots <- struct{}{}
	}()

	if success {
		m.Lock()
		p.failures = 0                   // Reset failures on success.
		p.CertsDownloaded.Add(certCount) // <-- NEW: Add to proxy's total.
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
		go m.probeProxy(p, m.cfg.ProxyCooldown)
	}
}

// probeProxy remains the same.
func (m *ProxyManager) probeProxy(p *Proxy, currentCooldown time.Duration) {
	time.Sleep(currentCooldown)

	m.logger.Info("Probing unhealthy proxy.", "proxy", p.URL)

	healthCheckURL, err := url.JoinPath(m.logURL, "ct/v1/get-sth")
	if err != nil {
		m.logger.Error("Could not create health check URL, proxy remains unhealthy.", "error", err)
		return
	}

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
		m.logger.Info("Proxy probe successful. Returning to healthy pool.", "proxy", p.URL)
		p.state = healthy
		p.failures = 0
	} else {
		nextCooldown := currentCooldown * 2
		if nextCooldown > maxCooldown {
			nextCooldown = maxCooldown
		}
		m.logger.Warn("Proxy probe failed. Retrying after longer cooldown.", "proxy", p.URL, "next_cooldown", nextCooldown)
		go m.probeProxy(p, nextCooldown)
	}
}

// GetStatuses now populates the cert count for each proxy.
func (m *ProxyManager) GetStatuses() []ProxyStatus {
	m.RLock()
	defer m.RUnlock()

	statuses := make([]ProxyStatus, len(m.proxies))
	for i, p := range m.proxies {
		status := ProxyStatus{
			Failures:        p.failures,
			CertsDownloaded: p.CertsDownloaded.Load(), // <-- NEW: Load the atomic value.
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
