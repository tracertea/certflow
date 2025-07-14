package network

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"time"

	"github.com/tracertea/certflow/internal/config"
)

// ... (proxyState and Proxy structs remain the same) ...
type proxyState int

const (
	healthy proxyState = iota
	unhealthy
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
}

// NewProxyManager initializes the proxy pool.
func NewProxyManager(concurrencyPerProxy int, cfg *config.Config, logger *slog.Logger) (*ProxyManager, error) {
	mgr := &ProxyManager{
		cfg:    cfg,
		logger: logger.With("component", "proxy_manager"),
	}

	// This helper function creates a channel and fills it with initial slots.
	createSlots := func(size int) chan struct{} {
		slots := make(chan struct{}, size)
		for i := 0; i < size; i++ {
			slots <- struct{}{} // Add the initial "available" slots.
		}
		return slots
	}

	if len(cfg.Proxies) == 0 {
		logger.Info("No proxies provided. Running in direct mode.")
		proxy := &Proxy{
			URL:   nil,
			state: healthy,
			// CORRECTED: Create and prime the slots channel.
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
			URL:   proxyURL,
			state: healthy,
			// CORRECTED: Create and prime the slots channel for each proxy.
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
	// Taking the slot from the channel is now the first step.
	// This is much cleaner and less prone to race conditions.

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
		time.Sleep(1 * time.Second) // Wait for a proxy to potentially come back online
		return nil, fmt.Errorf("no healthy proxies available")
	}

	chosen, _, recvOK := reflect.Select(cases)
	if !recvOK {
		// This can happen if a channel is closed, which shouldn't happen in our case, but is good to guard against.
		return nil, fmt.Errorf("proxy channel was unexpectedly closed")
	}

	// We have successfully "taken" a slot.
	return activeProxies[chosen], nil
}

// ReleaseClient returns a client to the pool and updates its health.
func (m *ProxyManager) ReleaseClient(p *Proxy, success bool) {
	// First, always return the slot to the channel so another worker can use it.
	defer func() {
		p.concurrencySlots <- struct{}{}
	}()

	if success {
		m.Lock()
		p.failures = 0
		m.Unlock()
		return
	}

	// Handle failure
	m.Lock()
	p.failures++
	m.logger.Warn("Proxy reported failure.", "proxy", p.URL, "failures", p.failures)
	if p.failures >= m.cfg.ProxyFailures {
		p.state = unhealthy
		m.logger.Error("Proxy has been marked as UNHEALTHY.", "proxy", p.URL, "cooldown", m.cfg.ProxyCooldown)
		go m.cooldown(p)
	}
	m.Unlock()
}

// cooldown waits for the cooldown period and then attempts to revive the proxy.
func (m *ProxyManager) cooldown(p *Proxy) {
	time.Sleep(m.cfg.ProxyCooldown)

	m.Lock()
	defer m.Unlock()

	p.state = healthy
	p.failures = 0
	m.logger.Info("Proxy has been returned to the healthy pool after cooldown.", "proxy", p.URL)
}

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
