package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type App struct {
	podName         string
	podIP           string
	namespace       string
	port            string
	peerServiceFQDN string
	callInterval    time.Duration
	client          *http.Client
	rng             *rand.Rand

	mu               sync.RWMutex
	lastPeerCall     map[string]any
	incomingRequests int
	outgoingRequests int
}

func main() {
	app := &App{
		podName:         getEnv("POD_NAME", mustHostname()),
		podIP:           getEnv("POD_IP", ""),
		namespace:       getEnv("POD_NAMESPACE", "default"),
		port:            getEnv("PORT", "8080"),
		peerServiceFQDN: getEnv("PEER_SERVICE_FQDN", "example-service-peers.default.svc.cluster.local"),
		callInterval:    mustDuration(getEnv("CALL_INTERVAL", "5s")),
		client: &http.Client{
			Timeout: mustDuration(getEnv("REQUEST_TIMEOUT", "2s")),
		},
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", app.handleRoot)
	mux.HandleFunc("/ping", app.handlePing)
	mux.HandleFunc("/peers", app.handlePeers)
	mux.HandleFunc("/call-peer", app.handleCallPeer)
	mux.HandleFunc("/healthz", app.handleHealth)

	server := &http.Server{
		Addr:              ":" + app.port,
		Handler:           loggingMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go app.backgroundCaller()

	log.Printf("example-service started pod=%s ip=%s namespace=%s peerService=%s port=%s interval=%s",
		app.podName, app.podIP, app.namespace, app.peerServiceFQDN, app.port, app.callInterval)

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server failed: %v", err)
	}
}

func (a *App) handleRoot(w http.ResponseWriter, r *http.Request) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	respondJSON(w, http.StatusOK, map[string]any{
		"service":          "example-service",
		"podName":          a.podName,
		"podIP":            a.podIP,
		"namespace":        a.namespace,
		"peerServiceFQDN":  a.peerServiceFQDN,
		"incomingRequests": a.incomingRequests,
		"outgoingRequests": a.outgoingRequests,
		"lastPeerCall":     a.lastPeerCall,
		"timestamp":        time.Now().UTC().Format(time.RFC3339),
	})
}

func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *App) handlePing(w http.ResponseWriter, r *http.Request) {
	from := r.URL.Query().Get("from")
	sourceIP := clientIP(r)

	a.mu.Lock()
	a.incomingRequests++
	a.mu.Unlock()

	respondJSON(w, http.StatusOK, map[string]any{
		"message":   "pong",
		"handledBy": a.podName,
		"podIP":     a.podIP,
		"from":      from,
		"sourceIP":  sourceIP,
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func (a *App) handlePeers(w http.ResponseWriter, r *http.Request) {
	peers, err := a.discoverPeers(r.Context())
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]any{
			"error": err.Error(),
		})
		return
	}

	respondJSON(w, http.StatusOK, map[string]any{
		"podName": a.podName,
		"podIP":   a.podIP,
		"peers":   peers,
		"count":   len(peers),
	})
}

func (a *App) handleCallPeer(w http.ResponseWriter, r *http.Request) {
	result, err := a.callRandomPeer(r.Context())
	if err != nil {
		respondJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": err.Error(),
		})
		return
	}
	respondJSON(w, http.StatusOK, result)
}

func (a *App) backgroundCaller() {
	ticker := time.NewTicker(a.callInterval)
	defer ticker.Stop()

	time.Sleep(2 * time.Second)

	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), a.client.Timeout)
		result, err := a.callRandomPeer(ctx)
		cancel()
		if err != nil {
			log.Printf("peer call failed: %v", err)
			continue
		}
		payload, _ := json.Marshal(result)
		log.Printf("peer call ok: %s", string(payload))
	}
}

func (a *App) callRandomPeer(ctx context.Context) (map[string]any, error) {
	peers, err := a.discoverPeers(ctx)
	if err != nil {
		return nil, err
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("no peer replicas found")
	}

	a.mu.Lock()
	peerIP := peers[a.rng.Intn(len(peers))]
	a.mu.Unlock()

	url := fmt.Sprintf("http://%s:%s/ping?from=%s", hostForURL(peerIP), a.port, a.podName)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var remote map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&remote); err != nil {
		return nil, err
	}

	result := map[string]any{
		"caller":     a.podName,
		"callerIP":   a.podIP,
		"targetIP":   peerIP,
		"statusCode": resp.StatusCode,
		"response":   remote,
		"timestamp":  time.Now().UTC().Format(time.RFC3339Nano),
	}

	a.mu.Lock()
	a.outgoingRequests++
	a.lastPeerCall = result
	a.mu.Unlock()

	return result, nil
}

func (a *App) discoverPeers(ctx context.Context) ([]string, error) {
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, a.peerServiceFQDN)
	if err != nil {
		return nil, fmt.Errorf("dns lookup failed for %s: %w", a.peerServiceFQDN, err)
	}

	seen := map[string]struct{}{}
	peers := make([]string, 0, len(ips))
	for _, ip := range ips {
		s := ip.IP.String()
		if s == "" || s == a.podIP {
			continue
		}
		if _, exists := seen[s]; exists {
			continue
		}
		seen[s] = struct{}{}
		peers = append(peers, s)
	}

	sort.Strings(peers)
	return peers, nil
}

func respondJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s from=%s took=%s", r.Method, r.URL.Path, clientIP(r), time.Since(started))
	})
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func hostForURL(ip string) string {
	parsed := net.ParseIP(ip)
	if parsed != nil && strings.Contains(ip, ":") {
		return "[" + ip + "]"
	}
	return ip
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func mustHostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown-pod"
	}
	return h
}

func mustDuration(v string) time.Duration {
	d, err := time.ParseDuration(v)
	if err == nil {
		return d
	}

	secs, convErr := strconv.Atoi(v)
	if convErr == nil {
		return time.Duration(secs) * time.Second
	}

	log.Fatalf("invalid duration %q: %v", v, err)
	return 0
}
