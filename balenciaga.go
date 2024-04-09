package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

/**

request --> proxy -> pick a node

stategies
- Round robin
- Weighted round robin (todo maybe)
- Least connections (todo maybe)

how to pick
- cycle through all nodes
- exclude dead node

*/

type contextKey string

const (
	Retry   contextKey = "retry"
	Attempt contextKey = "attempt"
)

type Node struct {
	URL          *url.URL
	Alive        bool
	ReverseProxy *httputil.ReverseProxy
	mux          sync.RWMutex
}

func (n *Node) SetAlive(alive bool) {
	n.mux.Lock()
	n.Alive = alive
	n.mux.Unlock()
}

type NodePool struct {
	nodes   []*Node
	current uint64
}

type Config struct {
	Addr     string
	Port     int
	NodeList string
}

type LoadBalancer struct {
	Config Config
	pool   *NodePool
}

func (np *NodePool) NextIndex() int {
	return int(atomic.AddUint64(&np.current, uint64(1)) % uint64(len(np.nodes)))

}

func (np *NodePool) AddNode(node *Node) {
	np.nodes = append(np.nodes, node)
}

func (np *NodePool) GetNextNode() *Node {
	nextIdx := np.NextIndex()
	k := len(np.nodes) + nextIdx

	for i := nextIdx; i < k; i++ {
		idx := i % len(np.nodes)
		node := np.nodes[idx]
		if node.Alive {
			atomic.StoreUint64(&np.current, uint64(idx))
			return node
		}

	}
	return nil
}

func isBackendAlive(u *url.URL) bool {
	timeout := 2 * time.Second
	conn, err := net.DialTimeout("tcp", u.Host, timeout)
	if err != nil {
		log.Println("Site unreachable, error: ", err)
		return false
	}
	defer conn.Close()
	return true
}

func (np *NodePool) HealthCheck() {
	for _, node := range np.nodes {
		alive := isBackendAlive(node.URL)
		node.SetAlive(alive)
		log.Printf("%s alive status: %v", node.URL.Host, alive)
	}
}

func (np *NodePool) SetNodeAlive(nodeUrl *url.URL, alive bool) {
	for _, n := range np.nodes {
		if n.URL == nodeUrl {
			n.SetAlive(alive)
			return
		}
	}

}

func healthCheck(lb *LoadBalancer) {
	t := time.NewTicker(time.Minute * 1)
	for range t.C {
		lb.pool.HealthCheck()
	}
}

func (lb *LoadBalancer) handleIncoming(w http.ResponseWriter, r *http.Request) {
	attempts := GetAttempsFromContext(r)
	if attempts > 5 {
		log.Printf("%s(%s) max attempts reached", r.RemoteAddr, r.URL.Path)
		http.Error(w, "service not avaible", http.StatusServiceUnavailable)
		return
	}

	node := lb.pool.GetNextNode()
	if node != nil {

		node.ReverseProxy.ServeHTTP(w, r)
		return
	}
	http.Error(w, "service not avaible", http.StatusServiceUnavailable)

}

func GetRetriesFromContext(r *http.Request) int {
	if retries, ok := r.Context().Value(Retry).(int); !ok {
		return retries
	}
	return 0
}

func GetAttempsFromContext(r *http.Request) int {
	if attempts, ok := r.Context().Value(Attempt).(int); ok {
		return attempts
	}
	return 0
}

func NewConfig() Config {
	var nodes string
	var addr string
	var port int

	flag.StringVar(&nodes, "nodes", "", "server node URLs. use comma separated.")
	flag.StringVar(&addr, "addr", "localhost", "load balancer port")
	flag.IntVar(&port, "port", 3030, "load balancer port")
	flag.Parse()

	if len(nodes) == 0 {
		log.Fatal("Require url of nodes to be added to pool")
	}

	return Config{
		Addr:     "",
		Port:     port,
		NodeList: nodes,
	}
}

func main() {

	var lb *LoadBalancer
	var nodePool = &NodePool{}

	config := NewConfig()

	urls := strings.Split(config.NodeList, ",")

	for _, u := range urls {
		nodeUrl, err := url.Parse(u)
		if err != nil {
			log.Fatalf("error parsing node URL: %v", err)
		}

		proxy := httputil.NewSingleHostReverseProxy(nodeUrl)
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, e error) {
			log.Printf("[%s] %s\n", nodeUrl.Host, e.Error())

			retries := GetRetriesFromContext(r)

			if retries < 3 {
				time.AfterFunc(1*time.Second, func() {
					ctx := context.WithValue(r.Context(), Retry, retries+1)
					proxy.ServeHTTP(w, r.WithContext(ctx))
				})
			}

			nodePool.SetNodeAlive(nodeUrl, false)

			attempts := GetAttempsFromContext(r)
			log.Printf("%s(%s) Attempting retry %d\n", r.RemoteAddr, r.URL.Path, attempts)
			ctx := context.WithValue(r.Context(), Attempt, attempts+1)
			lb.handleIncoming(w, r.WithContext(ctx))

		}

		n := &Node{
			URL:          nodeUrl,
			Alive:        true,
			ReverseProxy: proxy,
		}

		nodePool.AddNode(n)

	}

	lb = &LoadBalancer{
		pool:   nodePool,
		Config: config,
	}

	server := http.Server{
		Addr:    fmt.Sprintf("%s:%d", config.Addr, config.Port),
		Handler: http.HandlerFunc(lb.handleIncoming),
	}

	go healthCheck(lb)

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("fail to start server: %v", err)
	}

}
