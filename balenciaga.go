package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

/**

request --> proxy -> pick a node

how to pick
- cycle through all nodes
- exclude dead node

stategies
- Round robin (all weights are equal)
- Weighted round robin (higher weight receives more requests )
- Least connections (useful when large number of persistent connections)
- IP hash (connects to the same server to get the same session)
- URL hash (good for caching)
- Least reponse Time (pick the fastest)
- Least bandwidth (pick the least traffic in Mbps)

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
	Weight       uint64
	mux          sync.RWMutex
}

func (n *Node) SetAlive(alive bool) {
	n.mux.Lock()
	n.Alive = alive
	n.mux.Unlock()
}

type NodePool struct {
	nodes        []*Node
	currentIndex uint64
	totalWeight  uint64
}

type Config struct {
	Addr    string
	Port    int
	Nodes   []*url.URL
	Weights []int
}

type LoadBalancer struct {
	Config Config
	pool   *NodePool
}

func (np *NodePool) AddNode(node *Node) {
	np.nodes = append(np.nodes, node)
	np.totalWeight += node.Weight
}

func (np *NodePool) GetNextNode() *Node {
	nextIdx := int(atomic.AddUint64(&np.currentIndex, uint64(1)) % uint64(len(np.nodes)))
	k := len(np.nodes) + nextIdx
	pick := rand.Intn(int(np.totalWeight))

	for i := nextIdx; i < k; i++ {
		idx := i % len(np.nodes)
		node := np.nodes[idx]

		pick -= int(node.Weight)
		if pick < 0 {
			if node.Alive {
				atomic.StoreUint64(&np.currentIndex, uint64(idx))
				return node
			}
		}
	}

	return nil
}

func checkNodeAlive(u *url.URL) bool {
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
		alive := checkNodeAlive(node.URL)
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
	var addr string
	var port int
	var nodeList string
	var weightList string

	flag.StringVar(&nodeList, "nodes", "", "server node URLs. use comma separated.")
	flag.StringVar(&weightList, "weights", "", "weights for the corresponding nodes. use comma separated.")
	flag.StringVar(&addr, "addr", "localhost", "load balancer address")
	flag.IntVar(&port, "port", 3030, "load balancer port")
	flag.Parse()

	if len(nodeList) == 0 {
		log.Fatal("Require url of nodes to be added to pool")
	}

	var urls []*url.URL
	for _, u := range strings.Split(nodeList, ",") {
		nodeUrl, err := url.Parse(u)
		if err != nil {
			log.Fatalf("error parsing node URL: %v", err)
		}
		urls = append(urls, nodeUrl)
	}

	var weights []int
	if len(weightList) == 0 {
		for range urls {
			weights = append(weights, 1)
		}
	} else {
		wlist := strings.Split(weightList, ",")
		if len(weightList) != 0 && len(urls) != len(wlist) {
			log.Fatal("Nodes and weights mismatch")
		}

		for _, w := range wlist {
			weight, err := strconv.Atoi(w)
			if err != nil {
				log.Fatalf("error converting weight to int: %v", err)
			}
			weights = append(weights, weight)
		}
	}

	return Config{
		Addr:    addr,
		Port:    port,
		Nodes:   urls,
		Weights: weights,
	}
}

func main() {

	var lb *LoadBalancer
	var nodePool = &NodePool{}

	config := NewConfig()

	for _, nodeUrl := range config.Nodes {
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
	fmt.Println("Started healthcheck.")

	fmt.Printf("Load balancer start. Listening at %s:%d", config.Addr, config.Port)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("fail to start server: %v", err)
	}

}
