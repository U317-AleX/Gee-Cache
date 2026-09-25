package geecache

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/U317-AleX/Gee-KVS/src/consistenthash"
	pb "github.com/U317-AleX/Gee-KVS/src/geecachepb"
	"github.com/segmentio/encoding/proto"
)

const defaultBasePath = "/_geecache/"
const defaultReplicas = 50

// HTTPPool implements PeerPicker for a pool of HTTP peers.
// it's the server instance of a peer actually
type HTTPPool struct {
	// this peer's base URL, e.g. "http://example.net:8000"
	self       string                 // self is the address of this peer
	basePath   string                 // basePath is the prefix of a peer's URL
	mu         sync.Mutex             // guards peers and httpGetters
	peers      *consistenthash.Map    // Map offers interface Get to pick a peer for a key
	httpGetter map[string]*httpGetter // map peer names to it's PeerGetter (httpGetter)
}

// NewHTTPPool initializes an HTTP pool of peers.
func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{
		self:     self,
		basePath: defaultBasePath,
	}
}

// Log info with server name
func (p *HTTPPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

// ServeHTTP handle all http requests
func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	p.Log("%s %s", r.Method, r.URL.Path)
	// /<basePath>/<groupname>/<key> required
	parts := strings.SplitN(r.URL.Path[len(p.basePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	groupName := parts[0]
	key := parts[1]

	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}

	view, err := group.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	body, err := proto.Marshal(&pb.Response{Value: view.ByteSlice()})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(body)
}

// Set updates the pool's list of peers.
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(defaultReplicas, nil)
	// add peers to consistenthash Map
	p.peers.Add(peers...)
	p.httpGetter = make(map[string]*httpGetter, len(peers))
	// initialize new httpGetters matched with peers
	for _, peer := range peers {
		p.httpGetter[peer] = &httpGetter{
			baseURL: peer + p.basePath,
		}
	}
}

// PickPeer picks a peer according to the key
func (p *HTTPPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// return nil, false while peer == p.self
	// to avoid call storm (call loop between group.Get and group.load)
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.Log("Pick peer %s", peer)
		return p.httpGetter[peer], true
	}
	return nil, false
}

// httpGetter is an implement of PeerGetter
type httpGetter struct {
	baseURL string
}

// Get gets value from a certain peer
func (h *httpGetter) Get(in *pb.Request, out *pb.Response) error {
	// /<basePath>/<groupname>/<key> required
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(in.GetGroup()),
		url.QueryEscape(in.GetKey()),
	)
	res, err := http.Get(u)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}

	bytes, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}

	if err = proto.Unmarshal(bytes, out); err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}
	return nil
}

var _ PeerGetter = (*httpGetter)(nil)
