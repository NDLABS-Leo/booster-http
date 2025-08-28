package main

import (
	"booster-http/idxprov"
	"booster-http/lp2pimpl"
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/filecoin-project/go-address"
	lotusclient "github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/ipfs/go-cid"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipld/go-car/v2/index"
	"github.com/ipni/go-libipni/maurl"
	"github.com/ipni/go-libipni/metadata"
	providers "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/engine"
	"github.com/ipni/index-provider/engine/xproviders"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	cryptos "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	chunkSize = 8 * 1024 * 1024 // Read 8MB per request
)

// 放在文件顶部其他 import/const 之后
var globalCfg *Config

// HttpServer struct
type HttpServer struct{}

func (s *HttpServer) getPieceContentPiece(ctx context.Context, rootCid string, baseURL string, w http.ResponseWriter) error {
	// Construct the target CAR file URL
	url := fmt.Sprintf("%s/piece/%s", strings.TrimRight(baseURL, "/"), rootCid)

	//url := fmt.Sprintf("http://203.160.84.158:51375/piece/%s", rootCid)

	log.Printf("[INFO] Processing request: CID=%s", rootCid)

	client := &http.Client{Timeout: 0} // 交由 ctx 控制生命周期
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("fetch upstream: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("unexpected status %d from %s", resp.StatusCode, url)
	}

	// 透传上游头
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		w.Header().Set("Content-Length", cl)
	}
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		w.Header().Set("Content-Range", cr)
	}

	// 流式复制
	buf := make([]byte, 1<<20) // 1MB
	flusher, _ := w.(http.Flusher)
	var total int64
	for {
		select {
		case <-ctx.Done():
			log.Printf("[INFO] client canceled piece stream, CID=%s, bytes=%d", rootCid, total)
			return ctx.Err()
		default:
		}
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			wn, werr := w.Write(buf[:n])
			total += int64(wn)
			if flusher != nil {
				flusher.Flush()
			}
			if werr != nil {
				return fmt.Errorf("write to client: %w", werr)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return fmt.Errorf("upstream read: %w", rerr)
		}
	}
	log.Printf("[INFO] piece stream done, CID=%s, bytes=%d", rootCid, total)
	return nil
}

func (s *HttpServer) getPieceContentRoot(
	ctx context.Context,
	rootCid string,
	baseURL string,
	w http.ResponseWriter,
	dagScope string,
) error {
	// 目标 URL: {baseURL}/ipfs/{cid}[?dag-scope=...]
	base := strings.TrimRight(baseURL, "/")
	url := fmt.Sprintf("%s/ipfs/%s", base, rootCid)
	if dagScope != "" {
		// 仅拼接你提到的参数；如有更多 query，可用 url.Values 组合
		url = url + "?dag-scope=" + dagScope
	}

	log.Printf("[INFO] Proxying: %s", url)

	// 长连接、禁用整体超时；依赖 ctx 控制生命周期
	client := &http.Client{Timeout: 0}

	// 禁止自动解压，避免 Content-Length 与内容不一致
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("fetch upstream: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		// 将上游错误往下游透传更合理
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return fmt.Errorf("unexpected status %d from %s", resp.StatusCode, url)
	}

	// —— 透传关键响应头 —— //
	// Content-Type
	if v := resp.Header.Get("Content-Type"); v != "" {
		w.Header().Set("Content-Type", v)
	} else {
		// 不知道时给个通用值
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	// Content-Encoding（很关键：gzip/identity 与内容匹配）
	if v := resp.Header.Get("Content-Encoding"); v != "" {
		w.Header().Set("Content-Encoding", v)
	}
	// Content-Length：只有当上游声明了长度且我们没有改变实体（未解压）时才安全透传
	if resp.ContentLength >= 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", resp.ContentLength))
	}
	// Content-Range（如果是范围响应）
	if v := resp.Header.Get("Content-Range"); v != "" {
		w.Header().Set("Content-Range", v)
	}

	// 如果你一定想让浏览器弹下载框，可以加上这行；
	// 但注意：若加 Content-Disposition 而不加 Content-Length 也没问题（走 chunked）
	// w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s.car", rootCid))

	// 将上游状态码写回（200 或 206）
	w.WriteHeader(resp.StatusCode)

	// —— 流式复制 —— //
	buf := make([]byte, 1<<20) // 1MB buffer
	flusher, _ := w.(http.Flusher)
	var total int64
	for {
		select {
		case <-ctx.Done():
			log.Printf("[INFO] client canceled, written=%d bytes", total)
			return ctx.Err()
		default:
		}
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			wn, werr := w.Write(buf[:n])
			total += int64(wn)
			if flusher != nil {
				flusher.Flush()
			}
			if werr != nil {
				return fmt.Errorf("write downstream: %w", werr)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return fmt.Errorf("read upstream: %w", rerr)
		}
	}

	log.Printf("[INFO] proxy finished, bytes=%d", total)
	return nil
}

// handleRequest processes incoming HTTP requests
func handleRequestRoot(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	log.Printf("[INFO] Received HTTP request: %s %s", r.Method, r.URL.Path)

	// Extract CID from the URL
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 3 {
		log.Printf("[ERROR] Invalid request path: %s", r.URL.Path)
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	rootCid := pathParts[2] // Extract CID as string

	// Get dag-scope parameter
	queryParams := r.URL.Query()
	dagScope := queryParams.Get("dag-scope")

	log.Printf("[INFO] Processing request: CID=%s, dag-scope=%s", rootCid, dagScope)

	// 直接从全局配置拿 baseURL
	if globalCfg == nil || strings.TrimSpace(globalCfg.UpstreamBaseUrl) == "" {
		http.Error(w, "upstream baseURL not configured", http.StatusInternalServerError)
		return
	}
	baseURL := strings.TrimRight(globalCfg.UpstreamBaseUrl, "/")

	server := &HttpServer{}
	ctx := r.Context()
	err := server.getPieceContentRoot(ctx, rootCid, baseURL, w, dagScope)
	if err != nil {
		log.Printf("[ERROR] Failed to retrieve CID: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	log.Printf("[INFO] Request completed: CID=%s, Duration=%.2f seconds", rootCid, time.Since(startTime).Seconds())
}

func handleRequestPiece(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	log.Printf("[INFO] Received HTTP request: %s %s", r.Method, r.URL.Path)

	// Extract CID from the URL
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 3 {
		log.Printf("[ERROR] Invalid request path: %s", r.URL.Path)
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	rootCid := pathParts[2] // Extract CID as string

	log.Printf("[INFO] Processing request: CID=%s", rootCid)

	// 直接从全局配置拿 baseURL
	if globalCfg == nil || strings.TrimSpace(globalCfg.UpstreamBaseUrl) == "" {
		http.Error(w, "upstream baseURL not configured", http.StatusInternalServerError)
		return
	}
	baseURL := strings.TrimRight(globalCfg.UpstreamBaseUrl, "/")

	server := &HttpServer{}
	ctx := r.Context()
	err := server.getPieceContentPiece(ctx, rootCid, baseURL, w)
	if err != nil {
		log.Printf("[ERROR] Failed to retrieve CID: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	log.Printf("[INFO] Request completed: CID=%s, Duration=%.2f seconds", rootCid, time.Since(startTime).Seconds())
}

// FileResponse 定义了返回结果中需要的字段
type FileResponse struct {
	ProposalCid string `json:"proposal_cid"` // 原 sign 字段
	RootCid     string `json:"root_cid"`     // 原 label 字段
	PieceCid    string `json:"piece_cid"`    // piece_cid 不变
}

// fetchFiles 发起 HTTP GET 请求，并解析返回结果为 []FileResponse
func fetchFiles(provider string) ([]FileResponse, error) {
	// 构造请求 URL
	url := fmt.Sprintf("http://159.138.9.214:5174/files?provider=%s", provider)

	// 发起 GET 请求
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch from url %s: %w", url, err)
	}
	defer resp.Body.Close()

	// 检查响应状态码
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d from %s", resp.StatusCode, url)
	}

	// 读取响应体
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// 解析 JSON 为 []FileResponse
	var files []FileResponse
	if err := json.Unmarshal(body, &files); err != nil {
		return nil, fmt.Errorf("failed to parse JSON response: %w", err)
	}

	return files, nil
}

var proposalToRootCidMap = make(map[string]string)

func main() {
	// Read port from environment variable

	// 解析命令行参数 --config
	deleteFlag := flag.Bool("delete", false, "Whether to delete advertisements (use NotifyRemove)")
	configDir := flag.String("config", ".", "Path to the config directory (containing config.json)")
	flag.Parse()

	configPath := filepath.Join(*configDir, "config.json")

	cfg, err := LoadConfig(configPath)
	if err != nil {
		log.Fatalf("[ERROR] Failed to load config: %v", err)
		return
	}

	globalCfg = cfg

	port := cfg.WebPort

	ctx := context.Background()

	provider, h, priKey, listener, hostAddress, ds, err := initIndexProvider(ctx, cfg)

	if err != nil {
		log.Fatalf("[ERROR] Failed to init IndexProvider: %v", err)
		return
	}

	prs := []ma.Multiaddr{hostAddress[0]}

	providerAddress := &peer.AddrInfo{ID: h.ID(), Addrs: prs}

	provider.RegisterMultihashLister(func(ctx context.Context, _ peer.ID, contextID []byte) (providers.MultihashIterator, error) {
		proposalCid := string(contextID)
		rootCidStr, ok := proposalToRootCidMap[proposalCid]
		if !ok {
			log.Printf("[WARN] proposalCid=%s not found in mapping", proposalCid)
			return nil, fmt.Errorf("proposalCid=%s not found", proposalCid)
		}

		c, err := cid.Decode(rootCidStr)
		if err != nil {
			log.Printf("[ERROR] Failed to decode rootCid '%s' from proposalCid '%s': %v", rootCidStr, proposalCid, err)
			return nil, fmt.Errorf("invalid rootCid: %w", err)
		}

		mh := c.Hash()
		log.Printf("[INFO] MultihashLister: proposalCid=%s → rootCid=%s → mh=%s", proposalCid, rootCidStr, mh.B58String())

		return providers.SliceMultihashIterator([]multihash.Multihash{mh}), nil
	})

	//provider.RegisterMultihashLister(func(ctx context.Context, _ peer.ID, contextID []byte) (providers.MultihashIterator, error) {
	//	rootCidStr := string(contextID)
	//	if err != nil {
	//		log.Printf("[WARN] Invalid root CID: %s, err: %v", rootCidStr, err)
	//		return nil, fmt.Errorf("invalid root CID: %w", err)
	//	}
	//
	//	// 下载 CAR 文件
	//	// 下载 CAR 文件
	//	url := fmt.Sprintf("http://202.77.20.108:51375/root/%s", rootCidStr)
	//	log.Printf("requestURl %v", url)
	//	resp, err := http.Get(url)
	//	if err != nil {
	//		log.Printf("[WARN] Failed to fetch CAR file: %v", err)
	//		return nil, fmt.Errorf("failed to fetch CAR: %w", err)
	//	}
	//	defer resp.Body.Close()
	//
	//	// 将 resp.Body 写入临时文件
	//	tmpFile, err := os.CreateTemp("", "*.car")
	//	if err != nil {
	//		log.Printf("[WARN] Failed to create temp file: %v", err)
	//		return nil, err
	//	}
	//	defer os.Remove(tmpFile.Name()) // 删除临时文件
	//	defer tmpFile.Close()
	//
	//	_, err = io.Copy(tmpFile, resp.Body)
	//	if err != nil {
	//		log.Printf("[WARN] Failed to save CAR to temp file: %v", err)
	//		return nil, err
	//	}
	//
	//	// 重新打开临时文件为 ReaderAt
	//	file, err := os.Open(tmpFile.Name())
	//	if err != nil {
	//		log.Printf("[WARN] Failed to reopen CAR file: %v", err)
	//		return nil, err
	//	}
	//	defer file.Close()
	//
	//	// 读取 index
	//	cr, err := carblockstore.NewReadOnly(file, nil)
	//	if err != nil {
	//		log.Printf("[WARN] Failed to read CAR index: %v", err)
	//		return nil, fmt.Errorf("failed to read CAR index: %w", err)
	//	}
	//	i := cr.Index()
	//
	//	sorted, ok := i.(index.IterableIndex)
	//	if !ok {
	//		log.Printf("[WARN] CAR index is not IterableIndex")
	//		return nil, fmt.Errorf("CAR index is not IterableIndex")
	//	}
	//
	//	var mhs []multihash.Multihash
	//	err = sorted.ForEach(func(mh multihash.Multihash, _ uint64) error {
	//		mhs = append(mhs, mh)
	//		return nil
	//	})
	//	if err != nil {
	//		log.Printf("[WARN] Failed to iterate multihashes: %v", err)
	//		return nil, fmt.Errorf("failed to collect multihashes: %w", err)
	//	}
	//
	//	log.Printf("[INFO] MultihashLister got %d entries for rootCid=%s", len(mhs), rootCidStr)
	//	return providers.SliceMultihashIterator(mhs), nil
	//})

	minerAddress := cfg.GetMinerAddress()

	adBuilder := xproviders.NewAdBuilder(h.ID(), priKey, prs)

	last, _, err := provider.GetLatestAdv(ctx)
	if err != nil {
		log.Fatalf("GetLatestAdv failed: %v", err)
		return
	}

	var httpAddress []string
	httpAddress = append(httpAddress, hostAddress[1].String())

	// marshal http metadata
	meta := metadata.Default.New(&metadata.IpfsGatewayHttp{})
	mbytes, err := meta.MarshalBinary()
	if err != nil {
		log.Fatalf("MarshalBinary hhtp failed: %v", err)
		return
	}

	ep := xproviders.Info{
		ID:       h.ID().String(),
		Addrs:    httpAddress,
		Priv:     priKey,
		Metadata: mbytes,
	}
	adBuilder.WithExtendedProviders(ep)
	adBuilder.WithLastAdID(last)
	ad, err := adBuilder.BuildAndSign()

	if err != nil {
		log.Fatalf("BuildAndSign failed: %v", err)
		return
	}

	var adCid cid.Cid
	const maxRetries = 3
	var publishErr error
	adCid, publishErr = provider.Publish(ctx, *ad)
	for i := 0; i < maxRetries; i++ {
		adCid, publishErr = provider.Publish(ctx, *ad)
		if publishErr == nil {
			break
		}
		log.Printf("[WARNING] Publish attempt %d failed for file with rootCid %s: %v", i+1, adCid, publishErr)
		time.Sleep(3 * time.Second)
	}
	if publishErr != nil {
		log.Printf("[ERROR] Failed to publish advertisement for file with rootCid %s after %d ", "", publishErr)
		return
	}

	log.Printf("announced endpoint to indexer with advertisement cid %s", adCid)

	fullNodeApi, err := connectToLotus(ctx, cfg.FullNodeApiUrl, cfg.FullNodeJwtToken)
	if err != nil {
		log.Fatalf("[ERROR] Failed to connect to lotus: %v", err)
		return
	}

	meshCreator := idxprov.NewMeshCreator(fullNodeApi, h)

	// 设置后台任务，每6小时执行一次 fetchAndPublishFiles，并在每个循环中休眠1秒
	go func() {
		for {
			log.Printf("[INFO] Starting fetchAndPublishFiles job")
			if err := publishIpni(ctx, minerAddress, h, priKey, provider, hostAddress, providerAddress, meshCreator, *deleteFlag); err != nil {
				log.Printf("[ERROR] publishIpni failed: %v", err)
			}
			log.Printf("[INFO] fetchAndPublishFiles job completed, sleeping for 6 hours")
			time.Sleep(6 * time.Hour)
		}
	}()

	// 设置优雅退出信号，程序退出时调用 subject.Shutdown() 和 listener.Stop()
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stopChan
		log.Printf("[INFO] Shutting down...")
		if err := provider.Shutdown(); err != nil {
			log.Printf("[ERROR] subject shutdown error: %v", err)
		}
		listener.Stop()
		ds.Close()
		os.Exit(0)
	}()

	http.HandleFunc("/ipfs/", handleRequestRoot)
	http.HandleFunc("/piece/", handleRequestPiece)
	//
	log.Printf("[INFO] Starting HTTP server on port %s...", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))

	//decodeString, err := hex.DecodeString("0801124009ca4370e3626ef77102ba0061ffa2a03fdc41a5dd7047659761f8eaf42fbc490ccef848997d0b41864aa839606823cc6e7e5b9b063699070feb7ddf7ad3ad46")
	//if err != nil {
	//	log.Fatal(err)
	//}
	//// 解析私钥
	//privKey, err := cryptos.UnmarshalPrivateKey(decodeString)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	////adds := "ip4/0.0.0.0/tcp/5667"
	//announceAdds := "/ip4/211.47.49.108/tcp/5667"
	////listenAddr, err := ma.NewMultiaddr(adds)
	//
	//annAddr := ma.StringCast(announceAdds)
	//
	//pubIp := "211.47.49.108"
	//pubPort := "7798"
	//pubPorti, err := strconv.Atoi(pubPort)
	//
	//announceAddr, err := ToHttpMultiaddr(pubIp, pubPorti)
	//
	//h, err := libp2p.New(
	//	//libp2p.AddrsFactory(func(m []ma.Multiaddr) []ma.Multiaddr {
	//	//	return []ma.Multiaddr{annAddr}
	//	//}),
	//	libp2p.Identity(privKey),
	//	//	libp2p.ListenAddrs(listenAddr), // 监听指定地址
	//)
	//
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//topicName := "/indexer/ingest/mainnet"
	////receiver, err := announce.NewReceiver(h, topicName)
	//
	//var hostAddress []ma.Multiaddr
	//hostAddress = append(hostAddress, annAddr)
	//hostAddress = append(hostAddress, announceAddr)
	//
	//provider := peer.AddrInfo{ID: h.ID(), Addrs: hostAddress}
	//
	//rootCid1, err := cid.Decode("bafykbzacedioanwn5zs2bqh2e6o3qlpppwjpm5idjs372f7ziamgb4m5uzjim")
	//rootCid2, err := cid.Decode("bafykbzacecjnykljodj5pfbcrsdw54pe6tza3fa2gbrtugm32gf346asmoq5e")
	//rootCid3, err := cid.Decode("bafykbzaceabxhnyhzopulkmyvzjt7c4qzerbslzfo7ijzk6p3i3wl266ii3nq")
	//rootCid4, err := cid.Decode("bafykbzacebaekbc7x5bvkavnwgcm5zx4sw7vimftpk4rqatjbc5iyrp6jol7q")
	//
	//var cidList []cid.Cid
	//cidList = append(cidList, rootCid1)
	//cidList = append(cidList, rootCid2)
	//cidList = append(cidList, rootCid3)
	//cidList = append(cidList, rootCid4)
	//
	//if err != nil {
	//	log.Fatal(err)
	//	return
	//}
	//
	//for _, c := range cidList {
	//	receiver, err := announce.NewReceiver(h, topicName)
	//	err = receiver.Direct(context.Background(), c, provider)
	//	receiver.Close()
	//	if err != nil {
	//		log.Fatal(err)
	//		return
	//	}
	//}

	//ctx := context.Background()
	////
	//client, err := httpc.New("https://cid.contact")
	//if err != nil {
	//	log.Fatal(err)
	//	return
	//}
	//
	//annAddr := ma.StringCast("/ip4/211.47.49.108/tcp/5667")
	//
	//pubIp := "211.47.49.108"
	//pubPort := "7798"
	//pubPorti, err := strconv.Atoi(pubPort)
	//announceAddr, err := ToHttpMultiaddr(pubIp, pubPorti)
	//
	//peerId, err := peer.Decode("12D3KooWAgN786gNLDBikHq4pjyRzxbTGdBGYVscjAYJdxviu3LZ")
	//if err != nil {
	//	log.Fatal(err)
	//	return
	//}
	//
	//var hostAddress []ma.Multiaddr
	//hostAddress = append(hostAddress, annAddr)
	//hostAddress = append(hostAddress, announceAddr)
	//
	//for _, multiaddr := range hostAddress {
	//	log.Printf("address: %s ", multiaddr.String())
	//}
	//
	//provider := &peer.AddrInfo{ID: peerId, Addrs: hostAddress}
	//
	//rootCid, err := cid.Decode("baguqeeraw7kp5yy7woazte4jn6zyoa7ug4ratbiltqpsitqiifojywuz3pda")
	//
	//if err != nil {
	//	log.Fatal(err)
	//	return
	//}
	//err = client.Announce(ctx, provider, rootCid)
	//
	//if err != nil {
	//	log.Fatal(err)
	//	return
	//}

	//decodeString, err := hex.DecodeString("0801124009ca4370e3626ef77102ba0061ffa2a03fdc41a5dd7047659761f8eaf42fbc490ccef848997d0b41864aa839606823cc6e7e5b9b063699070feb7ddf7ad3ad46")
	//
	//if err != nil {
	//
	//}
	//
	//privKey, err := cryptos.UnmarshalPrivateKey(decodeString)
	//
	//// 根据私钥计算 Peer ID
	//peerID, err := peer.IDFromPrivateKey(privKey)
	//if err != nil {
	//	log.Fatalf("failed to get peer ID from private key: %v", err)
	//}
	//
	//fmt.Printf("Peer ID: %s\n", peerID.String())

	//h, err := libp2p.New()
	//
	//if err != nil {
	//	log.Printf("[ERROR] subject shutdown error: %v", err)
	//}
	//client := lp2pimpl.NewTransportsClient(h)
	//
	//peerId, err := peer.Decode("12D3KooWNuYwwJcjbuvhs7MU5D5UXnexcoLrbK2iH3S6Mpo2tauw")
	//
	//if err != nil {
	//	log.Printf("[ERROR] subject shutdown error: %v", err)
	//}
	//
	//query, err := client.SendQuery(context.Background(), peerId)
	//
	//if err != nil {
	//	log.Printf("[ERROR] subject shutdown error: %v", err)
	//}
	//type addr struct {
	//	Multiaddr string `json:"multiaddr"`
	//	Address   string `json:"address,omitempty"`
	//}
	//
	//json := make(map[string]interface{}, len(query.Protocols))
	//for _, p := range query.Protocols {
	//	addrs := make([]addr, 0, len(p.Addresses))
	//	for _, ma := range p.Addresses {
	//		// Get the multiaddress, and also try to get the address
	//		// in the protocol's native format (eg URL format for
	//		// http protocol)
	//		addrs = append(addrs, addr{
	//			Multiaddr: ma.String(),
	//			Address:   multiaddrToNative(p.Name, ma),
	//		})
	//	}
	//	json[p.Name] = addrs
	//}
	//PrintJson(json)

}

func multiaddrToNative(proto string, ma ma.Multiaddr) string {
	switch proto {
	case "http", "https":
		u, err := maurl.ToURL(ma)
		if err != nil {
			return ""
		}
		return u.String()
	}

	return ""
}

func PrintJson(obj interface{}) error {
	resJson, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling json: %w", err)
	}

	fmt.Println(string(resJson))
	return nil
}

func connectToLotus(ctx context.Context, apiUrl, token string) (v1api.FullNode, error) {
	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+token)

	full, closer, err := lotusclient.NewFullNodeRPCV1(ctx, apiUrl, headers)
	if err != nil {
		return nil, fmt.Errorf("connecting to Lotus full node: %w", err)
	}
	// defer is not used here so the caller can hold it
	_ = closer
	return full, nil
}

func publishIpni(ctx context.Context, minerAddress string, h host.Host, priKey cryptos.PrivKey, provider *engine.Engine, hostAddress []ma.Multiaddr, providerAddress *peer.AddrInfo, meshCreator idxprov.MeshCreator, doRemove bool) error {
	// 获取待处理的文件列表
	files, err := fetchFiles(minerAddress)
	if err != nil {
		log.Printf("[ERROR] fetchFiles failed for minerAddress %s: %v", minerAddress, err)
		return fmt.Errorf("fetchFiles failed: %w", err)
	}
	log.Printf("[INFO] Retrieved %d files for minerAddress %s", len(files), minerAddress)

	// 遍历每个文件，处理并发布广告
	for _, file := range files {
		// 解析 pieceCid
		pieceCid, err := cid.Decode(file.PieceCid)
		if err != nil {
			log.Printf("[ERROR] Failed to decode pieceCid '%s': %v", file.PieceCid, err)
			continue
		}

		// 解析 proposalCid（原 sign 字段）
		//proposalCid, err := cid.Decode(file.ProposalCid)
		//if err != nil {
		//	log.Printf("[ERROR] Failed to decode proposalCid '%s': %v", file.ProposalCid, err)
		//	continue
		//}

		// 解析 rootCid（原 label 字段）
		//rootCid, err := cid.Decode(file.RootCid)
		//if err != nil {
		//	log.Printf("[ERROR] Failed to decode rootCid '%s': %v", file.RootCid, err)
		//	continue
		//}

		// 构造 metadata 信息
		metadatas := metadata.GraphsyncFilecoinV1{
			PieceCID:      pieceCid,
			FastRetrieval: true,
			VerifiedDeal:  true,
		}
		//metadatas := metadata.Default.WithProtocol(multicodec.TransportIpfsGatewayHttp, metadata.HTTPV1).New(&metadata.GraphsyncFilecoinV1{
		//	PieceCID:      pieceCid,
		//	FastRetrieval: true,
		//	VerifiedDeal:  true,
		//})
		mdBytes, err := metadatas.MarshalBinary()
		if err != nil {
			log.Printf("[ERROR] Failed to marshal metadata for file with rootCid %s: %v", file.RootCid, err)
			continue
		}
		fs := metadata.Default.New(&metadatas)
		log.Printf("[INFO] Marshaled metadata for rootCid %s, length: %d bytes", file.RootCid, len(mdBytes))

		if err := meshCreator.Connect(ctx); err != nil {
			log.Printf("[WARN] Failed to connect to indexer mesh via full node: %v", err)
		}

		// 发布广告到 IPNI，增加重试机制：最多尝试3次，每次间隔1秒
		var pubResult cid.Cid
		//const maxRetries = 3
		//for i := 0; i < maxRetries; i++ {
		// 保存映射（用于 MultihashLister 回调）
		proposalToRootCidMap[file.ProposalCid] = file.RootCid

		// 使用 ProposalCid 作为 contextID

		if doRemove {
			// 使用 ProposalCid 作为 contextID
			_, err = provider.NotifyRemove(ctx, providerAddress.ID, []byte(file.ProposalCid))
			if err != nil {
				log.Printf("[ERROR] Failed to remove advertisement for ProposalCid %s: %v", file.ProposalCid, err)
			} else {
				log.Printf("[INFO] Successfully removed advertisement for ProposalCid %s", file.ProposalCid)
			}
			time.Sleep(1 * time.Second)
			continue // 删除就不再执行发布
		}

		pubResult, err = provider.NotifyPut(ctx, providerAddress, []byte(file.ProposalCid), fs)
		//if err != nil {
		//	log.Printf("[WARNING] PublishLibP2p attempt %d failed for file with rootCid %s: %v", i+1, file.RootCid, err)
		//}
		//getAdv, err := provider.GetAdv(ctx, pubResult)
		//
		//if err != nil {
		//	log.Printf("[ERROR] Failed to provider GetAdv for file with advCid %s  %v", pubResult, err)
		//}
		//jsonData, err := json.Marshal(getAdv)
		//if err != nil {
		//	log.Fatalf("Failed to convert object to JSON: %v", err)
		//}
		//log.Printf("[INFO]  json advertisement  %s", string(jsonData))
		//time.Sleep(3 * time.Second)
		//}

		if err != nil {
			log.Printf("[ERROR] Failed to publish advertisement for file with rootCid %s after %d ", file.RootCid, err)
			continue
		}
		log.Printf("[INFO] Successfully published advertisement for file with rootCid %s, result: %v", file.RootCid, pubResult)

		// 每处理一条记录休眠1秒
		time.Sleep(1 * time.Second)
	}

	log.Printf("[INFO] Finished processing all files")
	return nil
}

type testIterableIndex struct {
	index.MultihashIndexSorted
	doForEach func(f func(multihash.Multihash, uint64) error) error
}

type IndexProviderAnnounceConfig struct {
	// 直接通过 HTTP 向一组索引节点公告。
	// 注意，公告功能无论该设置如何，都会通过 pubsub 进行公告。
	AnnounceOverHttp bool

	// 索引节点的 URL 列表
	DirectAnnounceURLs []string
}

// ToHttpMultiaddr 将 hostname 和端口转换为 HTTP multiaddr
func ToHttpMultiaddr(hostname string, port int) (ma.Multiaddr, error) {
	if hostname == "" {
		return nil, fmt.Errorf("hostname is empty")
	}

	var saddr string
	if n := net.ParseIP(hostname); n != nil {
		ipVersion := "ip4"
		if strings.Contains(hostname, ":") {
			ipVersion = "ip6"
		}
		saddr = fmt.Sprintf("/%s/%s/tcp/%d/http", ipVersion, hostname, port)
	} else {
		saddr = fmt.Sprintf("/dns/%s/tcp/%d/http", hostname, port)
	}
	return ma.NewMultiaddr(saddr)
}

// initIndexProvider 初始化并启动索引提供者引擎，返回 engine 和 libp2p host
func initIndexProvider(ctx context.Context, cfg *Config) (*engine.Engine, host.Host, cryptos.PrivKey, *lp2pimpl.TransportsListener, []ma.Multiaddr, *leveldb.Datastore, error) {

	// 如果配置中没有私钥，生成新的并保存
	if cfg.GetPeerIDPrivateKey() == "" {
		priv, _, err := cryptos.GenerateEd25519Key(nil)
		if err != nil {
			log.Fatalf("failed to generate private key: %v", err)
		}

		marshalled, err := cryptos.MarshalPrivateKey(priv)
		if err != nil {
			log.Fatalf("failed to marshal private key: %v", err)
		}

		peerID, err := peer.IDFromPrivateKey(priv)
		if err != nil {
			log.Fatalf("failed to derive peer ID from private key: %v", err)
		}

		cfg.SetPeerIDPrivateKey(hex.EncodeToString(marshalled))
		cfg.SetPeerID(peerID.String())

		if err := cfg.Save("config.json"); err != nil {
			log.Printf("[WARN] Failed to save updated config: %v", err)
		} else {
			log.Printf("[INFO] Generated new peer ID: %s and saved to config", peerID.String())
		}
	}

	// 解码私钥
	privKeyBytes, err := hex.DecodeString(cfg.GetPeerIDPrivateKey())
	if err != nil {
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("failed to decode peer private key: %w", err)
	}
	privKey, err := cryptos.UnmarshalPrivateKey(privKeyBytes)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("failed to unmarshal private key: %w", err)
	}

	// 处理 libp2p 地址
	listenAddr, err := ma.NewMultiaddr(cfg.Libp2pAddress)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("failed to parse libp2p listen address: %w", err)
	}
	annAddr := ma.StringCast(cfg.Libp2pAnnounceAddress)

	pubPortInt, err := strconv.Atoi(cfg.HttpPort)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("invalid http_port: %w", err)
	}
	ipniPortInt, err := strconv.Atoi(cfg.IpniPort)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("invalid ipni_port: %w", err)
	}

	announceAddr, err := ToHttpMultiaddr(cfg.HttpIp, pubPortInt)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("failed to construct announce multiaddr: %w", err)
	}

	h, err := libp2p.New(
		libp2p.AddrsFactory(func(_ []ma.Multiaddr) []ma.Multiaddr {
			return []ma.Multiaddr{
				ma.StringCast(cfg.Libp2pAnnounceAddress),
				ma.StringCast(cfg.Libp2pAddress),
			}
		}),
		libp2p.Identity(privKey),
		libp2p.ListenAddrs(listenAddr),
	)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	listener, err := lp2pimpl.NewTransportsListener(h, annAddr, announceAddr)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("failed to create lp2p listener: %w", err)
	}
	listener.Start()

	hostAddress := []ma.Multiaddr{annAddr, announceAddr}
	providerAddress := []ma.Multiaddr{annAddr}

	// 使用配置中指定的 DataPath
	indexProviderDir := cfg.GetDataPath() + "/IndexProvider"
	if indexProviderDir == "" {
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("config `data_path` cannot be empty")
	}
	if _, err := os.Stat(indexProviderDir); os.IsNotExist(err) {
		err = os.MkdirAll(indexProviderDir, 0755)
		if err != nil {
			return nil, nil, nil, nil, nil, nil, fmt.Errorf("failed to create data path: %w", err)
		}
		log.Printf("Created data path directory: %s", indexProviderDir)
	}
	ds, err := leveldb.NewDatastore(indexProviderDir, nil)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("failed to open leveldb datastore: %w", err)
	}

	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("failed to create pubsub: %w", err)
	}
	topic, err := ps.Join("/indexer/ingest/mainnet")
	if err != nil {
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("failed to join pubsub topic: %w", err)
	}

	maAddr, err := address.NewFromString(cfg.GetMinerAddress())
	if err != nil {
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("invalid miner address: %w", err)
	}

	subject, err := engine.New(
		engine.WithHost(h),
		engine.WithPrivateKey(privKey),
		engine.WithProvider(peer.AddrInfo{ID: h.ID(), Addrs: providerAddress}),
		engine.WithDatastore(ds),
		engine.WithTopic(topic),
		engine.WithTopicName("/indexer/ingest/mainnet"),
		engine.WithRetrievalAddrs(hostAddress[0].String()),
		engine.WithDirectAnnounce([]string{"https://cid.contact/ingest/announce"}...),
		engine.WithPublisherKind(engine.Libp2pHttpPublisher),
		engine.WithHttpPublisherListenAddr(fmt.Sprintf("0.0.0.0:%d", ipniPortInt)),
		engine.WithHttpPublisherAnnounceAddr(announceAddr.String()),
		engine.WithEntriesCacheCapacity(1024),
		engine.WithChainedEntries(16384),
		engine.WithExtraGossipData(maAddr.Bytes()),
		engine.WithPurgeCacheOnStart(false),
	)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("failed to create index provider engine: %w", err)
	}

	if err := subject.Start(ctx); err != nil {
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("failed to start index provider engine: %w", err)
	}

	return subject, h, privKey, listener, hostAddress, ds, nil
}
