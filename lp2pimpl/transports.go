package lp2pimpl

import (
	"context"
	"fmt"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
	"time"

	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
)

// TransportsListener listens for incoming queries over libp2p
type TransportsListener struct {
	host      host.Host
	protocols []Protocol
}

func NewTransportsListener(h host.Host, libp2pUrl ma.Multiaddr, httpUrl ma.Multiaddr) (*TransportsListener, error) {
	var protos []Protocol

	// Get the libp2p addresses from the Host
	if len(h.Addrs()) > 0 {

		var httpAddrs []ma.Multiaddr
		var libP2PAddrs []ma.Multiaddr

		httpAddrs = append(httpAddrs, httpUrl)
		libP2PAddrs = append(libP2PAddrs, libp2pUrl)

		protos = append(protos, Protocol{
			Name:      "http",
			Addresses: httpAddrs,
		})

		protos = append(protos, Protocol{
			Name:      "libp2p",
			Addresses: libP2PAddrs,
		})
	}

	return &TransportsListener{
		host:      h,
		protocols: protos,
	}, nil
}

type QueryClientOption func(*TransportsClient)

// TransportsClient sends retrieval queries over libp2p
type TransportsClient struct {
	retryStream *RetryStream
}

func NewTransportsClient(h host.Host, options ...QueryClientOption) *TransportsClient {
	c := &TransportsClient{
		retryStream: NewRetryStream(h),
	}
	for _, option := range options {
		option(c)
	}
	return c
}

const streamReadDeadline = 30 * time.Second

// SendQuery sends a retrieval query over a libp2p stream to the peer
func (c *TransportsClient) SendQuery(ctx context.Context, id peer.ID) (*QueryResponse, error) {
	log.Debugw("query", "peer", id)

	// Create a libp2p stream to the provider
	s, err := c.retryStream.OpenStream(ctx, id, []protocol.ID{TransportsProtocolID})
	if err != nil {
		return nil, err
	}

	defer s.Close() // nolint

	// Set a deadline on reading from the stream so it doesn't hang
	_ = s.SetReadDeadline(time.Now().Add(streamReadDeadline))
	defer s.SetReadDeadline(time.Time{}) // nolint

	// Read the response from the stream
	queryResponsei, err := BindnodeRegistry.TypeFromReader(s, (*QueryResponse)(nil), dagcbor.Decode)
	if err != nil {
		return nil, fmt.Errorf("reading query response: %w", err)
	}
	queryResponse := queryResponsei.(*QueryResponse)

	log.Debugw("response", "peer", id)

	return queryResponse, nil
}

const providerReadDeadline = 10 * time.Second

type AskRequest struct {
	Miner address.Address
}
type AskResponse struct {
	Ask *SignedStorageAsk
}

type SignedStorageAsk struct {
	Ask       *StorageAsk
	Signature *crypto.Signature
}

type StorageAsk struct {
	// Price per GiB / Epoch
	Price         abi.TokenAmount
	VerifiedPrice abi.TokenAmount

	MinPieceSize abi.PaddedPieceSize
	MaxPieceSize abi.PaddedPieceSize
	Miner        address.Address
	Timestamp    abi.ChainEpoch
	Expiry       abi.ChainEpoch
	SeqNo        uint64
}

func (l *TransportsListener) handleNewAskStream(s network.Stream) {
	start := time.Now()
	reqLog := log.With("client-peer", s.Conn().RemotePeer())
	reqLog.Debugw("new queryAsk request")

	defer func() {
		err := s.Close()
		if err != nil {
			reqLog.Infow("closing stream", "err", err)
		}
		reqLog.Debugw("handled queryAsk request", "duration", time.Since(start).String())
	}()

	// 读取请求
	_ = s.SetReadDeadline(time.Now().Add(providerReadDeadline))
	var req AskRequest
	err := cborutil.ReadCborRPC(s, &req)
	_ = s.SetReadDeadline(time.Time{}) // 清除
	if err != nil {
		reqLog.Warnw("reading queryAsk request from stream", "err", err)
		return
	}

	// 构造一个 StorageAsk（你也可以从 config 或外部来源加载）
	ask := &StorageAsk{
		Price:         abi.NewTokenAmount(1000000000000), // 每 GiB 每 epoch 价格
		VerifiedPrice: abi.NewTokenAmount(500000000000),  // 验证订单价格
		MinPieceSize:  256 << 20,                         // 256MiB
		MaxPieceSize:  32 << 30,                          // 32GiB
		Miner:         req.Miner,
		Timestamp:     abi.ChainEpoch(time.Now().Unix()),            // 当前时间戳
		Expiry:        abi.ChainEpoch(time.Now().Unix() + 24*60*60), // 一天后
		SeqNo:         1,                                            // 从 1 开始
	}

	// 对 ask 进行签名（使用你已有的 libp2p 私钥）
	askBytes, err := cborutil.Dump(ask)
	if err != nil {
		reqLog.Errorw("failed to marshal ask", "err", err)
		return
	}

	sig, err := l.host.Peerstore().PrivKey(l.host.ID()).Sign(askBytes)
	if err != nil {
		reqLog.Errorw("failed to sign ask", "err", err)
		return
	}

	resp := AskResponse{
		Ask: &SignedStorageAsk{
			Ask: ask,
			Signature: &crypto.Signature{
				Type: crypto.SigTypeSecp256k1,
				Data: sig,
			},
		},
	}

	_ = s.SetWriteDeadline(time.Now().Add(providerWriteDeadline))
	defer s.SetWriteDeadline(time.Time{})

	if err := cborutil.WriteCborRPC(s, &resp); err != nil {
		reqLog.Errorw("failed to write queryAsk response", "err", err)
	}
}

const providerWriteDeadline = 10 * time.Second
const AskProtocolID = "/fil/storage/ask/1.1.0"

func (l *TransportsListener) Start() {
	l.host.SetStreamHandler(TransportsProtocolID, l.handleNewQueryStream)
	l.host.SetStreamHandler(AskProtocolID, l.handleNewAskStream)
}

func (l *TransportsListener) Stop() {
	l.host.RemoveStreamHandler(TransportsProtocolID)
}

// Called when the client opens a libp2p stream
func (l *TransportsListener) handleNewQueryStream(s network.Stream) {
	defer s.Close() // nolint

	log.Debugw("query", "peer", s.Conn().RemotePeer())

	response := QueryResponse{Protocols: l.protocols}

	// Set a deadline on writing to the stream so it doesn't hang
	_ = s.SetWriteDeadline(time.Now().Add(time.Second * 30))
	defer s.SetWriteDeadline(time.Time{}) // nolint

	// Write the response to the client
	err := BindnodeRegistry.TypeToWriter(&response, s, dagcbor.Encode)
	if err != nil {
		log.Infow("error writing query response", "peer", s.Conn().RemotePeer(), "err", err)
		return
	}
}
