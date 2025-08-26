package main

import (
	"encoding/json"
	"fmt"
	ma "github.com/multiformats/go-multiaddr"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// Config 定义了节点的配置参数
type Config struct {
	MinerAddress          string `json:"miner_address"`
	PeerIDPrivateKey      string `json:"peer_id_private_key"`
	PeerId                string `json:"peer_id"`
	Libp2pAddress         string `json:"libp2p_address"`
	Libp2pAnnounceAddress string `json:"libp2p_announce_address"`
	HttpIp                string `json:"http_ip"`
	HttpPort              string `json:"http_port"`
	WebPort               string `json:"web_port"`
	IpniPort              string `json:"ipni_port"`
	FullNodeApiUrl        string `json:"full_node_api_url"`
	FullNodeJwtToken      string `json:"full_node_jwt_token"`
	DataPath              string `json:"data_path"`
	UpstreamBaseUrl       string `json:"upstream_base_url"`
}

// LoadConfig 从指定的文件路径加载配置
func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	var cfg Config
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to decode config file: %w", err)
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get user home dir: %w", err)
	}

	updated := false

	// ---- 始终尝试读取 .lotus/api ----
	apiPath := filepath.Join(homeDir, ".lotus", "api")
	if apiBytes, err := os.ReadFile(apiPath); err == nil {
		apiStr := strings.TrimSpace(string(apiBytes))
		maddr, err := ma.NewMultiaddr(apiStr)
		if err != nil {
			log.Printf("[WARN] Failed to parse multiaddr from .lotus/api: %v", err)
		} else {
			valueMap := map[string]string{}
			for _, p := range maddr.Protocols() {
				v, err := maddr.ValueForProtocol(p.Code)
				if err == nil {
					valueMap[p.Name] = v
				}
			}
			if ip, ok1 := valueMap["ip4"]; ok1 {
				if port, ok2 := valueMap["tcp"]; ok2 {
					newApiUrl := fmt.Sprintf("ws://%s:%s/rpc/v0", ip, port)
					if cfg.FullNodeApiUrl != newApiUrl {
						cfg.FullNodeApiUrl = newApiUrl
						updated = true
						log.Printf("[INFO] Updated FullNodeApiUrl from lotus: %s", newApiUrl)
					}
				}
			}
		}
	} else {
		log.Printf("[WARN] Failed to read .lotus/api: %v", err)
	}

	// ---- 始终尝试读取 .lotus/token ----
	tokenPath := filepath.Join(homeDir, ".lotus", "token")
	if tokenBytes, err := os.ReadFile(tokenPath); err == nil {
		newToken := strings.TrimSpace(string(tokenBytes))
		if cfg.FullNodeJwtToken != newToken {
			cfg.FullNodeJwtToken = newToken
			updated = true
			log.Printf("[INFO] Updated FullNodeJwtToken from lotus")
		}
	} else {
		log.Printf("[WARN] Failed to read .lotus/token: %v", err)
	}

	// 保存变更后的配置
	if updated {
		if err := cfg.Save(path); err != nil {
			log.Printf("[WARN] Failed to save updated config: %v", err)
		} else {
			log.Printf("[INFO] Updated config saved to %s", path)
		}
	}

	return &cfg, nil
}

// Save 将当前配置保存到指定的文件路径
func (cfg *Config) Save(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	return encoder.Encode(cfg)
}

// Getters 和 Setters

func (cfg *Config) GetMinerAddress() string {
	return cfg.MinerAddress
}

func (cfg *Config) SetMinerAddress(val string) {
	cfg.MinerAddress = val
}

func (cfg *Config) GetPeerIDPrivateKey() string {
	return cfg.PeerIDPrivateKey
}

func (cfg *Config) SetPeerID(val string) {
	cfg.PeerId = val
}

func (cfg *Config) GetPeerID() string {
	return cfg.PeerId
}

func (cfg *Config) SetPeerIDPrivateKey(val string) {
	cfg.PeerIDPrivateKey = val
}

func (cfg *Config) GetWebPort() string {
	return cfg.WebPort
}

func (cfg *Config) SetWebPort(val string) {
	cfg.WebPort = val
}

func (cfg *Config) GetIpniPort() string {
	return cfg.IpniPort
}

func (cfg *Config) SetIpniPort(val string) {
	cfg.IpniPort = val
}

func (cfg *Config) GetLibp2pAddress() string {
	return cfg.Libp2pAddress
}

func (cfg *Config) SetLibp2pAddress(val string) {
	cfg.Libp2pAddress = val
}

func (cfg *Config) GetHttpIp() string {
	return cfg.HttpIp
}

func (cfg *Config) SetHttpIp(val string) {
	cfg.HttpIp = val
}

func (cfg *Config) GetHttpPort() string {
	return cfg.HttpPort
}

func (cfg *Config) SetHttpPort(val string) {
	cfg.HttpPort = val
}

func (cfg *Config) GetLibp2pAnnounceAddress() string {
	return cfg.Libp2pAnnounceAddress
}

func (cfg *Config) SetLibp2pAnnounceAddress(val string) {
	cfg.Libp2pAnnounceAddress = val
}

func (cfg *Config) GetFullNodeApiUrl() string {
	return cfg.FullNodeApiUrl
}

func (cfg *Config) SetFullNodeApiUrl(val string) {
	cfg.FullNodeApiUrl = val
}

func (cfg *Config) GetFullNodeJwtToken() string {
	return cfg.FullNodeJwtToken
}

func (cfg *Config) SetFullNodeJwtToken(val string) {
	cfg.FullNodeJwtToken = val
}

func (cfg *Config) GetDataPath() string {
	return cfg.DataPath
}

func (cfg *Config) SetDataPath(val string) {
	cfg.DataPath = val
}

func (cfg *Config) GetUpstreamBaseUrl() string {
	return cfg.UpstreamBaseUrl
}

func (cfg *Config) SetUpstreamBaseUrl(val string) {
	cfg.UpstreamBaseUrl = val
}
