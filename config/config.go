package config

import (
	"log"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

const configFilePath = "."
const configFileName = "config"

type RaftGroup struct {
	RaftID  string
	From    string
	To      string
	Address []string
}

type MTikvCliConfig struct {
	Host      string
	PD        string
	RaftGroup map[string]RaftGroup
}

type PDConfig struct {
	Host string
}

type MTikvNode struct {
	Host    string
	DataDir string
	RaftID  string
	RGroup  string
	Peers   string
}

type MtikvNodeV2 struct {
	Host      string
	DataDir   string
	RaftID    []int
	RaftGroup []string
	Peers     []string
}

type MTikvConfig struct {
	Node    map[string]MTikvNode
	Network map[string]string
}

func LoadMtikvNodeV2(id string) *MtikvNodeV2 {
	cfg := LoadMtikvConfig()
	node := cfg.Node[id]

	raftID := strings.Split(node.RaftID, ",")
	intRaftID := make([]int, len(raftID))
	peers := strings.Split(node.Peers, ",")
	strPeers := make([]string, len(raftID))

	for idx := range raftID {
		intRaftID[idx], _ = strconv.Atoi(raftID[idx])
		strPeers[idx] = cfg.Network[peers[idx]]
	}

	return &MtikvNodeV2{
		Host:      node.Host,
		DataDir:   node.DataDir,
		RaftID:    intRaftID,
		RaftGroup: strings.Split(node.RGroup, ","),
		Peers:     strPeers,
	}
}

func LoadMtikvConfig() *MTikvConfig {
	cfg := &MTikvConfig{}
	LoadConfig()

	if err := viper.Unmarshal(cfg); err != nil {
		log.Fatal("load config: ", err)
	}
	return cfg
}

func LoadMTikvClientConfig() *MTikvCliConfig {
	cfg := &MTikvCliConfig{}
	LoadConfig()

	if err := viper.Unmarshal(cfg); err != nil {
		log.Fatal("load config: ", err)
	}
	return cfg
}

func LoadPDConfig() *PDConfig {
	cfg := &PDConfig{}
	LoadConfig()

	if err := viper.Unmarshal(cfg); err != nil {
		log.Fatal("load config: ", err)
	}
	return cfg
}

func LoadConfig() error {
	viper.SetConfigName(configFileName)
	viper.AddConfigPath(configFilePath)
	viper.SetConfigType("yml")

	return viper.ReadInConfig()
}
