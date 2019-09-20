package config

import (
	"log"

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

type MTikvClient struct {
	Host      string
	PD        string
	RaftGroup map[string]RaftGroup
}

type PDConfig struct {
	Host string
}

func LoadMTikvClientConfig() *MTikvClient {
	cfg := &MTikvClient{}
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
