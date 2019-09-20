package config

import (
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
	RaftGroup []RaftGroup
}

func LoadConfig() error {
	viper.SetConfigName(configFileName)
	viper.AddConfigPath(configFilePath)
	viper.SetConfigType("yml")

	return viper.ReadInConfig()
}
