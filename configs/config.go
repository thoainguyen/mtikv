package configs

import (
	"github.com/spf13/viper"
)

const configFilePath = "."
const configFileName = "config"

// KvServiceConfig struct
type KvServiceConfig struct {
	GRPCPort int
	GRPCHost string
	DBPath   string
}

type RaftServiceConfig struct {
	GRPCPort   int
	GRPCHost   string
	DBPath     string
	DBSnapPath string
}

//LoadConfig load config
func LoadConfig() error {
	viper.SetConfigName(configFileName)
	viper.AddConfigPath(configFilePath)
	viper.SetConfigType("yaml")
	return viper.ReadInConfig()
}
