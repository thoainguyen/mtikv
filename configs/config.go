package configs

import (
	"github.com/spf13/viper"
)

const configFilePath = "."
const configFileName = "config"

// MTikvServiceConfig struct
type MTikvServiceConfig struct {
	GRPCPort int
	GRPCHost string
	DBPath   string
}

//LoadConfig load config
func LoadConfig() error {
	viper.SetConfigName(configFileName)
	viper.AddConfigPath(configFilePath)
	viper.SetConfigType("yaml")
	return viper.ReadInConfig()
}
