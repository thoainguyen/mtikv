
package config

import (
	"fmt"

	"github.com/spf13/viper"
)

const (
	CFG_GRPCHost          	= "GRPCHost"
	CFG_GRPCPort         	= "GRPCPort"
	CFG_DBPath      		= "DBPath"
)

/*
Stores config data
*/

var (
	GRPCHost          	= "GRPCHost"
	GRPCPort         	= "GRPCPort"
	DBPath      		= "DBPath"
)

func init() {
	fmt.Println("Init global config")
	LoadConfigFile("config", ".")
	fmt.Println(ToString())
}

func LoadConfigFile(fileName string, path string) {
	viper.SetConfigName(fileName)         // name of config file (without extension)
	viper.AddConfigPath(path)             // path to look for the config file in
	viper.AddConfigPath("$HOME/.appname") // call multiple times to add many search paths
	viper.AddConfigPath(".")              // optionally look for config in the working directory
	viper.ReadInConfig()                  // Find and read the config file

	viper.SetDefault(CFG_GRPCHost, "localhost")
	viper.SetDefault(CFG_GRPCPort, "8089")
	viper.SetDefault(CFG_DBPath, 	"data")

	GRPCHost = fmt.Sprintf("%v", viper.Get(CFG_GRPCHost))
	GRPCPort = fmt.Sprintf("%v", viper.Get(CFG_GRPCPort))
	DBPath = fmt.Sprintf("%v", viper.Get(CFG_DBPath))
}

func ToString() string {
	return fmt.Sprintf("GRPCHost = %v\nGRPCPort = %v\nDBPath = %v\n",
		GRPCHost, GRPCPort, DBPath)
}