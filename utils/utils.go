package utils

import (
	"log"

	"github.com/gogo/protobuf/proto"
)

func Marshal(pb proto.Message) []byte {
	data, err := proto.Marshal(pb)
	CheckError(err)
	return data
}

func Unmarshal(buf []byte, pb proto.Message) {
	err := proto.Unmarshal(buf, pb)
	CheckError(err)
}

func CheckError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
