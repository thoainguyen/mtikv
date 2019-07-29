package main

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	pb "mtikv/pkg/api/kvpb"

	"google.golang.org/grpc"
)

const (
	address = "localhost:10002"
)

func main() {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewMTikvServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r1, err1 := c.Put(ctx, &pb.PutRequest{Key: []byte("thoainh"), Value: []byte("Nguyen Huynh Thoai")})
	if err1 != nil {
		log.Fatalf("could not put: %v", err1)
	}

	log.Infof("PutResponse : %#v", string(r1.Error))

	r2, err2 := c.Get(ctx, &pb.GetRequest{Key: []byte("thoainh")})
	if err2 != nil {
		log.Fatalf("could not get: %v", err2)
	}

	log.Infof("GetResponse : %#v", string(r2.Value))

	r3, err3 := c.Delete(ctx, &pb.DeleteRequest{Key: []byte("thoainh")})
	if err3 != nil {
		log.Fatalf("could not delete: %v", err3)
	}

	log.Infof("DeleteResponse : %#v", string(r3.Error))
}
