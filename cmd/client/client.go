package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"unicode"

	pb "github.com/thoainguyen/mtikv/proto/mtikv_clipb"
	"google.golang.org/grpc"
)

type Param struct {
	Op     string
	First  string
	Second string
}

func Parse(str string) (*Param, bool) {
	lastQuote := rune(0)
	f := func(c rune) bool {
		switch {
		case c == lastQuote:
			lastQuote = rune(0)
			return false
		case lastQuote != rune(0):
			return false
		case unicode.In(c, unicode.Quotation_Mark):
			lastQuote = c
			return false
		default:
			return unicode.IsSpace(c)
		}
	}

	srcStrs := strings.FieldsFunc(str, f)
	destStrs := srcStrs[:0]

	for i := range srcStrs {
		destStrs = append(destStrs, strings.Trim(srcStrs[i], "\""))
		if i == 0 {
			destStrs[i] = strings.ToLower(destStrs[i])
		}
	}

	pr := &Param{Op: destStrs[0]}

	switch destStrs[0] {
	case "get", "del":
		if len(destStrs) < 2 {
			return &Param{}, false
		}
		pr.First = destStrs[1]
	case "set":
		if len(destStrs) < 3 {
			return &Param{}, false
		}
		pr.First, pr.Second = destStrs[1], destStrs[2]
	case "begin", "commit", "rollback":
	default: // miss type of command
		return &Param{}, false
	}

	return pr, true
}

var (
	mtikv_cli = flag.String("mtikv_cli", "localhost:8938", "mtikv_cli host name")
)

func main() {

	flag.Parse()

	conn, err := grpc.Dial(*mtikv_cli, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Fail to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewMTikvCliClient(conn)

	reader := bufio.NewReader(os.Stdin)
	var tid uint64

	for {
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')

		text = strings.Replace(text, "\n", "", -1)

		params, ok := Parse(text)
		if !ok {
			continue
		}

		tid = Handler(tid, client, params)
	}

}

func Handler(tid uint64, cli pb.MTikvCliClient, pr *Param) uint64 {
	ctx := context.TODO()
	switch pr.Op {
	case "begin":
		result, err := cli.BeginTxn(ctx, &pb.BeginTxnRequest{})
		if err != nil {
			log.Fatal(err)
		}
		return result.GetTransID()
	case "commit":
		result, err := cli.CommitTxn(ctx, &pb.CommitTxnRequest{
			TransID: tid,
		})

		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(strings.ToLower(result.GetError().String()))
		return 0
	case "rollback":
		result, err := cli.RollBackTxn(ctx, &pb.RollBackTxnRequest{
			TransID: tid,
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(result.GetError().String())
		return 0
	case "get":
		result, err := cli.Get(ctx, &pb.GetRequest{
			TransID: tid,
			Key:     []byte(pr.First),
		})
		if err != nil {
			log.Fatal(err)
		}
		val := result.GetValue()
		if val == nil {
			fmt.Println("(nil)")
		} else {
			fmt.Printf("\"%s\"\n", string(val))
		}
		return result.GetTransID()
	case "set":
		result, err := cli.Set(ctx, &pb.SetRequest{
			TransID: tid,
			Key:     []byte(pr.First),
			Value:   []byte(pr.Second),
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(strings.ToLower(result.GetError().String()))
		return result.GetTransID()
	case "del":
		result, err := cli.Delete(ctx, &pb.DeleteRequest{
			TransID: tid,
			Key:     []byte(pr.First),
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(strings.ToLower(result.GetError().String()))
		return result.GetTransID()
	}
	return 0
}
