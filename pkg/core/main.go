package main

import (
	"bytes"
	"fmt"
	// "github.com/thoainguyen/mtikv/pkg/core/db"
)

func main() {
	// d, _ := db.CreateDB("volumes", "snap")
	// for i := 0; i < 10; i++ {
	// 	d.PutData(
	// 		fmt.Sprintf("key-%d", i),
	// 		fmt.Sprintf("value-%d", i),
	// 	)
	// }
	// err := d.Traverse([]byte("key-04"), func(key, val []byte) {
	// 	fmt.Println(string(key), string(val))
	// })
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// defer d.CloseDB()

	var x = bytes.Join([][]byte{
		[]byte("prefix_"),
		[]byte()
	}, []byte("|"))
	var y = bytes.Join([][]byte{
		[]byte("prefix_"),
		[]byte("")
	}, []byte("|"))
	fmt.Println(bytes.Compare(x, y))
}
