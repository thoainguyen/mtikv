package store

import (
	"bytes"
	"fmt"
	"testing"
)

func TestStore(t *testing.T) {
	s := CreateStore("volumes")
	for i := 0; i < 20; i += 4 {
		for j := 0; j < 4; j++ {
			s.Put(j, []byte(fmt.Sprintf("key-%d", i+j)), []byte(fmt.Sprintf("value-%d", i+j)))
		}
	}

	var v []byte
	for i := 0; i < 20; i += 4 {
		for j := 0; j < 4; j++ {
			v = s.Get(j, []byte(fmt.Sprintf("key-%d", i+j)))
			if bytes.Compare(v, []byte(fmt.Sprintf("value-%d", i+j))) != 0 {
				t.Error("Get wrong value")
			}
		}
	}

	s.Destroy()
}
