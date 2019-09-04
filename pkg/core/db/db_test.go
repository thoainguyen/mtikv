package db

import (
	"testing"
)

func TestPut(t *testing.T) {
	db, err := CreateDB("volumes", "snap")
	if err != nil {
		t.Error(err)
	}
	err = db.PutData("th", "TH")
	if err != nil {
		t.Error(err)
	}
	err = db.CloseDB()
	if err != nil {
		t.Error(err)
	}
}

func TestGet(t *testing.T) {
	db, err := CreateDB("volumes", "snap")
	if err != nil {
		t.Error(err)
	}

	var val string
	val, err = db.GetData("th")
	if err != nil {
		t.Error(err)
	}
	if val != "TH" {
		t.Error(err)
	}

	err = db.CloseDB()
	if err != nil {
		t.Error(err)
	}
}

func TestDelete(t *testing.T) {
	db, err := CreateDB("volumes", "snap")
	if err != nil {
		t.Error(err)
	}

	err = db.DeleteData("th")
	if err != nil {
		t.Error(err)
	}
	
	err = db.CloseDB()
	if err != nil {
		t.Error(err)
	}
}
