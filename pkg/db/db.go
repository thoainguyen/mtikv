package db

import (
	"github.com/tecbot/gorocksdb"
	log "github.com/sirupsen/logrus"
	"errors"
)


type DB struct {
	path      string
	database  *gorocksdb.DB
	readOpts  *gorocksdb.ReadOptions
	writeOpts *gorocksdb.WriteOptions
}

func CreateDB(path string) (*DB, error) {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, path)
	
	if err != nil {
		log.Fatal("Can't Open Database")
		return nil, err
	}

	return &DB{
		path, db,
		gorocksdb.NewDefaultReadOptions(),
		gorocksdb.NewDefaultWriteOptions()
	}, nil
}

func (db *DB) GetData(key []byte) ([]byte, error) {
	data, err := db.database.Get(db.readOpts, key)
	if err != nil {
		log.Fatal("Can not Get Key")
		return nil, err
	}
	return data.Data(), nil
}

func (db *DB) PutData(key []byte, value []byte) error {
	return db.database.Put(db.writeOpts, key, value)
}

func (db *DB) DeleleData(key []byte) error {
	return db.database.Delete(db.writeOpts, key)
}

func (db *DB) CloseDB() error {
	db.database.Close()
	return nil
}