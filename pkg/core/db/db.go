package db

import (
	log "github.com/sirupsen/logrus"
	"github.com/tecbot/gorocksdb"
)

type DB struct {
	path      string
	snapPath  string
	database  *gorocksdb.DB
	readOpts  *gorocksdb.ReadOptions
	writeOpts *gorocksdb.WriteOptions
}

func CreateDB(path, snapPath string) (*DB, error) {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, path)
	if err != nil {
		log.Fatalf("Can't Open Database: %v", err)
		return nil, err
	}

	return &DB{
		path, snapPath, db,
		gorocksdb.NewDefaultReadOptions(),
		gorocksdb.NewDefaultWriteOptions(),
	}, nil
}

func (db *DB) GetData(key string) (string, error) {
	data, err := db.database.Get(db.readOpts, []byte(key))
	if err != nil {
		log.Fatal("Can not Get Key")
		return "", err
	}
	return string(data.Data()), nil
}

func (db *DB) PutData(key string, value string) error {
	return db.database.Put(db.writeOpts, []byte(key), []byte(value))
}

func (db *DB) DeleteData(key string) error {
	return db.database.Delete(db.writeOpts, []byte(key))
}

func (db *DB) SaveSnapShot() string {
	envOptions := gorocksdb.NewDefaultEnvOptions()
	options := gorocksdb.NewDefaultOptions()
	sstWriter := gorocksdb.NewSSTFileWriter(envOptions, options)
	err := sstWriter.Open(db.snapPath)
	if err != nil {
		log.Fatal(err)
	}
	it := db.database.NewIterator(db.readOpts)
	defer it.Close()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		sstWriter.Add(it.Key().Data(), it.Value().Data())
	}
	if err := it.Err(); err != nil {
		log.Fatal(err)
	}
	err = sstWriter.Finish()
	if err != nil {
		log.Fatal(err)
	}
	return db.snapPath
}

func (db *DB) LoadSnapShot(filesPath string) {
	opts := gorocksdb.NewDefaultIngestExternalFileOptions()
	err := db.database.IngestExternalFile([]string{filesPath}, opts)
	if err != nil {
		log.Fatal(err)
	}
}

func (db *DB) CloseDB() error {
	db.database.Close()
	return nil
}
