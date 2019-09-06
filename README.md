# MTiKV

Mini [TiKV](https://github.com/tikv/tikv): A Distributed transactional key-value database

## 1. Architecture

### 1.1. Overview

![](./docs/images/mtikv.png)

### 1.2 Raft group

![](./docs/images/mtikv-raft-group.png)

## 2. Flow

### 2.1. TxnKV


```plantuml
client -> mtikv : Begin_Txn()
client -> pd : GetTs(start_ts)
loop command
    alt get
        client -> mtikv: get(key)
    else set, delete
        client -> client: write to Buffer
end
client -> mtikv: Commit_Txn()
client -> pd: GetTs(commit_ts)
client -> mtikv: Do 2PC

```


## Getting Started

### Prerequisites

### Installing

## Running the tests

### Break down into end to end tests

### And coding style tests

## Deployment

## Built With

## Contributing

## Versioning

## Authors

* **Nguyen Huynh Thoai** - *Maintainer* - [thoainguyen](https://github.com/thoainguyen)

## License

## Acknowledgments