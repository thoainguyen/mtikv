```plantuml
alt do get
    client -> mtikv_cli: get(key)
    mtikv_cli -> pd: last_ts = getTso()
    mtikv_cli -> mtikv: get(key, last_ts)
    mtikv-->mtikv_cli: value
    mtikv_cli->client: value
else do set, del
    opt if op == del
        client -> client: value = nil
    end
    client -> mtikv_cli: set(key, value)
    mtikv_cli -> pd: ts = getTso()
    mtikv_cli -> mtikv: set(key, value, ts)
    mtikv-->mtikv_cli: error
    mtikv_cli->client: error
end
```