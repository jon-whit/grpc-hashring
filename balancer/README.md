
## Example
```go
conn, err := grpc.Dial(
	...,
    grpc.WithDefaultServiceConfig(`"load_balancing_config": {
        "hashring": {
          "keyReplicationCount": 2
        }
    }`),
    ...
)
if err != nil {
    ...
}
defer conn.Close()
```