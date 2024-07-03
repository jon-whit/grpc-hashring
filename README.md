# grpc-hashring
A Go implementation of a gRPC client-side load balancer that uses consistent hashing to
load balance gRPC subconnections.

## Setup
```go
conn, err := grpc.Dial(
	...,
    grpc.WithDefaultServiceConfig(`"loadBalancingConfig": {
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

## Common Usages
