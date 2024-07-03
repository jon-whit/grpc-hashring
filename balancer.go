package hashring

import (
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/serviceconfig"
)

func init() {
	balancer.Register(NewHashringBalancerBuilder())
}

// Name is the name of hashring balancer.
const Name = "hashring"

var logger = grpclog.Component("hashring")

type hashringBalancerBuilder struct{}

var _ balancer.Builder = (*hashringBalancerBuilder)(nil)
var _ balancer.ConfigParser = (*hashringBalancerBuilder)(nil)

func NewHashringBalancerBuilder() balancer.Builder {
	return base.NewBalancerBuilder(Name, &hashringPickerBuilder{
		keyReplicationCount: 1,
	}, base.Config{HealthCheck: true})
}

// Build implements balancer.Builder.
func (h *hashringBalancerBuilder) Build(
	cc balancer.ClientConn,
	opts balancer.BuildOptions,
) balancer.Balancer {
	return &hashringBalancer{
		cc: cc,
	}
}

// Name implements balancer.Builder.
func (h *hashringBalancerBuilder) Name() string {
	return Name
}

type HashringBalancerConfig struct {
	serviceconfig.LoadBalancingConfig
	KeyReplicationCount int `json:"keyReplicationCount"`
}

// ParseConfig implements balancer.ConfigParser.
func (h *hashringBalancerBuilder) ParseConfig(
	configJSON json.RawMessage,
) (serviceconfig.LoadBalancingConfig, error) {
	var balancerConfig HashringBalancerConfig
	if err := json.Unmarshal(configJSON, &balancerConfig); err != nil {
		return nil, err
	}

	return &balancerConfig, nil
}

type hashringBalancer struct {
	cc balancer.ClientConn
}

var _ balancer.Balancer = (*hashringBalancer)(nil)

// Close implements balancer.Balancer.
func (h *hashringBalancer) Close() {
	panic("unimplemented")
}

// ResolverError implements balancer.Balancer.
func (h *hashringBalancer) ResolverError(err error) {
	panic("unimplemented")
}

// UpdateClientConnState implements balancer.Balancer.
func (h *hashringBalancer) UpdateClientConnState(
	clientConnState balancer.ClientConnState,
) error {
	_ = clientConnState.ResolverState
	_ = clientConnState.BalancerConfig

	h.cc.UpdateState(balancer.State{})

	return fmt.Errorf("not implemented")
}

// UpdateSubConnState implements balancer.Balancer.
func (h *hashringBalancer) UpdateSubConnState(
	subConn balancer.SubConn,
	subConnState balancer.SubConnState,
) {
	// UpdateSubConnState is deprecated.
}
