package hashring

import (
	"context"
	"fmt"

	"github.com/bytedance/gopkg/lang/fastrand"
	"github.com/cespare/xxhash/v2"
	"github.com/jon-whit/grpc-hashring/hashring"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

type ctxKey string

var (
	hashringCtxKey ctxKey = "hashringCtxKey"
)

func ContextWithHashringKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, hashringCtxKey, key)
}

func HashringKeyFromContext(ctx context.Context) (string, bool) {
	hashringKey, ok := ctx.Value(hashringCtxKey).(string)
	return hashringKey, ok
}

type hashringPickerBuilder struct {
	keyReplicationCount int
}

var _ base.PickerBuilder = (*hashringPickerBuilder)(nil)

// Build implements base.PickerBuilder.
func (h *hashringPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	ring := hashring.NewConsistentHashring(xxhash.Sum64, hashring.DefaultConsistentHashringConfig())

	subConns := make(map[string]balancer.SubConn, len(info.ReadySCs))
	for subConn, subConnInfo := range info.ReadySCs {
		targetAddr := subConnInfo.Address
		ring.AddMember(targetAddr)
		subConns[targetAddr.String()] = subConn
	}

	return &hashringPicker{
		subConns,
		ring,
		h.keyReplicationCount,
	}
}

type hashringPicker struct {
	subConns            map[string]balancer.SubConn
	hashring            hashring.Hashring
	keyReplicationCount int
}

var _ balancer.Picker = (*hashringPicker)(nil)

// Pick implements balancer.Picker.
//
// Pick uses the hashring key from the context (if any) and based on the key
// computes the member of the hashring owning that key. If the keyReplicationCount
// is greater than 1, then the first keyReplicationCount closest members of the
// hashring are calculated and selected at random.
func (h *hashringPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	hashringKey, ok := HashringKeyFromContext(info.Ctx)
	if !ok {
		return balancer.PickResult{}, fmt.Errorf("no hashring key provided in the context")
	}

	targetMembers, err := h.hashring.FindNearestN([]byte(hashringKey), h.keyReplicationCount)
	if err != nil {
		return balancer.PickResult{}, err
	}

	indx := fastrand.Intn(h.keyReplicationCount)
	targetMember := targetMembers[indx]

	targetAddrKey := targetMember.String()
	subConn := h.subConns[targetAddrKey]

	return balancer.PickResult{SubConn: subConn}, nil
}
