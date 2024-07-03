//go:generate mockgen -source hashring.go -destination ./mocks/mock_hashring.go Hasher
package hashring

import (
	"errors"
	"fmt"

	"github.com/google/btree"
)

var (
	ErrTooFewMembers = errors.New("too few members in the hashring")
)

// Member represents a virtual node or member of a hashring. The String
// implementation provides the key value for the virtual member.
type Member interface {
	String() string
}

type StringMember string

func (s StringMember) String() string {
	return string(s)
}

// Hashring defines an interface for managing virtual members of a consistent hashring
// circle.
type Hashring interface {

	// AddMember adds the provided member to the hashring.
	AddMember(Member)

	// RemoveMember removes the provided member from the hashring.
	RemoveMember(Member)

	// FindNearestN finds the nearest n members on the hashring for the
	// provided key. If there are an insufficient number of members in the
	// hashring an ErrTooFewMembers error is returned.
	FindNearestN(key []byte, n int) ([]Member, error)
}

// consistentHashring implements Hashring using consistent hashing to locate and manage members
// of the hashring.
type consistentHashring struct {
	hasher Hasher
	ring   map[uint64]Member

	// memberKeys is a BTree here because it provides efficient insertion
	// and lookup, especially because the keys are well distributed in the
	// first place (due to the uniform hash distribution)
	memberKeys *btree.BTree

	replicationFactor int
}

var _ Hashring = (*consistentHashring)(nil)

type Hasher func(b []byte) uint64

type ConsistentHashringConfig struct {

	//  ReplicationFactor controls the distribution of virtual members on the hashring.
	//
	// A ReplicationFactor greater than 1 means that each member added to the hashring
	// will be added in multiple positions on the hashring.
	ReplicationFactor int
}

func DefaultConsistentHashringConfig() ConsistentHashringConfig {
	return ConsistentHashringConfig{
		ReplicationFactor: 1,
	}
}

// NewConsistentHashring constructs a Hashring providing consistent hashing configured
// with the configurations provided.
//
// If one or more members are provided these members are immediately added to the hashring.
func NewConsistentHashring(
	hasher Hasher,
	cfg ConsistentHashringConfig,
	members ...Member,
) *consistentHashring {
	ring := &consistentHashring{
		ring:              make(map[uint64]Member),
		hasher:            hasher,
		replicationFactor: cfg.ReplicationFactor,
		memberKeys:        btree.New(2),
	}

	for _, member := range members {
		ring.AddMember(member)
	}

	return ring
}

// AddMember adds the provided member to the hashring.
//
// If the configured replication factor is greater than 1,
// then the member is added multiple times to the hashring
// in different positions.
func (c *consistentHashring) AddMember(m Member) {
	for i := range c.replicationFactor {
		memberKey := fmt.Sprintf("%s%d", m, i)
		memberHash := c.hasher([]byte(memberKey))
		c.ring[memberHash] = m
		c.memberKeys.ReplaceOrInsert(UInt64Item(memberHash))
	}
}

// RemoveMember removes the provided member and all virtual
// replicas of it from the hashring.
func (c *consistentHashring) RemoveMember(m Member) {
	for i := range c.replicationFactor {
		memberKey := fmt.Sprintf("%s%d", m, i)
		memberHash := c.hasher([]byte(memberKey))
		delete(c.ring, memberHash)
		c.memberKeys.Delete(UInt64Item(memberHash))
	}
}

// FindNearestN locates the n members (in ascending order) in the
// hashring that the provided key hashes closest to.
//
// The n closest member may require wrapping around to the first
// most member of the hashring and continuing to the next most
// members from that point onward.
func (c *consistentHashring) FindNearestN(
	key []byte,
	n int,
) ([]Member, error) {
	if len(c.ring) == 0 || len(c.ring) < n {
		return nil, ErrTooFewMembers
	}

	minKeyHash := c.memberKeys.Min().(UInt64Item)
	keyHash := c.hasher(key)

	// find the nearest (e.g. closest ascending) members of the hashring
	// with wrap around
	memberHashes := make([]UInt64Item, 0, n)

	c.memberKeys.AscendGreaterOrEqual(UInt64Item(keyHash), func(item btree.Item) bool {
		memberHashes = append(memberHashes, item.(UInt64Item))
		return len(memberHashes) != n
	})

	if len(memberHashes) < n {
		c.memberKeys.AscendGreaterOrEqual(minKeyHash, func(item btree.Item) bool {
			memberHashes = append(memberHashes, item.(UInt64Item))
			return len(memberHashes) != n
		})
	}

	if len(memberHashes) < n {
		return nil, ErrTooFewMembers
	}

	members := make([]Member, 0, n)
	for _, memberHash := range memberHashes {
		members = append(members, c.ring[uint64(memberHash)])
	}

	return members, nil
}

type UInt64Item uint64

func (a UInt64Item) Less(b btree.Item) bool {
	return a < b.(UInt64Item)
}
