package hashring_test

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/jon-whit/grpc-hashring/hashring"
	"github.com/stretchr/testify/require"
)

func TestConsistentHashring(t *testing.T) {
	t.Run("hashring_too_few_members", func(t *testing.T) {
		mockHasher := func(b []byte) uint64 {
			return rand.Uint64()
		}

		ring := hashring.NewConsistentHashring(mockHasher, hashring.DefaultConsistentHashringConfig())
		members, err := ring.FindNearestN([]byte("foo"), 1)
		require.ErrorIs(t, err, hashring.ErrTooFewMembers)
		require.Nil(t, members)

		ring.AddMember(hashring.StringMember("a"))
		members, err = ring.FindNearestN([]byte("foo"), 2)
		require.ErrorIs(t, err, hashring.ErrTooFewMembers)
		require.Nil(t, members)
	})

	t.Run("hashring_wraps_around", func(t *testing.T) {
		count := uint64(0)
		mockHasher := func(b []byte) uint64 {
			count++
			return count
		}

		ring := hashring.NewConsistentHashring(mockHasher, hashring.DefaultConsistentHashringConfig())
		ring.AddMember(hashring.StringMember("a"))
		ring.AddMember(hashring.StringMember("b"))
		ring.AddMember(hashring.StringMember("c"))

		members, err := ring.FindNearestN([]byte("foo"), 2)
		require.NoError(t, err)
		require.Len(t, members, 2)

		expected := []hashring.Member{
			hashring.StringMember("a"),
			hashring.StringMember("b"),
		}
		require.Equal(t, expected, members)
	})
}

func BenchmarkConsistentHashring_AddMember(b *testing.B) {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))

	ring := hashring.NewConsistentHashring(xxhash.Sum64, hashring.DefaultConsistentHashringConfig())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := rand.Intn(10000000)
		ring.AddMember(hashring.StringMember(strconv.Itoa(n)))
	}
}
func BenchmarkConsistentHashring_RemoveMember(b *testing.B) {}
func BenchmarkConsistentHashring_FindNearestN(b *testing.B) {}
