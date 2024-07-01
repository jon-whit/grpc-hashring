package hashring_test

import (
	"math/rand"
	"testing"

	"github.com/jon-whit/grpc-hashring/hashring"
	mock_hashring "github.com/jon-whit/grpc-hashring/hashring/mocks"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestConsistentHashring(t *testing.T) {
	t.Run("hashring_too_few_members", func(t *testing.T) {
		mockController := gomock.NewController(t)
		t.Cleanup(mockController.Finish)

		mockHasher := mock_hashring.NewMockHasher(mockController)
		mockHasher.EXPECT().Sum64(gomock.Any()).Return(rand.Uint64()).AnyTimes()

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
		mockController := gomock.NewController(t)
		t.Cleanup(mockController.Finish)

		mockHasher := mock_hashring.NewMockHasher(mockController)

		count := uint64(0)
		mockHasher.EXPECT().Sum64(gomock.Any()).DoAndReturn(func(bytes []byte) uint64 {
			count++
			return count
		}).AnyTimes()

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
