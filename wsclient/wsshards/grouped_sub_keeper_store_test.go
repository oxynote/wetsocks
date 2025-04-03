package wsshards

import (
	"testing"

	"github.com/jellydator/wetsocks/wsclient/wsreconn"
	wsreconnMock "github.com/jellydator/wetsocks/wsclient/wsreconn/_mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NewGroupedSubKeeperStore(t *testing.T) {
	fmt := &GroupedSubFormatterMock[any]{}
	s := NewGroupedSubKeeperStore(
		func(_ int) wsreconn.SubKeeperMetrics {
			return &wsreconnMock.SubKeeperMetrics{}
		},
		fmt,
		1,
		true,
		5,
	)
	require.NotNil(t, s)
	assert.Equal(t, 5, len(s.keepers))
}

func Test_GroupedSubKeeperStore_Close(t *testing.T) {
	k1 := &groupedSubKeeperMock[any]{}
	k2 := &groupedSubKeeperMock[any]{}

	store := &GroupedSubKeeperStore[any]{
		keepers: []groupedSubKeeper[any]{
			k1,
			k2,
		},
	}

	store.Close()
	assert.Len(t, k1.CloseCalls(), 1)
	assert.Len(t, k2.CloseCalls(), 1)
}

func Test_GroupedSubKeeperStore_SubKeepers(t *testing.T) {
	k1 := &groupedSubKeeperMock[any]{}
	k2 := &groupedSubKeeperMock[any]{}

	store := &GroupedSubKeeperStore[any]{
		keepers: []groupedSubKeeper[any]{
			k1,
			k2,
		},
	}

	keepers := store.SubKeepers()
	assert.Len(t, keepers, 2)
}

func Test_GroupedSubKeeperStore_Subscribe(t *testing.T) {
	k1 := &groupedSubKeeperMock[int]{}
	k2 := &groupedSubKeeperMock[int]{}

	store := &GroupedSubKeeperStore[int]{
		keepers: []groupedSubKeeper[int]{
			k1,
			k2,
		},
		fmt: &GroupedSubFormatterMock[int]{
			KeyHashFunc: func(key int) int {
				return key
			},
		},
	}

	store.Subscribe("1", 0)
	assert.Len(t, k1.SubscribeCalls(), 1)
	assert.Len(t, k2.SubscribeCalls(), 0)

	store.Subscribe("1", 1)
	assert.Len(t, k1.SubscribeCalls(), 1)
	assert.Len(t, k2.SubscribeCalls(), 1)
}

func Test_GroupedSubKeeperStore_UnsubscribeAll(t *testing.T) {
	k1 := &groupedSubKeeperMock[any]{}
	k2 := &groupedSubKeeperMock[any]{}

	store := &GroupedSubKeeperStore[any]{
		keepers: []groupedSubKeeper[any]{
			k1,
			k2,
		},
	}

	store.UnsubscribeAll()

	assert.Len(t, k1.UnsubscribeAllCalls(), 1)
	assert.Len(t, k2.UnsubscribeAllCalls(), 1)
}

func Test_GroupedSubKeeperStore_Unsubscribe(t *testing.T) {
	k1 := &groupedSubKeeperMock[int]{}
	k2 := &groupedSubKeeperMock[int]{}

	store := &GroupedSubKeeperStore[int]{
		keepers: []groupedSubKeeper[int]{
			k1,
			k2,
		},
		fmt: &GroupedSubFormatterMock[int]{
			KeyHashFunc: func(key int) int {
				return key
			},
		},
	}

	store.Unsubscribe("1", 0)
	assert.Len(t, k1.UnsubscribeCalls(), 1)
	assert.Len(t, k2.UnsubscribeCalls(), 0)

	store.Unsubscribe("1", 1)
	assert.Len(t, k1.UnsubscribeCalls(), 1)
	assert.Len(t, k2.UnsubscribeCalls(), 1)
}

func Test_GroupedSubKeeperStore_ConfirmSub(t *testing.T) {
	k1 := &groupedSubKeeperMock[int]{}
	k2 := &groupedSubKeeperMock[int]{}

	store := &GroupedSubKeeperStore[int]{
		keepers: []groupedSubKeeper[int]{
			k1,
			k2,
		},
		fmt: &GroupedSubFormatterMock[int]{
			KeyHashFunc: func(key int) int {
				return key
			},
		},
	}

	store.ConfirmSub("1", 0)
	assert.Len(t, k1.ConfirmSubCalls(), 1)
	assert.Len(t, k2.ConfirmSubCalls(), 0)

	store.ConfirmSub("1", 1)
	assert.Len(t, k1.ConfirmSubCalls(), 1)
	assert.Len(t, k2.ConfirmSubCalls(), 1)
}

func Test_GroupedSubKeeperStore_ConfirmUnsub(t *testing.T) {
	k1 := &groupedSubKeeperMock[int]{}
	k2 := &groupedSubKeeperMock[int]{}

	store := &GroupedSubKeeperStore[int]{
		keepers: []groupedSubKeeper[int]{
			k1,
			k2,
		},
		fmt: &GroupedSubFormatterMock[int]{
			KeyHashFunc: func(key int) int {
				return key
			},
		},
	}

	store.ConfirmUnsub("1", false, 0)
	assert.Len(t, k1.ConfirmUnsubCalls(), 1)
	assert.Len(t, k2.ConfirmUnsubCalls(), 0)
	assert.Equal(t, k1.ConfirmUnsubCalls()[0].Topic, "1")
	assert.Equal(t, k1.ConfirmUnsubCalls()[0].Keys, []int{0})

	store.ConfirmUnsub("1", false, 1, 3, 5)
	assert.Len(t, k1.ConfirmUnsubCalls(), 1)
	assert.Len(t, k2.ConfirmUnsubCalls(), 1)
	assert.Equal(t, k2.ConfirmUnsubCalls()[0].Topic, "1")
	assert.Equal(t, k2.ConfirmUnsubCalls()[0].Keys, []int{1, 3, 5})
}
