package wsshards

import (
	"testing"

	"github.com/davseby/wetsocks/wsclient/wsreconn"
	wsreconnMock "github.com/davseby/wetsocks/wsclient/wsreconn/_mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NewSoloSubKeeperStore(t *testing.T) {
	fmt := &wsreconnMock.SoloSubFormatter{}
	s := NewSoloSubKeeperStore(
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

func Test_SoloSubKeeperStore_Close(t *testing.T) {
	k1 := &soloSubKeeperMock{}
	k2 := &soloSubKeeperMock{}

	store := &SoloSubKeeperStore{
		keepers: []soloSubKeeper{
			k1,
			k2,
		},
	}

	store.Close()
	assert.Len(t, k1.CloseCalls(), 1)
	assert.Len(t, k2.CloseCalls(), 1)
}

func Test_SoloSubKeeperStore_SubKeepers(t *testing.T) {
	k1 := &soloSubKeeperMock{}
	k2 := &soloSubKeeperMock{}

	store := &SoloSubKeeperStore{
		keepers: []soloSubKeeper{
			k1,
			k2,
		},
	}

	keepers := store.SubKeepers()
	assert.Len(t, keepers, 2)
}

func Test_SoloSubKeeperStore_Subscribe(t *testing.T) {
	k1 := &soloSubKeeperMock{}
	k2 := &soloSubKeeperMock{}

	store := &SoloSubKeeperStore{
		keepers: []soloSubKeeper{
			k1,
			k2,
		},
	}

	store.Subscribe("1")
	assert.Len(t, k1.SubscribeCalls(), 0)
	assert.Len(t, k2.SubscribeCalls(), 1)

	store.Subscribe("2")
	assert.Len(t, k1.SubscribeCalls(), 1)
	assert.Len(t, k2.SubscribeCalls(), 1)
}

func Test_SoloSubKeeperStore_UnsubscribeAll(t *testing.T) {
	k1 := &soloSubKeeperMock{}
	k2 := &soloSubKeeperMock{}

	store := &SoloSubKeeperStore{
		keepers: []soloSubKeeper{
			k1,
			k2,
		},
	}

	store.UnsubscribeAll()
	assert.Len(t, k1.UnsubscribeAllCalls(), 1)
	assert.Len(t, k2.UnsubscribeAllCalls(), 1)
}

func Test_SoloSubKeeperStore_UnsubscribeLocal(t *testing.T) {
	k1 := &soloSubKeeperMock{}
	k2 := &soloSubKeeperMock{}

	store := &SoloSubKeeperStore{
		keepers: []soloSubKeeper{
			k1,
			k2,
		},
	}

	store.UnsubscribeLocal("1")
	assert.Len(t, k1.UnsubscribeLocalCalls(), 0)
	assert.Len(t, k2.UnsubscribeLocalCalls(), 1)

	store.UnsubscribeLocal("2")
	assert.Len(t, k1.UnsubscribeLocalCalls(), 1)
	assert.Len(t, k2.UnsubscribeLocalCalls(), 1)
}

func Test_SoloSubKeeperStore_Unsubscribe(t *testing.T) {
	k1 := &soloSubKeeperMock{}
	k2 := &soloSubKeeperMock{}

	store := &SoloSubKeeperStore{
		keepers: []soloSubKeeper{
			k1,
			k2,
		},
	}

	store.Unsubscribe("1")
	assert.Len(t, k1.UnsubscribeCalls(), 0)
	assert.Len(t, k2.UnsubscribeCalls(), 1)

	store.Unsubscribe("2")
	assert.Len(t, k1.UnsubscribeCalls(), 1)
	assert.Len(t, k2.UnsubscribeCalls(), 1)
}

func Test_SoloSubKeeperStore_ConfirmSub(t *testing.T) {
	k1 := &soloSubKeeperMock{}
	k2 := &soloSubKeeperMock{}

	store := &SoloSubKeeperStore{
		keepers: []soloSubKeeper{
			k1,
			k2,
		},
	}

	store.ConfirmSub("1")
	assert.Len(t, k1.ConfirmSubCalls(), 0)
	assert.Len(t, k2.ConfirmSubCalls(), 1)

	store.ConfirmSub("2")
	assert.Len(t, k1.ConfirmSubCalls(), 1)
	assert.Len(t, k2.ConfirmSubCalls(), 1)
}

func Test_SoloSubKeeperStore_ConfirmUnsub(t *testing.T) {
	k1 := &soloSubKeeperMock{}
	k2 := &soloSubKeeperMock{}

	store := &SoloSubKeeperStore{
		keepers: []soloSubKeeper{
			k1,
			k2,
		},
	}

	store.ConfirmUnsub("1")
	assert.Len(t, k1.ConfirmUnsubCalls(), 0)
	assert.Len(t, k2.ConfirmUnsubCalls(), 1)

	store.ConfirmUnsub("2")
	assert.Len(t, k1.ConfirmUnsubCalls(), 1)
	assert.Len(t, k2.ConfirmUnsubCalls(), 1)
}
