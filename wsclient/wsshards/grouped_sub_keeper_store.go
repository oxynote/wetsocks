package wsshards

import (
	"time"

	"github.com/jellydator/wetsocks/wsclient/wsreconn"
)

// GroupedSubKeeperStore holds sub keepers whose payloads are sent in groups.
type GroupedSubKeeperStore[K comparable] struct {
	keepers []groupedSubKeeper[K]
	fmt     GroupedSubFormatter[K]
}

// NewGroupedSubKeeperStore creates a fresh instance of GroupedSubKeeperStore.
// The duration parameter determines how long to wait between payload events.
// The bool parameter determines if a manual confirmation should be
// expected or not.
// The size parameter determines how many sub keepers should be created.
func NewGroupedSubKeeperStore[K comparable](
	metrics wsreconn.SubKeeperMetrics,
	fmt GroupedSubFormatter[K],
	cooldown time.Duration,
	manualConfirm bool,
	size int,
) *GroupedSubKeeperStore[K] {
	store := &GroupedSubKeeperStore[K]{
		keepers: make([]groupedSubKeeper[K], size),
		fmt:     fmt,
	}

	for i := 0; i < size; i++ {
		store.keepers[i] = wsreconn.NewGroupedSubKeeper(
			metrics,
			fmt,
			cooldown,
			manualConfirm,
		)
	}

	return store
}

// Close stops all background processes.
func (gsks *GroupedSubKeeperStore[K]) Close() {
	for _, keeper := range gsks.keepers {
		keeper.Close()
	}
}

// SubKeepers returns a slice of sub keepers.
func (gsks *GroupedSubKeeperStore[K]) SubKeepers() []wsreconn.SubKeeper {
	res := make([]wsreconn.SubKeeper, len(gsks.keepers))

	for i, keeper := range gsks.keepers {
		res[i] = keeper
	}

	return res
}

// Subscribe subscribes to the provided topic.
func (gsks *GroupedSubKeeperStore[K]) Subscribe(topic string, keys ...K) {
	for i, group := range gsks.deriveKeyGroups(keys...) {
		gsks.keepers[i].Subscribe(topic, group...)
	}
}

// UnsubscribeAll unsubscribes from all topics.
func (gsks *GroupedSubKeeperStore[K]) UnsubscribeAll() {
	for _, keeper := range gsks.keepers {
		keeper.UnsubscribeAll()
	}
}

// Unsubscribe unsubscribes from the provided topic.
func (gsks *GroupedSubKeeperStore[K]) Unsubscribe(topic string, keys ...K) {
	for i, group := range gsks.deriveKeyGroups(keys...) {
		gsks.keepers[i].Unsubscribe(topic, group...)
	}
}

// ConfirmSub manually confirms a subcription.
func (gsks *GroupedSubKeeperStore[K]) ConfirmSub(topic string, keys ...K) {
	for i, group := range gsks.deriveKeyGroups(keys...) {
		gsks.keepers[i].ConfirmSub(topic, group...)
	}
}

// ConfirmUnsub manually confirms an unsubcription.
func (gsks *GroupedSubKeeperStore[K]) ConfirmUnsub(topic string, invert bool, keys ...K) {
	for i, group := range gsks.deriveKeyGroups(keys...) {
		gsks.keepers[i].ConfirmUnsub(topic, invert, group...)
	}
}

// deriveKeyGroups derives the key groups.
func (gsks *GroupedSubKeeperStore[K]) deriveKeyGroups(keys ...K) map[int][]K {
	groups := make(map[int][]K)

	for _, key := range keys {
		index := gsks.fmt.KeyHash(key) % len(gsks.keepers)

		groups[index] = append(groups[index], key)
	}

	return groups
}

// groupedSubKeeper is an interface that handles topics.
//
//go:generate ../../scripts/codegen/mock -t internal groupedSubKeeper
type groupedSubKeeper[K comparable] interface {
	wsreconn.SubKeeper

	// Subscribe should subscribe to the provided topic.
	Subscribe(topic string, keys ...K)

	// UnsubscribeAll should unsubscribe from all topics.
	UnsubscribeAll()

	// Unsubscribe should unsubscribe from the provided topic.
	Unsubscribe(topic string, keys ...K)

	// ConfirmSub should manually confirm a subscription.
	ConfirmSub(topic string, keys ...K)

	// ConfirmUnsub should manually confirm an unsubscription.
	ConfirmUnsub(topic string, invert bool, keys ...K)

	// Close should stop all the client-related processes.
	Close()
}

// GroupedSubFormatter is an interface that handles grouped subscribers'
// message formatting.
//
//go:generate ../../scripts/codegen/mock -t internal GroupedSubFormatter
type GroupedSubFormatter[K comparable] interface {
	wsreconn.GroupedSubFormatter[K]

	// KeyHash should return the hash of the provided key.
	KeyHash(k K) int
}
