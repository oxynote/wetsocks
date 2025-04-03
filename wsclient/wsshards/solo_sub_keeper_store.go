package wsshards

import (
	"time"

	"github.com/jellydator/wetsocks/wsclient/wsreconn"
)

// SoloSubKeeperStore holds sub keepers whose payloads are sent one by one.
type SoloSubKeeperStore struct {
	keepers []soloSubKeeper
}

// NewSoloSubKeeperStore creates a fresh instance of SoloSubKeeperStore.
// The duration parameter determines how long to wait between payload events.
// The bool parameter determines if a manual confirmation should be
// expected or not.
// The size parameter determines how many sub keepers should be created.
func NewSoloSubKeeperStore(
	metricsFn func(connectionNumber int) wsreconn.SubKeeperMetrics,
	fmt wsreconn.SoloSubFormatter,
	cooldown time.Duration,
	manualConfirm bool,
	size int,
) *SoloSubKeeperStore {
	store := &SoloSubKeeperStore{
		keepers: make([]soloSubKeeper, size),
	}

	for i := 0; i < size; i++ {
		store.keepers[i] = wsreconn.NewSoloSubKeeper(
			metricsFn(i+1),
			fmt,
			cooldown,
			manualConfirm,
		)
	}

	return store
}

// Close stops all background processes.
func (ssks *SoloSubKeeperStore) Close() {
	for _, keeper := range ssks.keepers {
		keeper.Close()
	}
}

// SubKeepers returns a slice of sub keepers.
func (ssks *SoloSubKeeperStore) SubKeepers() []wsreconn.SubKeeper {
	res := make([]wsreconn.SubKeeper, len(ssks.keepers))

	for i, keeper := range ssks.keepers {
		res[i] = keeper
	}

	return res
}

// Subscribe subscribes to the provided topic.
func (ssks *SoloSubKeeperStore) Subscribe(topic string) {
	ssks.keepers[deriveTopicIndex(topic, len(ssks.keepers))].Subscribe(topic)
}

// UnsubscribeAll unsubscribes from all topics.
func (ssks *SoloSubKeeperStore) UnsubscribeAll() {
	for _, keeper := range ssks.keepers {
		keeper.UnsubscribeAll()
	}
}

// UnsubscribeLocal works the same way as Unsubscribe but without triggering
// an unsubscription event. In other words, it just deletes the subscription
// locally. It's useful in situations when the server sends a subscription
// drop event after a successful subscription.
func (ssks *SoloSubKeeperStore) UnsubscribeLocal(topic string) {
	ssks.keepers[deriveTopicIndex(topic, len(ssks.keepers))].UnsubscribeLocal(topic)
}

// Unsubscribe unsubscribes from the provided topic.
func (ssks *SoloSubKeeperStore) Unsubscribe(topic string) {
	ssks.keepers[deriveTopicIndex(topic, len(ssks.keepers))].Unsubscribe(topic)
}

// ConfirmSub manually confirms a subcription.
func (ssks *SoloSubKeeperStore) ConfirmSub(topic string) {
	ssks.keepers[deriveTopicIndex(topic, len(ssks.keepers))].ConfirmSub(topic)
}

// ConfirmUnsub manually confirms an unsubcription.
func (ssks *SoloSubKeeperStore) ConfirmUnsub(topic string) {
	ssks.keepers[deriveTopicIndex(topic, len(ssks.keepers))].ConfirmUnsub(topic)
}

// deriveTopicIndex derives the key index for the provided topic.
func deriveTopicIndex(topic string, total int) int {
	var key int

	for i := 0; i < len(topic); i++ {
		key += int(topic[i])
	}

	return key % total
}

// soloSubKeeper is an interface that handles topics.
//
//go:generate ../../scripts/codegen/mock -t internal soloSubKeeper
type soloSubKeeper interface {
	wsreconn.SubKeeper

	// Subscribe should subscribe to the provided topic.
	Subscribe(topic string)

	// UnsubscribeAll should unsubscribe from all topics.
	UnsubscribeAll()

	// UnsubscribeLocal should unsubscribe from the provided topic
	// without triggering an unsubscription event.
	UnsubscribeLocal(topic string)

	// Unsubscribe should unsubscribe from the provided topic.
	Unsubscribe(topic string)

	// ConfirmSub should manually confirm a subscription.
	ConfirmSub(topic string)

	// ConfirmUnsub should manually confirm an unsubscription.
	ConfirmUnsub(topic string)

	// Close should stop all the client-related processes.
	Close()
}
