package types

// RouterType is a type that represents whether an event is incoming or from a peer.
type RouterType bool

const (
	RouterTypeIncoming RouterType = true
	RouterTypePeer     RouterType = false
)

func (rt RouterType) String() string {
	if rt {
		return "incoming"
	}
	return "peer"
}

func (rt RouterType) IsIncoming() bool {
	return rt == RouterTypeIncoming
}

type TransmitType bool

const (
	TransmitTypeUpstream TransmitType = true
	TransmitTypePeer     TransmitType = false
)

func (tt TransmitType) String() string {
	if tt {
		return "upstream"
	}
	return "peer"
}
