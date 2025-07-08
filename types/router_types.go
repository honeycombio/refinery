package types

// RouterType defines which type of traffic a component is handling.
type RouterType int

const (
	RouterTypeIncoming RouterType = iota
	RouterTypePeer
)

func (rt RouterType) String() string {
	switch rt {
	case RouterTypeIncoming:
		return "incoming"
	case RouterTypePeer:
		return "peer"
	default:
		return "unknown"
	}
}

func (rt RouterType) IsIncoming() bool {
	return rt == RouterTypeIncoming
}

func (rt RouterType) IsPeer() bool {
	return rt == RouterTypePeer
}
