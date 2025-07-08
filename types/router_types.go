package types

// RouterType defines which type of traffic a component is handling.
type RouterType string

const (
	RouterTypeIncoming RouterType = "incoming"
	RouterTypePeer     RouterType = "peer"
)

func (rt RouterType) String() string {
	return string(rt)
}

func (rt RouterType) IsIncoming() bool {
	return rt == RouterTypeIncoming
}

func (rt RouterType) IsPeer() bool {
	return rt == RouterTypePeer
}
