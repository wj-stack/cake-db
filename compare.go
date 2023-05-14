package cake_db

//      // < 0 when a < b
//      // == 0 when a == b
//      // > 0 when a > b
//      Compare(a T, b T) int

type DataCompare struct {
}

func (c *DataCompare) Compare(a *Point, b *Point) int {

	if a.DeviceId < b.DeviceId {
		return -1
	} else if a.DeviceId > b.DeviceId {
		return 1
	}

	if a.Timestamp < b.Timestamp {
		return -1
	} else if a.Timestamp > b.Timestamp {
		return 1
	}

	return 0
}
