package now

import "time"

// Time returns current time
func Time() int64 {
	return time.Now().Unix()
}
