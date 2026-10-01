package node

import "time"

func wallMillis() int64 { return time.Now().UnixMilli() }
