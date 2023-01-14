package values

import "time"

type ValueEntry interface {
	// GetExpiry gets the expiry of the value entry
	GetExpiry() uint64

	// AsBytes retrieves the byte array that represents the value entry.
	AsBytes() []byte
}

// IsExpired returns true if key has lived for longer than its time-to-live
// It will always return false if time-to-live was never set
func IsExpired(v ValueEntry) bool {
	expiry := v.GetExpiry()
	if expiry == 0 {
		return false
	} else {
		return expiry < uint64(time.Now().Unix())
	}
}
