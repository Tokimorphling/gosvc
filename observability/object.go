package observability

import "time"

// ObserveObjectStorage implements object.Observer. Labels are operation/result
// categories only: object names, URLs, credentials and bucket contents never
// create metric series.
func (m *Metrics) ObserveObjectStorage(operation, outcome string, elapsed time.Duration) {
	if m == nil {
		return
	}
	switch operation {
	case "put", "get", "head", "delete", "list", "presign_get", "presign_put", "head_bucket":
	default:
		operation = "unknown"
	}
	switch outcome {
	case "ok", "invalid_argument", "unauthenticated", "permission_denied", "not_found", "conflict", "rate_limited", "unavailable", "internal":
	default:
		outcome = "unknown"
	}
	m.objectRequests.WithLabelValues(operation, outcome).Inc()
	m.objectDuration.WithLabelValues(operation).Observe(elapsed.Seconds())
}
