package domain

import "errors"

// PermanentDelivery marks a failure that retrying the same immutable message
// cannot repair. The dispatcher stops visibly; no delivery ACK is implied.
// Operators may repair transport configuration and restart with the pending row.
func PermanentDelivery(err error) error {
	if err == nil || IsPermanentDelivery(err) {
		return err
	}
	return &permanentDeliveryError{err}
}

type permanentDeliveryError struct{ error }

func (e *permanentDeliveryError) Unwrap() error { return e.error }
func IsPermanentDelivery(err error) bool {
	var e *permanentDeliveryError
	return errors.As(err, &e)
}
