package main

import (
	"context"
	"errors"
	"fmt"
)

// RollbackStrategy restores the canvas to the state immediately after targetID.
// Implementations can use different reconstruction strategies (reverse undo or snapshot-and-replay)
type RollbackStrategy interface {
	Name() string
	Rollback(ctx context.Context, targetID string) error
}

var (
	ErrInvalidRollbackTarget = errors.New("rollback target event does not exist")
	ErrUnsupportedEvent      = errors.New("unsupported event type in rollback history")
)

// Verifies that targetID (LoggedEvent ID) is an actual event in the stream.
// The target event itself is retained, only events after it are undone.
func validateRollbackTarget(ctx context.Context, log *EventLog, targetID string) error {
	if targetID == "" {
		return fmt.Errorf("empty rollback target: %w", ErrInvalidRollbackTarget)
	}

	events, err := log.Range(ctx, targetID, targetID, 1)
	if err != nil {
		return fmt.Errorf("read rollback target %q: %w", targetID, err)
	}
	if len(events) == 0 || events[0].ID != targetID {
		return fmt.Errorf("event %q: %w", targetID, ErrInvalidRollbackTarget)
	}
	return nil
}
