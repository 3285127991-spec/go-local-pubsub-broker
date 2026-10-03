// Package pubsubbroker implements a lightweight in-process publish/subscribe
// message broker with hierarchical topics and wildcard filters.
package pubsubbroker

import (
	"fmt"
	"strings"
)

// validateFilter reports whether filter is a well-formed subscription filter.
// A filter is a non-empty, "/"-separated list of levels where "*" may occupy
// a whole level (matching exactly one level) and "#" may occupy the final
// level (matching all remaining levels, including zero).
func validateFilter(filter string) error {
	if filter == "" {
		return fmt.Errorf("pubsubbroker: topic filter must not be empty")
	}
	levels := strings.Split(filter, "/")
	for i, level := range levels {
		switch {
		case level == "#":
			if i != len(levels)-1 {
				return fmt.Errorf("pubsubbroker: # must be the last level of filter %q", filter)
			}
		case strings.Contains(level, "#"):
			return fmt.Errorf("pubsubbroker: # must occupy a whole level in filter %q", filter)
		case level == "*":
		case strings.Contains(level, "*"):
			return fmt.Errorf("pubsubbroker: * must occupy a whole level in filter %q", filter)
		}
	}
	return nil
}

// validateTopic reports whether topic is a well-formed publish topic. Publish
// topics must be non-empty and must not contain wildcard characters.
func validateTopic(topic string) error {
	if topic == "" {
		return fmt.Errorf("pubsubbroker: topic must not be empty")
	}
	if strings.ContainsAny(topic, "*#") {
		return fmt.Errorf("pubsubbroker: topic %q must not contain wildcards", topic)
	}
	return nil
}

// matchFilter reports whether topic matches the given filter. Both filter and
// topic are assumed to be valid. "#" as the final filter level matches zero or
// more remaining topic levels; "*" matches exactly one level.
func matchFilter(filter, topic string) bool {
	flevels := strings.Split(filter, "/")
	tlevels := strings.Split(topic, "/")
	for i, fl := range flevels {
		if fl == "#" {
			return true
		}
		if i >= len(tlevels) {
			return false
		}
		if fl != "*" && fl != tlevels[i] {
			return false
		}
	}
	return len(flevels) == len(tlevels)
}
