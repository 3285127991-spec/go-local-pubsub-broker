// Package broker implements a lightweight in-process publish/subscribe
// message broker with hierarchical topics and wildcard matching.
package broker

import (
	"errors"
	"strings"
)

// ErrInvalidTopic is returned when a publish topic or subscription pattern
// does not follow the topic syntax rules.
var ErrInvalidTopic = errors.New("broker: invalid topic")

// isValidPublishTopic reports whether topic is a concrete topic name that
// publishers may use: non-empty, no wildcard segments.
func isValidPublishTopic(topic string) bool {
	if topic == "" {
		return false
	}
	for _, seg := range strings.Split(topic, "/") {
		if seg == "" || seg == "*" || seg == "#" {
			return false
		}
	}
	return true
}

// isValidPattern reports whether pattern is a legal subscription pattern:
// non-empty segments, "*" only as a whole segment, and "#" only as the
// final segment.
func isValidPattern(pattern string) bool {
	if pattern == "" {
		return false
	}
	segs := strings.Split(pattern, "/")
	for i, seg := range segs {
		switch {
		case seg == "":
			return false
		case seg == "#":
			if i != len(segs)-1 {
				return false
			}
		case strings.Contains(seg, "*") && seg != "*":
			return false
		case strings.Contains(seg, "#"):
			return false
		}
	}
	return true
}

// matchTopic reports whether the concrete topic matches the subscription
// pattern. "*" matches exactly one level; a trailing "#" matches zero or
// more remaining levels (so "a/#" also matches "a").
func matchTopic(pattern, topic string) bool {
	psegs := strings.Split(pattern, "/")
	tsegs := strings.Split(topic, "/")
	for i, pseg := range psegs {
		if pseg == "#" {
			return true
		}
		if i >= len(tsegs) {
			return false
		}
		if pseg != "*" && pseg != tsegs[i] {
			return false
		}
	}
	return len(psegs) == len(tsegs)
}
