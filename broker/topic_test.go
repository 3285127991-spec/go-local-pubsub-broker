package broker

import "testing"

func TestMatchTopic(t *testing.T) {
	cases := []struct {
		pattern string
		topic   string
		want    bool
	}{
		// Exact matching.
		{"a/b", "a/b", true},
		{"a/b", "a/c", false},
		{"a/b", "a/b/c", false},
		{"a/b", "a", false},
		{"a", "a", true},

		// "*" matches exactly one level.
		{"a/*", "a/b", true},
		{"a/*", "a/b/c", false},
		{"a/*", "a", false},
		{"*", "a", true},
		{"*", "a/b", false},
		{"a/*/c", "a/b/c", true},
		{"a/*/c", "a/b/d", false},
		{"a/*/c", "a/c", false},
		{"*/b", "a/b", true},
		{"*/*", "a/b", true},
		{"*/*", "a/b/c", false},

		// Trailing "#" matches zero or more remaining levels.
		{"#", "a", true},
		{"#", "a/b/c", true},
		{"a/#", "a", true},
		{"a/#", "a/b", true},
		{"a/#", "a/b/c", true},
		{"a/#", "b/a", false},
		{"a/b/#", "a/b", true},
		{"a/b/#", "a/b/c/d", true},
		{"a/b/#", "a/c", false},
		{"a/*/#", "a/b/c/d", true},
		{"a/*/#", "a/b", true},
		{"a/*/#", "a", false},
	}
	for _, tc := range cases {
		if got := matchTopic(tc.pattern, tc.topic); got != tc.want {
			t.Errorf("matchTopic(%q, %q) = %v, want %v", tc.pattern, tc.topic, got, tc.want)
		}
	}
}

func TestIsValidPattern(t *testing.T) {
	valid := []string{"a", "a/b", "*", "a/*", "#", "a/#", "a/*/#", "a/b/c"}
	for _, p := range valid {
		if !isValidPattern(p) {
			t.Errorf("isValidPattern(%q) = false, want true", p)
		}
	}
	invalid := []string{"", "a//b", "/a", "a/", "a/#/b", "a/b#", "a/b*", "#/a", "a*"}
	for _, p := range invalid {
		if isValidPattern(p) {
			t.Errorf("isValidPattern(%q) = true, want false", p)
		}
	}
}

func TestIsValidPublishTopic(t *testing.T) {
	valid := []string{"a", "a/b", "a/b/c", "x1/y2"}
	for _, topic := range valid {
		if !isValidPublishTopic(topic) {
			t.Errorf("isValidPublishTopic(%q) = false, want true", topic)
		}
	}
	invalid := []string{"", "a/#", "a/*", "*", "#", "a//b", "/a", "a/"}
	for _, topic := range invalid {
		if isValidPublishTopic(topic) {
			t.Errorf("isValidPublishTopic(%q) = true, want false", topic)
		}
	}
}
