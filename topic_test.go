package pubsubbroker

import "testing"

func TestMatchFilter(t *testing.T) {
	cases := []struct {
		filter string
		topic  string
		want   bool
	}{
		// Exact matches.
		{"a", "a", true},
		{"a/b/c", "a/b/c", true},
		{"a/b", "a/b/c", false},
		{"a/b/c", "a/b", false},
		{"a/b", "a/c", false},

		// "*" matches exactly one level.
		{"a/*/c", "a/b/c", true},
		{"a/*/c", "a/x/c", true},
		{"a/*/c", "a/c", false},
		{"a/*/c", "a/b/d/c", false},
		{"*/b", "a/b", true},
		{"*/b", "a/c", false},
		{"*", "a", true},
		{"*", "a/b", false},

		// Trailing "#" matches zero or more remaining levels.
		{"a/#", "a", true},
		{"a/#", "a/b", true},
		{"a/#", "a/b/c/d", true},
		{"a/#", "b", false},
		{"a/#", "ab", false},
		{"#", "a", true},
		{"#", "a/b/c", true},

		// Wildcards combined.
		{"a/*/#", "a/b", true},
		{"a/*/#", "a/b/c/d", true},
		{"a/*/#", "a", false},
		{"*/#", "a/b", true},

		// Empty levels are ordinary levels.
		{"a//b", "a//b", true},
		{"a//b", "a/x/b", false},
		{"a/*/b", "a//b", true},
	}
	for _, c := range cases {
		if got := matchFilter(c.filter, c.topic); got != c.want {
			t.Errorf("matchFilter(%q, %q) = %v, want %v", c.filter, c.topic, got, c.want)
		}
	}
}

func TestValidateFilter(t *testing.T) {
	valid := []string{"a", "a/b/c", "*", "#", "a/*", "a/#", "a/*/c", "a/*/#", "//"}
	for _, f := range valid {
		if err := validateFilter(f); err != nil {
			t.Errorf("validateFilter(%q) returned unexpected error: %v", f, err)
		}
	}
	invalid := []string{"", "a/#/b", "#/a", "a#", "a/b#", "a*", "a*b", "*/a#"}
	for _, f := range invalid {
		if err := validateFilter(f); err == nil {
			t.Errorf("validateFilter(%q) = nil, want error", f)
		}
	}
}

func TestValidateTopic(t *testing.T) {
	valid := []string{"a", "a/b/c", "//"}
	for _, topic := range valid {
		if err := validateTopic(topic); err != nil {
			t.Errorf("validateTopic(%q) returned unexpected error: %v", topic, err)
		}
	}
	invalid := []string{"", "a/#", "a/*", "#", "*", "a#b", "a*b"}
	for _, topic := range invalid {
		if err := validateTopic(topic); err == nil {
			t.Errorf("validateTopic(%q) = nil, want error", topic)
		}
	}
}
