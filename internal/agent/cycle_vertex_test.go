package agent

import "testing"

func TestExtractJSONObject(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "plain json",
			in:   `{"a":1}`,
			want: `{"a":1}`,
		},
		{
			name: "markdown code fence",
			in:   "```json\n{\"a\":1}\n```",
			want: `{"a":1}`,
		},
		{
			name: "prefix and suffix text",
			in:   "Here is output:\n{\"a\":1}\nthanks",
			want: `{"a":1}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractJSONObject(tt.in)
			if got != tt.want {
				t.Fatalf("extractJSONObject()=%q want=%q", got, tt.want)
			}
		})
	}
}
