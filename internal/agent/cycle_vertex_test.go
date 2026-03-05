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

func TestRepairTruncatedJSON(t *testing.T) {
	in := `{"decision":"call_tool","tool":"market.sync.batch","args":{"limit":10}`
	got := repairTruncatedJSON(in)
	want := `{"decision":"call_tool","tool":"market.sync.batch","args":{"limit":10}}`
	if got != want {
		t.Fatalf("repairTruncatedJSON()=%q want=%q", got, want)
	}
}

func TestDecodeVertexJSON_WithTruncatedPayload(t *testing.T) {
	raw := "```json\n{\"decision\":\"finish\",\"summary\":\"ok\"\n```"
	var out cycleActionOutput
	if err := decodeVertexJSON(raw, &out); err != nil {
		t.Fatalf("decodeVertexJSON() unexpected err: %v", err)
	}
	if out.Decision != "finish" {
		t.Fatalf("decision=%q want=finish", out.Decision)
	}
}
