package dataset

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConversationFileOrdersTurnsByChunkID(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "modamart__0_transcript.csv")
	content := "" +
		"Conversation,Chunk_id,Speaker,Text,Embedding\n" +
		"modamart__0_transcript,2,Customer,Second turn,[]\n" +
		"modamart__0_transcript,0,Sales Rep,First turn,[]\n" +
		"modamart__0_transcript,1,Sales Rep,Middle turn,[]\n"

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	conversation, err := LoadConversationFile(path)
	if err != nil {
		t.Fatalf("LoadConversationFile error: %v", err)
	}

	if got, want := conversation.ConversationID, "modamart__0_transcript"; got != want {
		t.Fatalf("conversation id mismatch: got %q want %q", got, want)
	}
	if got, want := conversation.CompanyKey, "modamart"; got != want {
		t.Fatalf("company key mismatch: got %q want %q", got, want)
	}
	if got, want := len(conversation.Turns), 3; got != want {
		t.Fatalf("turn count mismatch: got %d want %d", got, want)
	}
	if got, want := conversation.Turns[0].TurnID, 0; got != want {
		t.Fatalf("first turn_id mismatch: got %d want %d", got, want)
	}
	if got, want := conversation.Turns[1].TurnID, 1; got != want {
		t.Fatalf("second turn_id mismatch: got %d want %d", got, want)
	}
	if got, want := conversation.Turns[2].TurnID, 2; got != want {
		t.Fatalf("third turn_id mismatch: got %d want %d", got, want)
	}
}
