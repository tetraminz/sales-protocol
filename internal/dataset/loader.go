package dataset

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Turn is a single utterance in a conversation.
type Turn struct {
	TurnID  int    `json:"turn_id"`
	Speaker string `json:"speaker"`
	Text    string `json:"text"`
}

// Conversation contains parsed turns plus lightweight metadata.
type Conversation struct {
	ConversationID string
	CompanyKey     string
	SourceFile     string
	Turns          []Turn
	RawTranscript  string
}

// LoadConversations reads CSV files from inputDir and returns parsed conversations.
func LoadConversations(inputDir, filterPrefix string, limit int) ([]Conversation, error) {
	if strings.TrimSpace(inputDir) == "" {
		return nil, errors.New("input directory is required")
	}
	if limit < 0 {
		return nil, errors.New("limit must be >= 0")
	}

	paths, err := listCSVFiles(inputDir)
	if err != nil {
		return nil, err
	}

	conversations := make([]Conversation, 0, len(paths))
	for _, path := range paths {
		base := filepath.Base(path)
		if filterPrefix != "" && !strings.HasPrefix(base, filterPrefix) {
			continue
		}

		conversation, err := LoadConversationFile(path)
		if err != nil {
			return nil, err
		}

		conversations = append(conversations, conversation)
		if limit > 0 && len(conversations) >= limit {
			break
		}
	}

	return conversations, nil
}

// LoadConversationFile parses one conversation CSV file.
func LoadConversationFile(path string) (Conversation, error) {
	file, err := os.Open(path)
	if err != nil {
		return Conversation{}, fmt.Errorf("open %q: %w", path, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1

	header, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Conversation{}, fmt.Errorf("read %q: empty csv", path)
		}
		return Conversation{}, fmt.Errorf("read %q header: %w", path, err)
	}

	idx, err := headerIndexes(header)
	if err != nil {
		return Conversation{}, fmt.Errorf("parse %q header: %w", path, err)
	}

	var conversationID string
	turns := make([]Turn, 0, 32)

	for {
		record, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return Conversation{}, fmt.Errorf("read %q row: %w", path, err)
		}

		chunkIDRaw := strings.TrimSpace(valueAt(record, idx.chunkID))
		if chunkIDRaw == "" {
			continue
		}

		chunkID, err := strconv.Atoi(chunkIDRaw)
		if err != nil {
			return Conversation{}, fmt.Errorf("parse %q chunk_id %q: %w", path, chunkIDRaw, err)
		}

		speaker := strings.TrimSpace(valueAt(record, idx.speaker))
		text := strings.TrimSpace(valueAt(record, idx.text))
		if speaker == "" && text == "" {
			continue
		}

		if conversationID == "" {
			candidate := strings.TrimSpace(valueAt(record, idx.conversation))
			if candidate != "" {
				conversationID = candidate
			}
		}

		turns = append(turns, Turn{
			TurnID:  chunkID,
			Speaker: speaker,
			Text:    text,
		})
	}

	if len(turns) == 0 {
		return Conversation{}, fmt.Errorf("parse %q: no turns", path)
	}

	sort.SliceStable(turns, func(i, j int) bool {
		return turns[i].TurnID < turns[j].TurnID
	})

	if conversationID == "" {
		conversationID = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	}

	companyKey := conversationID
	if idx := strings.Index(conversationID, "__"); idx > 0 {
		companyKey = conversationID[:idx]
	}

	return Conversation{
		ConversationID: conversationID,
		CompanyKey:     companyKey,
		SourceFile:     filepath.ToSlash(path),
		Turns:          turns,
		RawTranscript:  buildRawTranscript(turns),
	}, nil
}

func listCSVFiles(root string) ([]string, error) {
	paths := make([]string, 0, 256)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if strings.EqualFold(filepath.Ext(path), ".csv") {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk %q: %w", root, err)
	}

	sort.Strings(paths)
	return paths, nil
}

func buildRawTranscript(turns []Turn) string {
	lines := make([]string, 0, len(turns))
	for _, turn := range turns {
		lines = append(lines, fmt.Sprintf("%s: %s", turn.Speaker, turn.Text))
	}
	return strings.Join(lines, "\n")
}

func valueAt(record []string, index int) string {
	if index < 0 || index >= len(record) {
		return ""
	}
	return record[index]
}

type requiredIndexes struct {
	conversation int
	chunkID      int
	speaker      int
	text         int
}

func headerIndexes(header []string) (requiredIndexes, error) {
	idx := requiredIndexes{
		conversation: -1,
		chunkID:      -1,
		speaker:      -1,
		text:         -1,
	}

	for i, col := range header {
		normalized := normalizeHeader(col)
		switch normalized {
		case "conversation":
			idx.conversation = i
		case "chunk_id", "chunkid":
			idx.chunkID = i
		case "speaker":
			idx.speaker = i
		case "text":
			idx.text = i
		}
	}

	if idx.conversation == -1 || idx.chunkID == -1 || idx.speaker == -1 || idx.text == -1 {
		return requiredIndexes{}, fmt.Errorf("missing required columns in header %v", header)
	}
	return idx, nil
}

func normalizeHeader(s string) string {
	s = strings.TrimSpace(strings.TrimPrefix(s, "\ufeff"))
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, " ", "")
	return s
}
