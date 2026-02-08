package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func findCSVFiles(inputDir string) ([]string, error) {
	files, err := filepath.Glob(filepath.Join(inputDir, "*.csv"))
	if err != nil {
		return nil, fmt.Errorf("glob csv files: %w", err)
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no csv files found in %s", inputDir)
	}
	return files, nil
}

func selectCSVRange(files []string, fromIdx, toIdx int) ([]string, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("no files available")
	}
	if fromIdx < 1 {
		return nil, fmt.Errorf("from_idx must be >= 1")
	}
	if toIdx < fromIdx {
		return nil, fmt.Errorf("to_idx must be >= from_idx")
	}
	if toIdx > len(files) {
		return nil, fmt.Errorf("to_idx (%d) is out of range, max=%d", toIdx, len(files))
	}
	return files[fromIdx-1 : toIdx], nil
}

func readConversationTurns(csvPath string) ([]salesTurn, error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1

	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read csv header: %w", err)
	}
	index := indexColumns(headers)
	required := []string{"Conversation", "Chunk_id", "Speaker", "Text"}
	for _, col := range required {
		if _, ok := index[col]; !ok {
			return nil, fmt.Errorf("missing required column %q", col)
		}
	}

	fallbackConversationID := strings.TrimSuffix(filepath.Base(csvPath), filepath.Ext(csvPath))
	turns := make([]salesTurn, 0, 64)
	for {
		rec, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read csv row: %w", err)
		}
		if len(rec) == 0 {
			continue
		}

		turnID, err := strconv.Atoi(strings.TrimSpace(getField(rec, index["Chunk_id"])))
		if err != nil {
			continue
		}
		conversationID := strings.TrimSpace(getField(rec, index["Conversation"]))
		if conversationID == "" {
			conversationID = fallbackConversationID
		}
		turns = append(turns, salesTurn{
			ConversationID: conversationID,
			TurnID:         turnID,
			Speaker:        canonicalSpeakerLabel(getField(rec, index["Speaker"])),
			Text:           strings.TrimSpace(getField(rec, index["Text"])),
		})
	}

	sort.SliceStable(turns, func(i, j int) bool {
		return turns[i].TurnID < turns[j].TurnID
	})
	return turns, nil
}

func buildUtteranceBlocks(turns []salesTurn) []utteranceBlock {
	if len(turns) == 0 {
		return nil
	}
	blocks := make([]utteranceBlock, 0, len(turns))
	for _, turn := range turns {
		if len(blocks) == 0 || blocks[len(blocks)-1].GroundTruthSpeaker != turn.Speaker {
			blocks = append(blocks, utteranceBlock{
				ConversationID:     turn.ConversationID,
				UtteranceIndex:     len(blocks) + 1,
				GroundTruthSpeaker: turn.Speaker,
				UtteranceText:      turn.Text,
			})
			continue
		}
		if strings.TrimSpace(turn.Text) == "" {
			continue
		}
		last := &blocks[len(blocks)-1]
		if strings.TrimSpace(last.UtteranceText) == "" {
			last.UtteranceText = turn.Text
		} else {
			last.UtteranceText += "\n" + turn.Text
		}
	}
	return blocks
}

func indexColumns(headers []string) map[string]int {
	out := make(map[string]int, len(headers))
	for i, header := range headers {
		name := strings.TrimSpace(header)
		if i == 0 {
			name = strings.TrimPrefix(name, "\uFEFF")
		}
		out[name] = i
	}
	return out
}

func getField(rec []string, idx int) string {
	if idx < 0 || idx >= len(rec) {
		return ""
	}
	return rec[idx]
}
