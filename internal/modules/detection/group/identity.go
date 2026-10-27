package group

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/butorovv/cluster-anomaly-detection-v2/internal/modules/detection/analysis"
)

// RunID preserves the offline v1 JSON identity. inputSHA must use the original
// logical input keys, not physical mount/worktree paths. It performs no file I/O.
// All groups in a selected universe must receive the same ID.
func RunID(profileSHA string, inputSHA map[string]string, groups int, window analysis.Window) (string, error) {
	if !validSHA(profileSHA) || len(inputSHA) == 0 || (groups != 4 && groups != 8 && groups != 16) || !window.Valid() {
		return "", fmt.Errorf("invalid run identity")
	}
	for key, hash := range inputSHA {
		if key == "" || !validSHA(hash) {
			return "", fmt.Errorf("invalid logical input hash")
		}
	}
	identity, err := json.Marshal([]any{Version, profileSHA, inputSHA, groups, window})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(identity)
	return hex.EncodeToString(digest[:]), nil
}

func validSHA(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(s) == 64 && len(b) == sha256.Size
}
