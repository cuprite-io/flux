package compiler

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	emailRegex = regexp.MustCompile(`(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}`)
	phoneRegex = regexp.MustCompile(`\d{3}-\d{3}-\d{4}`)
	ssnRegex   = regexp.MustCompile(`\d{3}-\d{2}-\d{4}`)
	ipRegex    = regexp.MustCompile(`\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}`)
	uuidRegex  = regexp.MustCompile(`(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
)

// CacheAccessor defines the minimal cache contract needed by operators at runtime.
type CacheAccessor interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, val any, ttl time.Duration) error
	IncrementSlidingWindow(ctx context.Context, key string, window time.Duration) (int64, error)
}

// --- 1. DAG State & Maps ---

func OpSet(state map[string]any, key string, val any) bool {
	if state != nil && key != "" {
		state[key] = val
		return true
	}
	return false
}

func OpGet(state map[string]any, key string, fallback any) any {
	if state != nil {
		if v, ok := state[key]; ok {
			return v
		}
	}
	return fallback
}

func OpMapMerge(m1Raw, m2Raw any) map[string]any {
	res := make(map[string]any)
	if m1, ok := m1Raw.(map[string]any); ok {
		for k, v := range m1 {
			res[k] = v
		}
	}
	if m2, ok := m2Raw.(map[string]any); ok {
		for k, v := range m2 {
			res[k] = v
		}
	}
	return res
}

func OpMapDelete(mRaw any, key string) map[string]any {
	res := make(map[string]any)
	if m, ok := mRaw.(map[string]any); ok {
		for k, v := range m {
			if k != key {
				res[k] = v
			}
		}
	}
	return res
}

// --- 2. Security, PII & Masking ---

func OpIsPII(val string) bool {
	return emailRegex.MatchString(val) ||
		phoneRegex.MatchString(val) ||
		ssnRegex.MatchString(val) ||
		ipRegex.MatchString(val) ||
		uuidRegex.MatchString(val)
}

func OpMaskEmail(val string) string {
	parts := strings.Split(val, "@")
	if len(parts) == 2 && len(parts[0]) > 0 {
		return parts[0][0:1] + "****@" + parts[1]
	}
	return "****"
}

func OpMaskCard(val string) string {
	clean := strings.ReplaceAll(val, "-", "")
	clean = strings.ReplaceAll(clean, " ", "")
	if len(clean) >= 4 {
		return strings.Repeat("*", len(clean)-4) + clean[len(clean)-4:]
	}
	return "****"
}

func OpMask(val, maskType string) string {
	switch strings.ToLower(maskType) {
	case "email":
		return OpMaskEmail(val)
	case "card", "credit_card":
		return OpMaskCard(val)
	default:
		return "****"
	}
}

// --- 3. Cryptography & Hashing ---

func OpEncrypt(plaintext, key string) (string, error) {
	p := []byte(plaintext)
	k := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(k[:])
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	return hex.EncodeToString(gcm.Seal(nonce, nonce, p, nil)), nil
}

func OpDecrypt(ciphertext, key string) (string, error) {
	data, err := hex.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}
	k := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(k[:])
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return "", errors.New("ciphertext too short")
	}
	nonce, cipherBytes := data[:nonceSize], data[nonceSize:]
	plain, err := gcm.Open(nil, nonce, cipherBytes, nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

func OpSha256(val string) string {
	h := sha256.Sum256([]byte(val))
	return hex.EncodeToString(h[:])
}

func OpSha512(val string) string {
	h := sha512.Sum512([]byte(val))
	return hex.EncodeToString(h[:])
}

func OpMd5(val string) string {
	h := md5.Sum([]byte(val))
	return hex.EncodeToString(h[:])
}

func OpCrc32(val string) uint32 {
	return crc32.ChecksumIEEE([]byte(val))
}

func OpHmac(val, key string) string {
	h := hmac.New(sha256.New, []byte(key))
	h.Write([]byte(val))
	return hex.EncodeToString(h.Sum(nil))
}

// --- 4. Geo-Spatial ---

func OpGeoDistanceKM(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371.0 // Earth radius in km
	dLat := (lat2 - lat1) * math.Pi / 180.0
	dLon := (lon2 - lon1) * math.Pi / 180.0
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*math.Pi/180.0)*math.Cos(lat2*math.Pi/180.0)*
			math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return R * c
}

func OpGeoDistanceM(lat1, lon1, lat2, lon2 float64) float64 {
	return OpGeoDistanceKM(lat1, lon1, lat2, lon2) * 1000.0
}

// --- 5. Math & Lists ---

func OpClamp(v, min, max float64) float64 {
	return math.Max(min, math.Min(max, v))
}

func OpStats(list []float64) map[string]float64 {
	size := len(list)
	if size == 0 {
		return map[string]float64{"mean": 0, "stddev": 0, "count": 0}
	}
	var sum, sumSq float64
	for _, val := range list {
		sum += val
		sumSq += val * val
	}
	mean := sum / float64(size)
	variance := (sumSq / float64(size)) - (mean * mean)
	if variance < 0 {
		variance = 0
	}
	stddev := math.Sqrt(variance)
	return map[string]float64{"mean": mean, "stddev": stddev, "count": float64(size)}
}

func OpUnique(list []any) []any {
	seen := make(map[any]bool, len(list))
	res := make([]any, 0, len(list))
	for _, val := range list {
		if !seen[val] {
			seen[val] = true
			res = append(res, val)
		}
	}
	return res
}

// --- 6. Encoding & UUID ---

func OpUUID() string {
	return uuid.New().String()
}

func OpBase64Encode(val string) string {
	return base64.StdEncoding.EncodeToString([]byte(val))
}

func OpBase64Decode(val string) (string, error) {
	b, err := base64.StdEncoding.DecodeString(val)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func OpHexEncode(val string) string {
	return hex.EncodeToString([]byte(val))
}

func OpHexDecode(val string) (string, error) {
	b, err := hex.DecodeString(val)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// --- 7. Machine Learning / Heuristic Models ---

func OpMLScore(modelID string, vector []float64) float64 {
	// Fast heuristic default dot-product anomaly scorer
	if len(vector) == 0 {
		return 0.0
	}
	var sum float64
	for i, v := range vector {
		weight := 1.0 / float64(i+1)
		sum += v * weight
	}
	// Sigmoid normalization to 0.0..1.0
	return 1.0 / (1.0 + math.Exp(-sum/1000.0))
}
