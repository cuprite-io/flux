package compiler_test

import (
	"context"
	"os"
	"testing"

	"github.com/cuprite-io/capacitor"
	"github.com/cuprite-io/flux/internal/compiler"
)

// --- 1. STATE & MAP OPERATORS BENCHMARKS ---

func BenchmarkOperator_State_Set(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`set("session_id", "sess_994821")`)
	input := map[string]any{"state": map[string]any{}}

	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

func BenchmarkOperator_State_Get(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`get(state, "tier", "STANDARD")`)
	input := map[string]any{"state": map[string]any{"tier": "VIP_PLATINUM"}}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

func BenchmarkOperator_State_MapMerge(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`map.merge(payload.base, payload.delta)`)
	input := map[string]any{
		"payload": map[string]any{
			"base":  map[string]any{"a": 1, "b": 2},
			"delta": map[string]any{"b": 20, "c": 30},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

func BenchmarkOperator_State_MapDelete(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`map.delete(payload.data, "secret_token")`)
	input := map[string]any{
		"payload": map[string]any{
			"data": map[string]any{"id": "u1", "secret_token": "xyz999"},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

// --- 2. SECURITY & MASKING OPERATORS BENCHMARKS ---

func BenchmarkOperator_Security_IsPII(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`is_pii("john.doe@enterprise-domain.com")`)
	input := map[string]any{}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

func BenchmarkOperator_Security_MaskEmail(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`mask.email("john.doe@enterprise-domain.com")`)
	input := map[string]any{}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

func BenchmarkOperator_Security_MaskCard(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`mask.card("4111-2222-3333-4444")`)
	input := map[string]any{}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

// --- 3. CRYPTOGRAPHY OPERATORS BENCHMARKS ---

func BenchmarkOperator_Crypto_Sha256(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`crypto.sha256("transaction_payload_data_string_482910")`)
	input := map[string]any{}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

func BenchmarkOperator_Crypto_Sha512(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`crypto.sha512("transaction_payload_data_string_482910")`)
	input := map[string]any{}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

func BenchmarkOperator_Crypto_Md5(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`crypto.md5("transaction_payload_data_string_482910")`)
	input := map[string]any{}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

func BenchmarkOperator_Crypto_HMAC(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`crypto.hmac("message_to_authenticate", "super_secret_hmac_key_123")`)
	input := map[string]any{}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

func BenchmarkOperator_Crypto_Encrypt(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`crypto.encrypt("confidential_user_record_data", "aes_gcm_passphrase_999")`)
	input := map[string]any{}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

func BenchmarkOperator_Crypto_Decrypt(b *testing.B) {
	encrypted, _ := compiler.OpEncrypt("confidential_user_record_data", "aes_gcm_passphrase_999")
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`crypto.decrypt(payload.cipher, "aes_gcm_passphrase_999")`)
	input := map[string]any{"payload": map[string]any{"cipher": encrypted}}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

// --- 4. GEO-SPATIAL OPERATORS BENCHMARKS ---

func BenchmarkOperator_Geo_DistanceKM(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`geo.distance_km(37.7749, -122.4194, 40.7128, -74.0060)`)
	input := map[string]any{}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

// --- 5. MATH & LIST OPERATORS BENCHMARKS ---

func BenchmarkOperator_Math_Clamp(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`math.clamp(175.5, 0.0, 100.0)`)
	input := map[string]any{}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

func BenchmarkOperator_List_Unique(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`list.unique([10, 20, 20, 30, 40, 10, 50, 60, 30])`)
	input := map[string]any{}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

// --- 6. ENCODINGS & IDENTIFIERS BENCHMARKS ---

func BenchmarkOperator_Encoding_UUIDv4(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`uuid.v4()`)
	input := map[string]any{}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

func BenchmarkOperator_Encoding_Base64(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`base64.encode("payload_data_string_for_base64_encoding_test")`)
	input := map[string]any{}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

func BenchmarkOperator_Encoding_Hex(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`hex.encode("payload_data_string_for_hex_encoding_test")`)
	input := map[string]any{}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

// --- 7. ML INFERENCE OPERATORS BENCHMARKS ---

func BenchmarkOperator_ML_Score(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	prog, _ := comp.Compile(`ml.score("fraud_model_v1", [1500.0, 3.2, 0.95, 45.0])`)
	input := map[string]any{}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

// --- 8. STATEFUL CACHE & SLIDING WINDOWS WITH CAPACITOR BENCHMARKS ---

func BenchmarkOperator_Cache_Capacitor_Get(b *testing.B) {
	dir, err := os.MkdirTemp("", "capacitor-bench-get-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cp, err := capacitor.New(capacitor.Config{
		NodeID:   "bench-get-01",
		DataPath: dir,
		BindPort: 19501,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer cp.Close()

	ctx := context.Background()
	_ = cp.Set(ctx, "entity:player_42", `{"status":"ACTIVE","vip":true}`, 0)

	comp, err := compiler.NewCompiler(cp)
	if err != nil {
		b.Fatal(err)
	}

	prog, err := comp.Compile(`cache.get("entity:player_42")`)
	if err != nil {
		b.Fatal(err)
	}

	input := map[string]any{}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}

func BenchmarkOperator_Cache_Capacitor_SlidingWindow(b *testing.B) {
	dir, err := os.MkdirTemp("", "capacitor-bench-window-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cp, err := capacitor.New(capacitor.Config{
		NodeID:   "bench-window-01",
		DataPath: dir,
		BindPort: 19502,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer cp.Close()

	comp, err := compiler.NewCompiler(cp)
	if err != nil {
		b.Fatal(err)
	}

	prog, err := comp.Compile(`window.count("rate_limit:user_42", "60s")`)
	if err != nil {
		b.Fatal(err)
	}

	input := map[string]any{}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _, _ = prog.Eval(input)
	}
}
