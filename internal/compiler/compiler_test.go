package compiler_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cuprite-io/flux/internal/compiler"
	"github.com/cuprite-io/flux/internal/vm"
)

func TestCompiler_VoltExpressions(t *testing.T) {
	comp, err := compiler.NewCompiler(nil)
	if err != nil {
		t.Fatalf("failed to create compiler: %v", err)
	}

	tests := []struct {
		name     string
		expr     string
		input    map[string]any
		expected any
	}{
		{
			name:     "Arithmetic & Ternary",
			expr:     `payload.amount >= 1000.0 ? payload.amount * 0.20 : 0.0`,
			input:    map[string]any{"payload": map[string]any{"amount": 4500.0}},
			expected: 900.0,
		},
		{
			name:     "PII Detection",
			expr:     `is_pii("user@example.com")`,
			input:    map[string]any{},
			expected: true,
		},
		{
			name:     "Email Masking",
			expr:     `mask.email("alice@domain.com")`,
			input:    map[string]any{},
			expected: "a****@domain.com",
		},
		{
			name:     "Card Masking",
			expr:     `mask.card("1234-5678-9012-3456")`,
			input:    map[string]any{},
			expected: "************3456",
		},
		{
			name:     "Crypto Hashing (SHA256)",
			expr:     `crypto.sha256("flux")`,
			input:    map[string]any{},
			expected: compiler.OpSha256("flux"),
		},
		{
			name:     "Crypto Hashing (SHA512)",
			expr:     `crypto.sha512("flux")`,
			input:    map[string]any{},
			expected: compiler.OpSha512("flux"),
		},
		{
			name:     "Crypto Hashing (MD5)",
			expr:     `crypto.md5("flux")`,
			input:    map[string]any{},
			expected: compiler.OpMd5("flux"),
		},
		{
			name:     "Crypto HMAC",
			expr:     `crypto.hmac("data", "secret")`,
			input:    map[string]any{},
			expected: compiler.OpHmac("data", "secret"),
		},
		{
			name:     "Crypto Encrypt / Decrypt",
			expr:     `crypto.decrypt(crypto.encrypt("secret_payload", "my_passphrase"), "my_passphrase")`,
			input:    map[string]any{},
			expected: "secret_payload",
		},
		{
			name:     "Geo Distance KM",
			expr:     `geo.dist_km(37.7749, -122.4194, 34.0522, -118.2437) > 500.0`,
			input:    map[string]any{},
			expected: true,
		},
		{
			name:     "Geo Distance Meters",
			expr:     `geo.dist_m(37.7749, -122.4194, 34.0522, -118.2437) > 500000.0`,
			input:    map[string]any{},
			expected: true,
		},
		{
			name:     "Math Clamp",
			expr:     `math.clamp(150.0, 0.0, 100.0)`,
			input:    map[string]any{},
			expected: 100.0,
		},
		{
			name:     "List Unique",
			expr:     `size(list.unique([1, 2, 2, 3, 1]))`,
			input:    map[string]any{},
			expected: int64(3),
		},
		{
			name:     "Base64 Encode/Decode",
			expr:     `base64.decode(base64.encode("hello flux"))`,
			input:    map[string]any{},
			expected: "hello flux",
		},
		{
			name:     "Hex Encode/Decode",
			expr:     `hex.decode(hex.encode("flux_hex_test"))`,
			input:    map[string]any{},
			expected: "flux_hex_test",
		},
		{
			name:     "UUID v4 Generation",
			expr:     `size(uuid.v4()) == 36`,
			input:    map[string]any{},
			expected: true,
		},
		{
			name:     "ML Scoring",
			expr:     `ml.score("model_1", [100.0, 2.5]) > 0.0`,
			input:    map[string]any{},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prog, err := comp.Compile(tt.expr)
			if err != nil {
				t.Fatalf("failed to compile %q: %v", tt.expr, err)
			}

			out, _, err := prog.Eval(tt.input)
			if err != nil {
				t.Fatalf("failed to eval %q: %v", tt.expr, err)
			}

			val := out.Value()
			if val != tt.expected {
				t.Errorf("expr %q: expected %v, got %v", tt.expr, tt.expected, val)
			}
		})
	}
}

func TestVerifier_ValidAndInvalidPrograms(t *testing.T) {
	v := compiler.NewVerifier()

	// Valid Program
	validSteps := []vm.Step{
		{Op: vm.OpAddInt, InRegs: [3]uint8{0, 1, 0}, OutReg: 2},
		{Op: vm.OpReturn, InRegs: [3]uint8{2, 0, 0}},
	}
	validProg := vm.NewProgram("valid", validSteps, nil, nil, nil)
	if err := v.Verify(validProg); err != nil {
		t.Errorf("expected valid program to pass, got: %v", err)
	}

	// Invalid Register Bounds (> 64)
	invalidSteps := []vm.Step{
		{Op: vm.OpAddInt, InRegs: [3]uint8{0, 65, 0}, OutReg: 2},
	}
	invalidProg := vm.NewProgram("invalid_reg", invalidSteps, nil, nil, nil)
	if err := v.Verify(invalidProg); err == nil {
		t.Errorf("expected register out-of-bounds error, got nil")
	}

	// Invalid Jump Offset (points out of bounds)
	invalidJump := []vm.Step{
		{Op: vm.OpJump, BranchOffset: 100},
	}
	invalidJumpProg := vm.NewProgram("invalid_jump", invalidJump, nil, nil, nil)
	if err := v.Verify(invalidJumpProg); err == nil {
		t.Errorf("expected jump offset out-of-bounds error, got nil")
	}
}

type mockCacheAccessor struct {
	store map[string]string
}

func (m *mockCacheAccessor) Get(ctx context.Context, key string) (string, error) {
	if v, ok := m.store[key]; ok {
		return v, nil
	}
	return "", nil
}

func (m *mockCacheAccessor) Set(ctx context.Context, key string, val any, ttl time.Duration) error {
	m.store[key] = fmt.Sprintf("%v", val)
	return nil
}

func (m *mockCacheAccessor) IncrementSlidingWindow(ctx context.Context, key string, window time.Duration) (int64, error) {
	return 42, nil
}

func TestCompiler_CacheAndWindowOperators(t *testing.T) {
	mockCache := &mockCacheAccessor{store: map[string]string{"user:101:status": "ACTIVE"}}
	comp, err := compiler.NewCompiler(mockCache)
	if err != nil {
		t.Fatalf("failed to create compiler: %v", err)
	}

	// 1. Test cache.get
	prog1, err := comp.Compile(`cache.get("user:101:status") == "ACTIVE"`)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}
	out1, _, err := prog1.Eval(map[string]any{})
	if err != nil || out1.Value() != true {
		t.Fatalf("expected true from cache.get, got: %v (err: %v)", out1.Value(), err)
	}

	// 2. Test window.count
	prog2, err := comp.Compile(`window.count("ip:127.0.0.1", "60s") == 42`)
	if err != nil {
		t.Fatalf("failed to compile: %v", err)
	}
	out2, _, err := prog2.Eval(map[string]any{})
	if err != nil || out2.Value() != true {
		t.Fatalf("expected true from window.count, got: %v (err: %v)", out2.Value(), err)
	}
}

func BenchmarkCompiler_Eval(b *testing.B) {
	comp, err := compiler.NewCompiler(nil)
	if err != nil {
		b.Fatal(err)
	}

	expr := `payload.amount >= 1000.0 && is_pii(payload.card_id)`
	prog, err := comp.Compile(expr)
	if err != nil {
		b.Fatal(err)
	}

	input := map[string]any{
		"payload": map[string]any{
			"amount":  5000.0,
			"card_id": "user@example.com",
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		out, _, err := prog.Eval(input)
		if err != nil || !out.Value().(bool) {
			b.Fatal("eval failed")
		}
	}
}
