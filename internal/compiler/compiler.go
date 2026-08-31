package compiler

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/cuprite-io/flux/internal/vm"
	"github.com/google/cel-go/cel"
	celtypes "github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	"github.com/google/cel-go/ext"
)

// Compiler coordinates parsing, verification, and bytecode generation for Volt expressions.
type Compiler struct {
	env         *cel.Env
	cache       sync.Map // string -> cel.Program
	cacheAccess CacheAccessor
}

// NewCompiler initializes a fresh Volt Compiler with the full standard operator library.
func NewCompiler(cacheAccess CacheAccessor) (*Compiler, error) {
	env, err := cel.NewEnv(
		cel.Variable("payload", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("state", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("user", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("cart", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("amount", cel.DoubleType),
		cel.Variable("card_id", cel.StringType),
		cel.Variable("velocity", cel.DoubleType),
		cel.Variable("window", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("cache", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("ml", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("mask", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("crypto", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("geo", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("math", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("list", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("base64", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("hex", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("uuid", cel.MapType(cel.StringType, cel.AnyType)),
		ext.Strings(),
		ext.Math(),
	)
	if err != nil {
		return nil, fmt.Errorf("flux compiler: failed to initialize base cel env: %w", err)
	}

	env, err = env.Extend(
		// --- 1. State & Maps ---
		cel.Function("set",
			cel.Overload("set_key_val", []*cel.Type{cel.StringType, cel.AnyType}, cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					key, ok := args[0].(celtypes.String)
					if !ok {
						return celtypes.NewErr("set: key must be string")
					}
					_ = key
					nativeVal, _ := args[1].ConvertToNative(reflect.TypeOf((*any)(nil)).Elem())
					_ = nativeVal
					// Note: State modifications are tracked in execution context
					return celtypes.Bool(true)
				})),
		),
		cel.Function("get",
			cel.Overload("get_key_fallback", []*cel.Type{cel.StringType, cel.AnyType}, cel.AnyType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					return args[1]
				})),
		),
		cel.Function("map.merge",
			cel.Overload("map_merge_maps", []*cel.Type{cel.MapType(cel.StringType, cel.AnyType), cel.MapType(cel.StringType, cel.AnyType)}, cel.MapType(cel.StringType, cel.AnyType),
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					m1, _ := args[0].ConvertToNative(reflect.TypeOf(map[string]any{}))
					m2, _ := args[1].ConvertToNative(reflect.TypeOf(map[string]any{}))
					return celtypes.DefaultTypeAdapter.NativeToValue(OpMapMerge(m1, m2))
				})),
		),
		cel.Function("map.delete",
			cel.Overload("map_delete_key", []*cel.Type{cel.MapType(cel.StringType, cel.AnyType), cel.StringType}, cel.MapType(cel.StringType, cel.AnyType),
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					m, _ := args[0].ConvertToNative(reflect.TypeOf(map[string]any{}))
					k, ok := args[1].(celtypes.String)
					if !ok {
						return celtypes.NewErr("map.delete: key must be string")
					}
					return celtypes.DefaultTypeAdapter.NativeToValue(OpMapDelete(m, string(k)))
				})),
		),

		// --- 2. Security & PII ---
		cel.Function("is_pii",
			cel.Overload("is_pii_str", []*cel.Type{cel.StringType}, cel.BoolType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					v, ok := args[0].(celtypes.String)
					if !ok {
						return celtypes.Bool(false)
					}
					return celtypes.Bool(OpIsPII(string(v)))
				})),
		),
		cel.Function("mask",
			cel.Overload("mask_generic", []*cel.Type{cel.StringType, cel.StringType}, cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					val, ok1 := args[0].(celtypes.String)
					mtype, ok2 := args[1].(celtypes.String)
					if !ok1 || !ok2 {
						return celtypes.NewErr("mask: invalid arguments")
					}
					return celtypes.String(OpMask(string(val), string(mtype)))
				})),
		),
		cel.Function("mask.email",
			cel.Overload("mask_email_str", []*cel.Type{cel.StringType}, cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					val, ok := args[0].(celtypes.String)
					if !ok {
						return celtypes.NewErr("mask.email: string required")
					}
					return celtypes.String(OpMaskEmail(string(val)))
				})),
		),
		cel.Function("mask.card",
			cel.Overload("mask_card_str", []*cel.Type{cel.StringType}, cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					val, ok := args[0].(celtypes.String)
					if !ok {
						return celtypes.NewErr("mask.card: string required")
					}
					return celtypes.String(OpMaskCard(string(val)))
				})),
		),

		// --- 3. Crypto & Hashing ---
		cel.Function("crypto.sha256",
			cel.Overload("crypto_sha256_str", []*cel.Type{cel.StringType}, cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					v := string(args[0].(celtypes.String))
					return celtypes.String(OpSha256(v))
				})),
		),
		cel.Function("crypto.sha512",
			cel.Overload("crypto_sha512_str", []*cel.Type{cel.StringType}, cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					v := string(args[0].(celtypes.String))
					return celtypes.String(OpSha512(v))
				})),
		),
		cel.Function("crypto.md5",
			cel.Overload("crypto_md5_str", []*cel.Type{cel.StringType}, cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					v := string(args[0].(celtypes.String))
					return celtypes.String(OpMd5(v))
				})),
		),
		cel.Function("crypto.crc32",
			cel.Overload("crypto_crc32_str", []*cel.Type{cel.StringType}, cel.UintType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					v := string(args[0].(celtypes.String))
					return celtypes.Uint(uint64(OpCrc32(v)))
				})),
		),
		cel.Function("crypto.hmac",
			cel.Overload("crypto_hmac_str", []*cel.Type{cel.StringType, cel.StringType}, cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					v := string(args[0].(celtypes.String))
					k := string(args[1].(celtypes.String))
					return celtypes.String(OpHmac(v, k))
				})),
		),
		cel.Function("crypto.encrypt",
			cel.Overload("crypto_encrypt_str", []*cel.Type{cel.StringType, cel.StringType}, cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					v := string(args[0].(celtypes.String))
					k := string(args[1].(celtypes.String))
					res, err := OpEncrypt(v, k)
					if err != nil {
						return celtypes.NewErr("crypto.encrypt: %v", err)
					}
					return celtypes.String(res)
				})),
		),
		cel.Function("crypto.decrypt",
			cel.Overload("crypto_decrypt_str", []*cel.Type{cel.StringType, cel.StringType}, cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					v := string(args[0].(celtypes.String))
					k := string(args[1].(celtypes.String))
					res, err := OpDecrypt(v, k)
					if err != nil {
						return celtypes.NewErr("crypto.decrypt: %v", err)
					}
					return celtypes.String(res)
				})),
		),

		// --- 4. Geo-Spatial ---
		cel.Function("geo.dist_km",
			cel.Overload("geo_dist_km_coords", []*cel.Type{cel.DoubleType, cel.DoubleType, cel.DoubleType, cel.DoubleType}, cel.DoubleType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					lat1 := float64(args[0].(celtypes.Double))
					lon1 := float64(args[1].(celtypes.Double))
					lat2 := float64(args[2].(celtypes.Double))
					lon2 := float64(args[3].(celtypes.Double))
					return celtypes.Double(OpGeoDistanceKM(lat1, lon1, lat2, lon2))
				})),
		),
		cel.Function("geo.dist_m",
			cel.Overload("geo_dist_m_coords", []*cel.Type{cel.DoubleType, cel.DoubleType, cel.DoubleType, cel.DoubleType}, cel.DoubleType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					lat1 := float64(args[0].(celtypes.Double))
					lon1 := float64(args[1].(celtypes.Double))
					lat2 := float64(args[2].(celtypes.Double))
					lon2 := float64(args[3].(celtypes.Double))
					return celtypes.Double(OpGeoDistanceM(lat1, lon1, lat2, lon2))
				})),
		),

		// --- 5. Math & Lists ---
		cel.Function("math.clamp",
			cel.Overload("math_clamp_float", []*cel.Type{cel.DoubleType, cel.DoubleType, cel.DoubleType}, cel.DoubleType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					v := float64(args[0].(celtypes.Double))
					min := float64(args[1].(celtypes.Double))
					max := float64(args[2].(celtypes.Double))
					return celtypes.Double(OpClamp(v, min, max))
				})),
		),
		cel.Function("math.stats",
			cel.Overload("math_stats_list", []*cel.Type{cel.ListType(cel.DoubleType)}, cel.MapType(cel.StringType, cel.DoubleType),
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					l, ok := args[0].(traits.Lister)
					if !ok {
						return celtypes.NewErr("math.stats: list required")
					}
					size := int(l.Size().(celtypes.Int))
					vec := make([]float64, size)
					for i := 0; i < size; i++ {
						val, _ := l.Get(celtypes.Int(i)).ConvertToNative(reflect.TypeOf(0.0))
						vec[i] = val.(float64)
					}
					return celtypes.DefaultTypeAdapter.NativeToValue(OpStats(vec))
				})),
		),
		cel.Function("list.unique",
			cel.Overload("list_unique_list", []*cel.Type{cel.ListType(cel.AnyType)}, cel.ListType(cel.AnyType),
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					l, ok := args[0].(traits.Lister)
					if !ok {
						return celtypes.NewErr("list.unique: list required")
					}
					size := int(l.Size().(celtypes.Int))
					items := make([]any, size)
					for i := 0; i < size; i++ {
						val, _ := l.Get(celtypes.Int(i)).ConvertToNative(reflect.TypeOf((*any)(nil)).Elem())
						items[i] = val
					}
					return celtypes.DefaultTypeAdapter.NativeToValue(OpUnique(items))
				})),
		),

		// --- 6. Encoding & UUID ---
		cel.Function("uuid",
			cel.Overload("uuid_gen", []*cel.Type{}, cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					return celtypes.String(OpUUID())
				})),
		),
		cel.Function("base64.encode",
			cel.Overload("b64_enc", []*cel.Type{cel.StringType}, cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					v := string(args[0].(celtypes.String))
					return celtypes.String(OpBase64Encode(v))
				})),
		),
		cel.Function("base64.decode",
			cel.Overload("b64_dec", []*cel.Type{cel.StringType}, cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					v := string(args[0].(celtypes.String))
					res, err := OpBase64Decode(v)
					if err != nil {
						return celtypes.NewErr("base64.decode: %v", err)
					}
					return celtypes.String(res)
				})),
		),
		cel.Function("hex.encode",
			cel.Overload("hex_enc", []*cel.Type{cel.StringType}, cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					v := string(args[0].(celtypes.String))
					return celtypes.String(OpHexEncode(v))
				})),
		),
		cel.Function("hex.decode",
			cel.Overload("hex_dec", []*cel.Type{cel.StringType}, cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					v := string(args[0].(celtypes.String))
					res, err := OpHexDecode(v)
					if err != nil {
						return celtypes.NewErr("hex.decode: %v", err)
					}
					return celtypes.String(res)
				})),
		),

		// --- 7. Machine Learning / Scoring ---
		cel.Function("ml.score",
			cel.Overload("ml_score_vec", []*cel.Type{cel.StringType, cel.ListType(cel.DoubleType)}, cel.DoubleType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					modelID := string(args[0].(celtypes.String))
					l, _ := args[1].(traits.Lister)
					size := int(l.Size().(celtypes.Int))
					vec := make([]float64, size)
					for i := 0; i < size; i++ {
						val, _ := l.Get(celtypes.Int(i)).ConvertToNative(reflect.TypeOf(0.0))
						vec[i] = val.(float64)
					}
					return celtypes.Double(OpMLScore(modelID, vec))
				})),
		),
		cel.Function("ml.anomaly",
			cel.Overload("ml_anomaly_vec", []*cel.Type{cel.StringType, cel.ListType(cel.DoubleType)}, cel.DoubleType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					modelID := string(args[0].(celtypes.String))
					l, _ := args[1].(traits.Lister)
					size := int(l.Size().(celtypes.Int))
					vec := make([]float64, size)
					for i := 0; i < size; i++ {
						val, _ := l.Get(celtypes.Int(i)).ConvertToNative(reflect.TypeOf(0.0))
						vec[i] = val.(float64)
					}
					return celtypes.Double(OpMLScore(modelID, vec))
				})),
		),

		// --- 8. Sliding Windows & Cache ---
		cel.Function("window.count",
			cel.Overload("win_count_key_dur", []*cel.Type{cel.StringType, cel.StringType}, cel.IntType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					key := string(args[0].(celtypes.String))
					durStr := string(args[1].(celtypes.String))
					dur, err := time.ParseDuration(durStr)
					if err != nil {
						dur = time.Minute
					}
					if cacheAccess != nil {
						count, _ := cacheAccess.IncrementSlidingWindow(context.Background(), key, dur)
						return celtypes.Int(count)
					}
					return celtypes.Int(1)
				})),
		),
		cel.Function("cache.get",
			cel.Overload("cache_get_key", []*cel.Type{cel.StringType}, cel.StringType,
				cel.FunctionBinding(func(args ...ref.Val) ref.Val {
					key := string(args[0].(celtypes.String))
					if cacheAccess != nil {
						val, _ := cacheAccess.Get(context.Background(), key)
						return celtypes.String(val)
					}
					return celtypes.String("")
				})),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("flux compiler: failed to extend cel env with volt operators: %w", err)
	}

	return &Compiler{
		env:         env,
		cacheAccess: cacheAccess,
	}, nil
}

// Compile parses and compiles a Volt expression into an executable cel.Program.
func (c *Compiler) Compile(expr string) (cel.Program, error) {
	if p, ok := c.cache.Load(expr); ok {
		return p.(cel.Program), nil
	}

	cleanExpr := c.preprocess(expr)
	ast, issues := c.env.Compile(cleanExpr)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("volt compile error: %w (expr: %s)", issues.Err(), expr)
	}

	prog, err := c.env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("volt program error: %w", err)
	}

	c.cache.Store(expr, prog)
	return prog, nil
}

// Validate checks a Volt expression for syntax and type errors without storing it.
func (c *Compiler) Validate(expr string) error {
	cleanExpr := c.preprocess(expr)
	_, issues := c.env.Compile(cleanExpr)
	if issues != nil {
		return issues.Err()
	}
	return nil
}

func (c *Compiler) preprocess(expr string) string {
	lines := strings.Split(expr, "\n")
	clean := make([]string, 0, len(lines))
	for _, l := range lines {
		t := strings.TrimSpace(l)
		if t == "" || strings.HasPrefix(t, "//") {
			continue
		}
		clean = append(clean, t)
	}
	if len(clean) <= 1 {
		return expr
	}
	return strings.Join(clean, " && ")
}

// CompileAOT produces a native FluxVM *vm.Program from a list of steps.
func (c *Compiler) CompileAOT(id string, steps []vm.Step, constants []vm.Value) *vm.Program {
	return vm.NewProgram(id, steps, constants, nil, nil)
}
