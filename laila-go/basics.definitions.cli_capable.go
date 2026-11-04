package laila

import (
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/amirzadehlambda/laila/laila-go/py"
)

// basics/definitions/cli_capable.py -- CLI-capable base class with 4-tier
// parameter resolution.
//
// Every "first-class" laila object opts into LAILA_CLI_CAPABLE_CLASS_ so
// that, during construction, missing constructor arguments are auto-filled
// from laila.Args: explicit kwarg > laila.args lookup > pydantic default >
// required-field check (RuntimeError).
//
// CLIExempt fields: mark the Go field with the ``cli_exempt`` tag option
// (``py:"name,cli_exempt"``) -- the equivalent of ``CLIExempt(...)`` in
// place of ``Field(...)``. Exempt fields are skipped by both the
// laila.args injection and the environment mirror (Build_environment).
//
// Environment mirror: every successful CLI-capable construction marks its
// owning policy dirty (Mark_environment_dirty_, O(1)); the dump is produced
// by Flush_environment_mirror_ the next time ``laila.args.environment`` is
// read. The logger branch stays eager.

// DIRTY_POLICY_GIDS_ holds the owning-policy gids whose mirror entry is stale.
var DIRTY_POLICY_GIDS_ = py.NewSet()
var DIRTY_LOCK_ = py.NewLock()
var FLUSH_STATE_ = py.NewLocal()

// Is_cli_exempt_ returns true if a FieldInfo carries the CLI-exempt marker.
func Is_cli_exempt_(field_info *py.FieldInfo) bool {
	if field_info.Json_schema_extra == nil {
		return false
	}
	return py.Truthy(field_info.Json_schema_extra["cli_exempt"])
}

var SCOPE_TO_ARGS_PATH_ = map[string]string{
	"POLICY":                "policy",
	"CENTRAL_COMMAND":       "policy.central.command",
	"CENTRAL_MEMORY":        "policy.central.memory",
	"CENTRAL_COMMUNICATION": "policy.central.communication",
	"POOL":                  "policy.central.memory.pools.{global_id}",
	"POOL_ROUTER":           "policy.central.memory.pool_router",
	"TASK_FORCE":            "policy.central.command.taskforces.{global_id}",
	"COMM_PROTOCOL":         "policy.central.communication.connections.{global_id}",
	"LOGGER":                "logger",
}

// Get_args_subtree_ traverses laila.Args using ``.get()`` to avoid DotMap
// auto-creation; nil when absent or an empty mapping.
func Get_args_subtree_(path string) any {
	var subtree any = Args
	for _, part := range strings.Split(path, ".") {
		if py.IsNone(subtree) {
			return nil
		}
		subtree = dotmapGet(subtree, part)
	}
	if !py.IsNone(subtree) {
		if n, err := py.Len(subtree); err == nil && n == 0 && hasKeys(subtree) {
			return nil
		}
	}
	return subtree
}

// Resolve_global_id_for_path_ computes a global_id from constructor data
// for ``{global_id}`` path templates. Checks explicit data first, then the
// thread-local INIT_PENDING_ staging area.
func Resolve_global_id_for_path_(cls *py.Class, data py.Kw) (string, bool) {
	if v, ok := data["global_id"]; ok {
		return py.Str(v), true
	}
	uuid_val := data.Get("uuid", nil)
	if py.IsNone(uuid_val) {
		uuid_val, _ = INIT_PENDING_.Get("uuid")
	}
	if py.IsNone(uuid_val) {
		return "", false
	}
	factory, ok := cls.Private_default("_scopes")
	if !ok {
		return "", false
	}
	scopes := toStrings(factory())
	return To_global_id(py.Kw{"uuid": uuid_val, "scopes": scopes}), true
}

// Scope_for_class_ reads the first scope from a class's ``_scopes``
// PrivateAttr default.
func Scope_for_class_(cls *py.Class) (string, bool) {
	if cls == nil {
		return "", false
	}
	factory, ok := cls.Private_default("_scopes")
	if !ok {
		return "", false
	}
	scopes := toStrings(factory())
	if len(scopes) > 0 {
		return scopes[0], true
	}
	return "", false
}

// Resolve_path_for_class_ determines the laila.args path for a CLI-capable
// class instance.
func Resolve_path_for_class_(cls *py.Class, data py.Kw) (string, bool) {
	scope, ok := Scope_for_class_(cls)
	if !ok {
		return "", false
	}
	path_template, ok := SCOPE_TO_ARGS_PATH_[scope]
	if !ok {
		return "", false
	}
	if strings.Contains(path_template, "{global_id}") {
		gid, ok := Resolve_global_id_for_path_(cls, data)
		if !ok {
			return "", false
		}
		path_template = strings.ReplaceAll(path_template, "{global_id}", gid)
	}
	return path_template, true
}

var (
	eligibleFieldsMu    sync.Mutex
	eligibleFieldsCache = map[*py.Class][]string{}
	eligibleSettersMu   sync.Mutex
	eligibleSetterCache = map[*py.Class][]string{}
)

// Eligible_model_fields_ returns public, non-exempt pydantic field names
// for cls (memoised per class).
func Eligible_model_fields_(cls *py.Class) []string {
	eligibleFieldsMu.Lock()
	defer eligibleFieldsMu.Unlock()
	if names, ok := eligibleFieldsCache[cls]; ok {
		return names
	}
	names := []string{}
	for _, field_info := range cls.Model_fields() {
		if strings.HasPrefix(field_info.Name, "_") {
			continue
		}
		if Is_cli_exempt_(field_info) {
			continue
		}
		names = append(names, field_info.Name)
	}
	eligibleFieldsCache[cls] = names
	return names
}

// Eligible_property_setters_ returns names of public property descriptors
// that have a setter (memoised per class): every ``Set_x`` method with a
// matching ``X`` getter whose Python name is not a model field.
func Eligible_property_setters_(cls *py.Class) []string {
	eligibleSettersMu.Lock()
	defer eligibleSettersMu.Unlock()
	if names, ok := eligibleSetterCache[cls]; ok {
		return names
	}
	names := []string{}
	t := cls.Type()
	if t != nil {
		fields := map[string]bool{}
		for _, f := range cls.Model_fields() {
			fields[f.Name] = true
		}
		for i := 0; i < t.NumMethod(); i++ {
			m := t.Method(i)
			if !strings.HasPrefix(m.Name, "Set_") || m.Type.NumIn() != 2 {
				continue
			}
			goName := strings.TrimPrefix(m.Name, "Set_")
			getter, ok := t.MethodByName(goName)
			if !ok || getter.Type.NumIn() != 1 {
				continue
			}
			name := py.Pyname(goName)
			if strings.HasPrefix(name, "_") || fields[name] {
				continue
			}
			names = append(names, name)
		}
		sort.Strings(names)
	}
	eligibleSetterCache[cls] = names
	return names
}

// All_subclasses_ returns a flat ``{class_name: class}`` map of root and
// every subclass (last wins on shared names).
func All_subclasses_(root *py.Class) map[string]*py.Class {
	out := map[string]*py.Class{root.Name: root}
	for _, sub := range root.Subclasses() {
		for k, v := range All_subclasses_(sub) {
			out[k] = v
		}
	}
	return out
}

// Resolve_class_ looks a subclass of root up by ``__name__`` token.
func Resolve_class_(token string, root *py.Class) (*py.Class, error) {
	classes := All_subclasses_(root)
	cls, ok := classes[token]
	if !ok {
		known := make([]string, 0, len(classes))
		for k := range classes {
			known = append(known, k)
		}
		sort.Strings(known)
		return nil, py.ValueError.Newf("unknown %s subclass token: %s (known: %s)", root.Name, py.Repr(token), strings.Join(known, ", "))
	}
	return cls, nil
}

// Build_environment builds a JSON-serializable environment dict from a live
// policy: only CLI-eligible fields (public, non-exempt, non-private).
func Build_environment(policy any) *py.Dict {
	var dump_obj func(obj any) *py.Dict
	dump_obj = func(obj any) *py.Dict {
		result := py.NewDict()
		cls := py.Classof(obj)
		for _, name := range Eligible_model_fields_(cls) {
			val := getattr(obj, name)
			if !py.IsNone(val) && py.Classof(val) != nil {
				result.Set(name, dump_obj(val))
			} else if d, ok := asDict(val); ok {
				nested := py.NewDict()
				for _, it := range d.Items() {
					if py.Classof(it.Value) != nil {
						nested.Set(it.Key, dump_obj(it.Value))
					} else {
						nested.Set(it.Key, it.Value)
					}
				}
				result.Set(name, nested)
			} else {
				result.Set(name, val)
			}
		}
		for _, name := range Eligible_property_setters_(cls) {
			if v, err := py.Getattr(obj, name); err == nil {
				result.Set(name, v)
			}
		}
		return result
	}
	dump_props := func(obj any, into *py.Dict) {
		for _, prop_name := range Eligible_property_setters_(py.Classof(obj)) {
			if v, err := py.Getattr(obj, prop_name); err == nil {
				into.Set(prop_name, v)
			}
		}
	}

	env := py.DictOf("policy", py.NewDict())
	policy_data := dump_obj(policy)
	dump_props(policy, policy_data)
	env.Set("policy", policy_data)

	if py.Hasattr(policy, "central") {
		central := getattr(policy, "central")
		if !py.Truthy(policy_data.GetDefault("central", nil)) {
			policy_data.Set("central", py.NewDict())
		}
		central_env := policy_data.GetDefault("central", nil).(*py.Dict)

		if cmd := getattr(central, "command"); py.Hasattr(central, "command") && !py.IsNone(cmd) {
			cmd_data := dump_obj(cmd)
			dump_props(cmd, cmd_data)
			if py.Hasattr(cmd, "taskforces") {
				tf_data := py.NewDict()
				for _, it := range mappingItems(getattr(cmd, "taskforces")) {
					tf_dump := dump_obj(it.Value)
					dump_props(it.Value, tf_dump)
					tf_dump.Set("class_token", py.Classof(it.Value).Name)
					tf_data.Set(it.Key, tf_dump)
				}
				cmd_data.Set("taskforces", tf_data)
			}
			central_env.Set("command", cmd_data)
		}

		if mem := getattr(central, "memory"); py.Hasattr(central, "memory") && !py.IsNone(mem) {
			mem_data := dump_obj(mem)
			dump_props(mem, mem_data)
			if router := getattr(mem, "pool_router"); py.Hasattr(mem, "pool_router") && !py.IsNone(router) {
				router_data := dump_obj(router)
				dump_props(router, router_data)
				if py.Hasattr(router, "pools") {
					pool_data := py.NewDict()
					for _, it := range mappingItems(getattr(router, "pools")) {
						pool_dump := dump_obj(it.Value)
						dump_props(it.Value, pool_dump)
						pool_dump.Set("class_token", py.Classof(it.Value).Name)
						pool_data.Set(it.Key, pool_dump)
					}
					router_data.Set("pools", pool_data)
				}
				mem_data.Set("pool_router", router_data)
			}
			central_env.Set("memory", mem_data)
		}

		if comm := getattr(central, "communication"); py.Hasattr(central, "communication") && !py.IsNone(comm) {
			comm_data := dump_obj(comm)
			dump_props(comm, comm_data)
			if py.Hasattr(comm, "connections") {
				conn_data := py.NewDict()
				for _, it := range mappingItems(getattr(comm, "connections")) {
					proto_dump := dump_obj(it.Value)
					dump_props(it.Value, proto_dump)
					proto_dump.Set("class_token", py.Classof(it.Value).Name)
					conn_data.Set(it.Key, proto_dump)
				}
				comm_data.Set("connections", conn_data)
			}
			central_env.Set("communication", comm_data)
		}
	}
	return env
}

// LAILA_CLI_CAPABLE_CLASS_ is the base class enabling 4-tier parameter
// resolution from laila.Args. It is a stateless mixin: embed it alongside
// LAILA_IDENTIFIABLE_OBJECT_ (or a subclass), list
// LAILA_CLI_CAPABLE_CLASS_class among the class's bases, and chain
// ``Model_post_init`` to CLI_CAPABLE_model_post_init(self) after the
// identifiable base's hook (the Python MRO order). ``_cli_required_fields``
// is a class variable (``Set_classvar("_cli_required_fields", []string{...})``).
type LAILA_CLI_CAPABLE_CLASS_ struct{}

// LAILA_CLI_CAPABLE_CLASS_class is the class descriptor of
// _LAILA_CLI_CAPABLE_CLASS.
var LAILA_CLI_CAPABLE_CLASS_class = py.NewClass("_LAILA_CLI_CAPABLE_CLASS", nil).
	Set_classvar("_cli_required_fields", []string{}).
	Add_before(Resolve_from_cli_args_).
	Add_after(Refresh_environment_mirror_)

// Resolve_from_cli_args_ is the pydantic before-validator that injects
// values from laila.Args for eligible fields the caller did not pass.
func Resolve_from_cli_args_(cls *py.Class, data py.Kw) (py.Kw, error) {
	if data == nil {
		return data, nil
	}
	path, ok := Resolve_path_for_class_(cls, data)
	if !ok {
		return data, nil
	}
	subtree := Get_args_subtree_(path)
	if py.IsNone(subtree) {
		return data, nil
	}
	for _, field_name := range Eligible_model_fields_(cls) {
		if _, ok := data[field_name]; ok {
			continue
		}
		val := dotmapGet(subtree, field_name)
		if !py.IsNone(val) {
			if dm, ok := val.(*py.DotMap); ok && dm.Len() == 0 {
				continue
			}
			data[field_name] = val
		}
	}
	return data, nil
}

// CLI_CAPABLE_model_post_init is _LAILA_CLI_CAPABLE_CLASS.model_post_init:
// backfills property setters from the args subtree and enforces
// ``_cli_required_fields``.
func CLI_CAPABLE_model_post_init(self any) error {
	cls := py.Classof(self)
	path, ok := Resolve_path_for_class_(cls, py.Kw{})
	var subtree any
	if ok {
		subtree = Get_args_subtree_(path)
	}
	if !py.IsNone(subtree) {
		for _, prop_name := range Eligible_property_setters_(cls) {
			if strings.HasPrefix(prop_name, "_") {
				continue
			}
			val := dotmapGet(subtree, prop_name)
			if !py.IsNone(val) {
				if dm, ok := val.(*py.DotMap); ok && dm.Len() == 0 {
					continue
				}
				_ = py.Setattr(self, prop_name, val)
			}
		}
	}
	if req, ok := cls.Classvar("_cli_required_fields"); ok {
		for _, field_name := range toStrings(req) {
			if py.IsNone(getattr(self, field_name)) {
				return py.RuntimeError.Newf("%s.%s is required but was not provided explicitly, via laila.args, or as a default.", cls.Name, field_name)
			}
		}
	}
	return nil
}

// Refresh_environment_mirror_ (after-validator) marks this instance's
// owning policy dirty in the environment mirror. Best-effort.
func Refresh_environment_mirror_(self any) error {
	func() {
		defer func() { _ = recover() }()
		Mark_environment_dirty_(self)
	}()
	return nil
}

// Find_owning_policy_ returns the policy that instance belongs to, or nil:
// the instance itself when it is a policy, the policy registered under its
// ``policy_id``, else the currently active local policy.
func Find_owning_policy_(instance any) any {
	policyCls, _ := py.Class_by_name("_LAILA_IDENTIFIABLE_POLICY")
	if policyCls != nil && py.Isinstance(instance, policyCls) {
		return instance
	}
	policy_id := getattr(instance, "policy_id")
	if !py.IsNone(policy_id) {
		if py.Isinstance(policy_id, LAILA_IDENTIFIABLE_OBJECT_class) {
			policy_id = policy_id.(interface{ Global_id() string }).Global_id()
		}
		if policy, ok := Local_policies_.Get(py.Str(policy_id)); ok && !py.IsNone(policy) {
			return policy
		}
	}
	if gid := Active_policy_gid_(); gid != "" {
		if policy, ok := Local_policies_.Get(gid); ok && !py.IsNone(policy) {
			return policy
		}
	}
	return nil
}

// Strip_class_token_ returns a shallow copy of data (dict-like) without
// ``class_token``.
func Strip_class_token_(data any) py.Kw {
	if py.IsNone(data) {
		return py.Kw{}
	}
	d, ok := asDict(Coerce_to_plain_dict_(data))
	if !ok {
		return py.Kw{}
	}
	out := py.Kw{}
	for _, it := range d.Items() {
		if k := py.Str(it.Key); k != "class_token" {
			out[k] = it.Value
		}
	}
	return out
}

// Coerce_to_plain_dict_ is a best-effort conversion of DotMap / nested
// dicts to plain dicts.
func Coerce_to_plain_dict_(value any) any {
	switch x := value.(type) {
	case *py.DotMap:
		return x.ToDict()
	case *py.Dict:
		out := py.NewDict()
		for _, it := range x.Items() {
			out.Set(it.Key, Coerce_to_plain_dict_(it.Value))
		}
		return out
	case py.Kw:
		return Coerce_to_plain_dict_(py.DictFromKw(x))
	case map[string]any:
		return Coerce_to_plain_dict_(py.DictFromMap(x))
	}
	return value
}

// Load_environment_ replaces the entire laila runtime with the policies
// described by env (4-rule active-policy resolution; see the Python docs).
// Protocols are registered but not started.
func Load_environment_(envIn any) error {
	envAny := Coerce_to_plain_dict_(envIn)
	env, ok := asDict(envAny)
	if !ok {
		return py.ValueError.Newf("environment must be a dict, got %s", py.Pytypename(envIn))
	}
	if !env.Contains("policies") {
		return py.ValueError.New("environment must contain a 'policies' key")
	}
	policies_raw := env.GetDefault("policies", nil)
	if !py.Truthy(policies_raw) {
		policies_raw = py.NewDict()
	}
	policies_dump, ok := asDict(policies_raw)
	if !ok {
		return py.ValueError.Newf("environment.policies must be a dict, got %s", py.Pytypename(policies_raw))
	}
	active_gid := env.GetDefault("active_gid", nil)
	if !py.IsNone(active_gid) && policies_dump.Len() == 0 {
		return py.ValueError.New("active_gid is set but environment.policies is empty")
	}

	Terminate(py.Kw{"wait": true, "cancel_pending": false})

	if policies_dump.Len() == 0 {
		new_policy, err := New_DefaultPolicy()
		if err != nil {
			return err
		}
		Activate_policy(new_policy)
		return nil
	}

	seen_uuids := py.NewSet()
	seen_proto_gids := py.NewSet()
	built_policies := []any{}

	buildErr := func() error {
		for _, it := range policies_dump.Items() {
			policy_gid := py.Str(it.Key)
			policy_data, ok := asDict(Coerce_to_plain_dict_(it.Value))
			if !ok {
				return py.ValueError.Newf("policy entry for %s must be a dict, got %s", py.Repr(policy_gid), py.Pytypename(it.Value))
			}
			ident, err := Process_global_id(policy_gid)
			if err != nil {
				return py.ValueError.Newf("invalid policy gid %s: %s", py.Repr(policy_gid), err.Error()).From(err)
			}
			policy_uuid := ident["uuid"].(string)
			if seen_uuids.Contains(policy_uuid) {
				return py.ValueError.Newf("duplicate policy uuid %s in environment.policies", py.Repr(policy_uuid))
			}
			seen_uuids.Add(policy_uuid)

			policy, err := Build_policy_from_dump_(policy_uuid, policy_data, seen_proto_gids)
			if err != nil {
				return err
			}
			if gid := py.Str(getattr(policy, "global_id")); gid != policy_gid {
				func() {
					defer func() { _ = recover() }()
					Shutdown_orphan_policy_(policy)
				}()
				return py.ValueError.Newf("reconstructed policy gid %s does not match dumped key %s", py.Repr(gid), py.Repr(policy_gid))
			}
			Local_policies_.Set(py.Str(getattr(policy, "global_id")), policy)
			built_policies = append(built_policies, policy)
		}
		return nil
	}()
	if buildErr != nil {
		Terminate(py.Kw{"wait": true, "cancel_pending": false})
		return buildErr
	}

	var chosen any
	if !py.IsNone(active_gid) {
		gid := py.Str(active_gid)
		if !policies_dump.Contains(gid) {
			return py.ValueError.Newf("active_gid %s not present in environment.policies", py.Repr(gid))
		}
		chosen, _ = Local_policies_.Get(gid)
	} else if len(built_policies) == 1 {
		chosen = built_policies[0]
	} else {
		return py.ValueError.New("active_gid required when env contains multiple policies")
	}
	Activate_policy(chosen)
	return nil
}

// Shutdown_orphan_policy_ is the best-effort tear-down of policy and all
// its sub-resources.
func Shutdown_orphan_policy_(policy any) {
	central := getattr(policy, "central")
	if comm := getattr(central, "communication"); !py.IsNone(comm) {
		_, _ = callm(comm, "stop")
	}
	if cmd := getattr(central, "command"); !py.IsNone(cmd) {
		_, _ = callm(cmd, "shutdown", py.Kw{"wait": true, "cancel_pending": false})
	}
	if mem := getattr(central, "memory"); !py.IsNone(mem) {
		if router := getattr(mem, "pool_router"); !py.IsNone(router) {
			for _, it := range mappingItems(getattr(router, "pools")) {
				if py.Hasattr(it.Value, "close") {
					_, _ = callm(it.Value, "close")
				}
			}
		}
	}
}

// Build_policy_from_dump_ builds one DefaultPolicy from a policy_data dump
// branch, re-creating taskforces, pools and comm-protocols via
// ``class_token``.
func Build_policy_from_dump_(policy_uuid string, policy_data *py.Dict, seen_proto_gids *py.Set) (any, error) {
	policy, err := New_DefaultPolicy()
	if err != nil {
		return nil, err
	}
	if policy_uuid != py.Str(getattr(policy, "uuid")) {
		_ = py.Setattr(policy, "_uuid", policy_uuid)
		new_gid := py.Str(getattr(policy, "global_id"))
		central := getattr(policy, "central")
		if cmd := getattr(central, "command"); !py.IsNone(cmd) {
			_ = py.Setattr(cmd, "policy_id", new_gid)
			for _, it := range mappingItems(getattr(cmd, "taskforces")) {
				_ = py.Setattr(it.Value, "policy_id", new_gid)
			}
		}
		if comm := getattr(central, "communication"); !py.IsNone(comm) {
			_ = py.Setattr(comm, "policy_id", new_gid)
		}
	}
	Local_policies_.Set(py.Str(getattr(policy, "global_id")), policy)
	central := getattr(policy, "central")

	sub := func(d *py.Dict, key string) *py.Dict {
		v := Coerce_to_plain_dict_(d.GetDefault(key, nil))
		if out, ok := asDict(v); ok {
			return out
		}
		return py.NewDict()
	}
	central_data := sub(policy_data, "central")
	cmd_data := sub(central_data, "command")
	tf_dump := sub(cmd_data, "taskforces")

	if tf_dump.Len() > 0 {
		cmd := getattr(central, "command")
		for _, it := range mappingItems(getattr(cmd, "taskforces")) {
			if py.Hasattr(it.Value, "shutdown") {
				_, _ = callm(it.Value, "shutdown", py.Kw{"wait": true, "cancel_pending": true})
			}
		}
		_, _ = callm(getattr(cmd, "taskforces"), "clear")

		tfRoot, _ := py.Class_by_name("_LAILA_IDENTIFIABLE_TASK_FORCE")
		for _, it := range tf_dump.Items() {
			tf_gid := py.Str(it.Key)
			tf_data_plain, ok := asDict(Coerce_to_plain_dict_(it.Value))
			if !ok {
				return nil, py.ValueError.Newf("taskforce entry %s must be a dict", py.Repr(tf_gid))
			}
			token := tf_data_plain.GetDefault("class_token", nil)
			if py.IsNone(token) {
				return nil, py.ValueError.Newf("taskforce entry %s is missing 'class_token'", py.Repr(tf_gid))
			}
			cls, err := Resolve_class_(py.Str(token), tfRoot)
			if err != nil {
				return nil, err
			}
			tf_kwargs := Strip_class_token_(tf_data_plain)
			delete(tf_kwargs, "status")
			delete(tf_kwargs, "queue_len")
			ident, err := Process_global_id(tf_gid)
			if err != nil {
				return nil, py.ValueError.Newf("invalid taskforce gid %s: %s", py.Repr(tf_gid), err.Error()).From(err)
			}
			tf_uuid := ident["uuid"].(string)
			tf_kwargs["uuid"] = tf_uuid
			tf_kwargs["policy_id"] = getattr(policy, "global_id")
			tf, err := newInstance(cls, tf_kwargs)
			if err != nil {
				return nil, err
			}
			if py.Str(getattr(tf, "uuid")) != tf_uuid {
				_ = py.Setattr(tf, "_uuid", tf_uuid)
			}
			if _, err := callm(cmd, "add_taskforce", tf); err != nil {
				return nil, err
			}
		}

		taskforces := getattr(cmd, "taskforces")
		alpha_tf := cmd_data.GetDefault("alpha_taskforce", nil)
		if !py.IsNone(alpha_tf) && mappingContains(taskforces, py.Str(alpha_tf)) {
			_ = py.Setattr(cmd, "alpha_taskforce", py.Str(alpha_tf))
		} else if items := mappingItems(taskforces); len(items) > 0 {
			_ = py.Setattr(cmd, "alpha_taskforce", items[0].Key)
		}
		internal_tf := cmd_data.GetDefault("internal_taskforce", nil)
		if !py.IsNone(internal_tf) && mappingContains(taskforces, py.Str(internal_tf)) {
			_ = py.Setattr(cmd, "internal_taskforce", py.Str(internal_tf))
		} else {
			// Environments dumped before the internal/alpha split run laila's
			// internals on the alpha taskforce.
			_ = py.Setattr(cmd, "internal_taskforce", getattr(cmd, "alpha_taskforce"))
		}
	}

	mem_data := sub(central_data, "memory")
	router_data := sub(mem_data, "pool_router")
	pool_dump := sub(router_data, "pools")

	if pool_dump.Len() > 0 {
		mem := getattr(central, "memory")
		router := getattr(mem, "pool_router")
		for _, it := range mappingItems(getattr(router, "pools")) {
			if py.Hasattr(it.Value, "close") {
				_, _ = callm(it.Value, "close")
			}
		}
		_, _ = callm(getattr(router, "pools"), "clear")
		_, _ = callm(router, "reset_pools_pq_")
		nicks_in := sub(router_data, "pools_nicknames")
		gid_to_nick := map[string]string{}
		for _, it := range nicks_in.Items() {
			gid_to_nick[py.Str(it.Value)] = py.Str(it.Key)
		}
		_, _ = callm(getattr(router, "pools_nicknames"), "clear")

		poolRoot, _ := py.Class_by_name("_LAILA_IDENTIFIABLE_POOL")
		first_pool_gid := ""
		for _, it := range pool_dump.Items() {
			pool_gid := py.Str(it.Key)
			pool_data_plain, ok := asDict(Coerce_to_plain_dict_(it.Value))
			if !ok {
				return nil, py.ValueError.Newf("pool entry %s must be a dict", py.Repr(pool_gid))
			}
			token := pool_data_plain.GetDefault("class_token", nil)
			if py.IsNone(token) {
				return nil, py.ValueError.Newf("pool entry %s is missing 'class_token'", py.Repr(pool_gid))
			}
			cls, err := Resolve_class_(py.Str(token), poolRoot)
			if err != nil {
				return nil, err
			}
			pool_kwargs := Strip_class_token_(pool_data_plain)
			delete(pool_kwargs, "pool_id")
			ident, err := Process_global_id(pool_gid)
			if err != nil {
				return nil, py.ValueError.Newf("invalid pool gid %s: %s", py.Repr(pool_gid), err.Error()).From(err)
			}
			pool_uuid := ident["uuid"].(string)
			pool_kwargs["uuid"] = pool_uuid
			pool, err := newInstance(cls, pool_kwargs)
			if err != nil {
				return nil, err
			}
			if py.Str(getattr(pool, "uuid")) != pool_uuid {
				_ = py.Setattr(pool, "_uuid", pool_uuid)
			}
			var nickname any
			if nick, ok := gid_to_nick[py.Str(getattr(pool, "global_id"))]; ok {
				nickname = nick
			}
			if _, err := callm(router, "extend", pool, py.Kw{"affinity": nil, "pool_nickname": nickname}); err != nil {
				return nil, err
			}
			if first_pool_gid == "" {
				first_pool_gid = py.Str(getattr(pool, "global_id"))
			}
		}
		alpha := mem_data.GetDefault("alpha_pool", nil)
		if !py.IsNone(alpha) && mappingContains(getattr(router, "pools"), py.Str(alpha)) {
			_ = py.Setattr(mem, "alpha_pool", py.Str(alpha))
		} else if first_pool_gid != "" {
			_ = py.Setattr(mem, "alpha_pool", first_pool_gid)
		}
	}

	comm_data := sub(central_data, "communication")
	conn_dump := sub(comm_data, "connections")
	if conn_dump.Len() > 0 {
		comm := getattr(central, "communication")
		protoRoot, _ := py.Class_by_name("_LAILA_IDENTIFIABLE_COMM_PROTOCOL")
		for _, it := range conn_dump.Items() {
			proto_gid := py.Str(it.Key)
			proto_data_plain, ok := asDict(Coerce_to_plain_dict_(it.Value))
			if !ok {
				return nil, py.ValueError.Newf("protocol entry %s must be a dict", py.Repr(proto_gid))
			}
			token := proto_data_plain.GetDefault("class_token", nil)
			if py.IsNone(token) {
				return nil, py.ValueError.Newf("protocol entry %s is missing 'class_token'", py.Repr(proto_gid))
			}
			cls, err := Resolve_class_(py.Str(token), protoRoot)
			if err != nil {
				return nil, err
			}
			proto_kwargs := Strip_class_token_(proto_data_plain)
			ident, err := Process_global_id(proto_gid)
			if err != nil {
				return nil, py.ValueError.Newf("invalid protocol gid %s: %s", py.Repr(proto_gid), err.Error()).From(err)
			}
			proto_uuid := ident["uuid"].(string)
			proto_kwargs["uuid"] = proto_uuid
			proto, err := newInstance(cls, proto_kwargs)
			if err != nil {
				return nil, err
			}
			if py.Str(getattr(proto, "uuid")) != proto_uuid {
				_ = py.Setattr(proto, "_uuid", proto_uuid)
			}
			full_gid := py.Str(getattr(proto, "global_id"))
			if seen_proto_gids.Contains(full_gid) {
				return nil, py.ValueError.Newf("duplicate protocol gid %s across policies", py.Repr(full_gid))
			}
			seen_proto_gids.Add(full_gid)
			_ = py.Setattr(proto, "_communication", comm)
			if _, err := callm(getattr(comm, "connections"), "__setitem__", full_gid, proto); err != nil {
				if d, ok := getattr(comm, "connections").(interface{ Setitem(any, any) }); ok {
					d.Setitem(full_gid, proto)
				} else if d, ok := getattr(comm, "connections").(*py.Dict); ok {
					d.Set(full_gid, proto)
				}
			}
		}
	}
	return policy, nil
}

// newInstance is ``cls(**kwargs)`` for a registered class.
func newInstance(cls *py.Class, kwargs py.Kw) (any, error) {
	return py.New_instance(cls, kwargs)
}

// mappingContains is ``key in mapping``.
func mappingContains(v any, key string) bool {
	switch x := v.(type) {
	case *py.Dict:
		return x.Contains(key)
	case *py.DotMap:
		return x.Has(key)
	case interface{ Contains(any) bool }:
		return x.Contains(key)
	}
	for _, it := range mappingItems(v) {
		if py.Str(it.Key) == key {
			return true
		}
	}
	return false
}

// Mark_environment_dirty_ records that instance's owning policy needs a
// fresh mirror entry (O(1)); the logger branch is written immediately.
func Mark_environment_dirty_(instance any) {
	if scope, _ := Scope_for_class_(py.Classof(instance)); scope == "LOGGER" {
		Refresh_args_environment_(instance)
		return
	}
	if py.Truthy(FLUSH_STATE_.GetOr("active", false)) {
		return
	}
	policy := Find_owning_policy_(instance)
	if py.IsNone(policy) {
		return
	}
	DIRTY_LOCK_.Lock_()
	DIRTY_POLICY_GIDS_.Add(py.Str(getattr(policy, "global_id")))
	DIRTY_LOCK_.Unlock()
}

// Flush_environment_mirror_ writes the mirror entry of every dirty policy
// (at most once each). Re-entrant reads from inside the flush are no-ops.
func Flush_environment_mirror_() {
	if py.Truthy(FLUSH_STATE_.GetOr("active", false)) {
		return
	}
	DIRTY_LOCK_.Lock_()
	if DIRTY_POLICY_GIDS_.Len() == 0 {
		DIRTY_LOCK_.Unlock()
		return
	}
	gids := DIRTY_POLICY_GIDS_.Items()
	DIRTY_POLICY_GIDS_.Clear()
	DIRTY_LOCK_.Unlock()

	FLUSH_STATE_.Set("active", true)
	defer FLUSH_STATE_.Set("active", false)
	for _, gid := range gids {
		policy, _ := Local_policies_.Get(gid)
		if py.IsNone(policy) {
			policy = Live_policies_get_(py.Str(gid))
		}
		if py.IsNone(policy) {
			continue
		}
		func() {
			defer func() { _ = recover() }()
			Refresh_args_environment_(policy)
		}()
	}
}

// Clear_environment_dirty_ forgets every pending mirror update (Terminate).
func Clear_environment_dirty_() {
	DIRTY_LOCK_.Lock_()
	DIRTY_POLICY_GIDS_.Clear()
	DIRTY_LOCK_.Unlock()
}

// Refresh_args_environment_ writes the owning policy's Build_environment
// dump under ``laila.args.environment.policies[<gid>]`` now (or
// ``laila.args.environment.logger`` for the logger singleton). The mirror
// maps are mutated in place so the write cannot trip the environment-load
// hook on laila.Args.
func Refresh_args_environment_(instance any) {
	args := Args
	was_active := FLUSH_STATE_.GetOr("active", false)
	FLUSH_STATE_.Set("active", true)
	defer FLUSH_STATE_.Set("active", was_active)

	envAny, _ := args.Lookup("environment")
	env, ok := envAny.(*py.DotMap)
	if !ok {
		env = py.NewDotMap()
		args.Set("environment", env)
		stored, _ := args.Lookup("environment")
		env = envAny2DotMap(stored)
	}

	if scope, _ := Scope_for_class_(py.Classof(instance)); scope == "LOGGER" {
		dump := py.DictOf("class_token", py.Classof(instance).Name)
		for _, name := range Eligible_model_fields_(py.Classof(instance)) {
			dump.Set(name, getattr(instance, name))
		}
		for _, name := range Eligible_property_setters_(py.Classof(instance)) {
			dump.Set(name, getattr(instance, name))
		}
		env.Set("logger", py.NewDotMap(dump))
		return
	}

	policy := Find_owning_policy_(instance)
	if py.IsNone(policy) {
		return
	}
	policiesAny, _ := env.Lookup("policies")
	policies, ok := policiesAny.(*py.DotMap)
	if !ok {
		policies = py.NewDotMap()
		env.Set("policies", policies)
		policiesAny, _ = env.Lookup("policies")
		policies = policiesAny.(*py.DotMap)
	}
	dump := Build_environment(policy)
	policies.Set(py.Str(getattr(policy, "global_id")), py.NewDotMap(dump.GetDefault("policy", nil)))
}

// ---------------------------------------------------------------------------
// dynamic-attribute helpers (Python's getattr(x, name, None) / hasattr)
// ---------------------------------------------------------------------------

// getattr is ``getattr(obj, name, None)``.
func getattr(obj any, name string) any {
	if py.IsNone(obj) {
		return nil
	}
	v, err := py.Getattr(obj, name)
	if err != nil {
		return nil
	}
	return v
}

// callm is ``obj.name(*args, **kw)`` through the dynamic protocol.
func callm(obj any, name string, args ...any) (any, error) {
	m, err := py.Getattr(obj, name)
	if err != nil {
		return nil, err
	}
	pos, kw := py.SplitKw(args)
	return py.Call(m, pos, kw)
}

// asDict views dict-like values (*py.Dict, py.Kw, map, DotMap) as *py.Dict.
func asDict(v any) (*py.Dict, bool) {
	switch x := v.(type) {
	case *py.Dict:
		return x, true
	case py.Kw:
		return py.DictFromKw(x), true
	case map[string]any:
		return py.DictFromMap(x), true
	case *py.DotMap:
		return x.ToDict(), true
	}
	return nil, false
}

// mappingItems iterates any mapping-like value (dict, DotMap, AtomicDict
// or anything with Items()).
func mappingItems(v any) []py.Item {
	switch x := v.(type) {
	case nil:
		return nil
	case *py.Dict:
		return x.Items()
	case *py.DotMap:
		return x.Items()
	case py.Kw:
		return py.DictFromKw(x).Items()
	case map[string]any:
		return py.DictFromMap(x).Items()
	case interface{ Items() []py.Item }:
		return x.Items()
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Map {
		var out []py.Item
		for _, k := range rv.MapKeys() {
			out = append(out, py.Item{Key: k.Interface(), Value: rv.MapIndex(k).Interface()})
		}
		sort.Slice(out, func(i, j int) bool { return py.Str(out[i].Key) < py.Str(out[j].Key) })
		return out
	}
	return nil
}

// hasKeys is ``hasattr(x, "keys")``.
func hasKeys(v any) bool {
	switch v.(type) {
	case *py.Dict, *py.DotMap, py.Kw, map[string]any:
		return true
	case interface{ Keys() []any }, interface{ Keys() []string }:
		return true
	}
	return false
}

// dotmapGet is ``x.get(key)`` for DotMap / dict-like values (no
// auto-vivification); nil when x has no ``get``.
func dotmapGet(v any, key string) any {
	switch x := v.(type) {
	case *py.DotMap:
		val, _ := x.Lookup(key)
		return val
	case *py.Dict:
		return x.GetDefault(key, nil)
	case py.Kw:
		return x.Get(key, nil)
	case map[string]any:
		return x[key]
	case interface{ Get(key string) any }:
		return x.Get(key)
	}
	return nil
}

func envAny2DotMap(v any) *py.DotMap {
	if dm, ok := v.(*py.DotMap); ok {
		return dm
	}
	return py.NewDotMap()
}
