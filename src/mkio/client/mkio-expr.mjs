/**
 * mkio expression language — JavaScript implementation.
 *
 * Auto-served at /mkio-expr.js by the mkio server (ES module). Mirrors
 * mkio/expr/ in Python section for section; both run tests/expr_cases.json.
 * Load with <script type="module" src="/mkio-expr.js"></script> — it also
 * assigns globalThis.mkioExpr for classic scripts (mkio.js's string filters).
 */

export const LANGUAGE_VERSION = "1";

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

export class ExprError extends Error {
  constructor(message, pos = null) {
    super(pos == null ? message : `${message} (at position ${pos})`);
    this.name = "ExprError";
    this.detail = message;
    this.pos = pos;
  }
}

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

export const KEYWORDS = new Set(["TRUE", "FALSE", "NULL"]);
const OPS2 = ["|>", "->", "??", "**", "//", "&&", "||", "==", "!=", "<=", ">="];
const OPS1 = "<>+-*/%!";
const ESCAPES = { n: "\n", t: "\t", r: "\r", "\\": "\\", "'": "'", '"': '"' };
const DOT_AFTER = new Set(["IDENT", "RPAREN", "RBRACKET", "RBRACE", "NUMBER", "STRING"]);
const SIMPLE = { "(": "LPAREN", ")": "RPAREN", "[": "LBRACKET", "]": "RBRACKET", "{": "LBRACE", "}": "RBRACE", ",": "COMMA", ":": "COLON" };

const isDigit = (c) => c >= "0" && c <= "9";
const isAlpha = (c) => /[A-Za-z_]/.test(c) || (c > "\x7f" && /\p{L}/u.test(c));
const isAlnum = (c) => isAlpha(c) || isDigit(c);

export function tokenize(expr) {
  const tokens = [];
  const n = expr.length;
  let i = 0;
  while (i < n) {
    const c = expr[i];
    if (c === " " || c === "\t" || c === "\r" || c === "\n") { i++; continue; }

    if (c === "'" || c === '"') {
      const start = i;
      const quote = c;
      i++;
      let out = "";
      while (i < n && expr[i] !== quote) {
        const ch = expr[i];
        if (ch === "\\") {
          i++;
          if (i >= n) break;
          const esc = expr[i];
          if (esc === "u" && expr[i + 1] === "{") {
            const end = expr.indexOf("}", i + 2);
            if (end < 0) throw new ExprError("Unterminated \\u{...} escape", i);
            const hex = expr.slice(i + 2, end);
            if (!/^[0-9a-fA-F]+$/.test(hex)) throw new ExprError("Bad \\u{...} escape", i);
            out += String.fromCodePoint(parseInt(hex, 16));
            i = end + 1;
            continue;
          }
          if (!(esc in ESCAPES)) throw new ExprError(`Unknown escape: \\${esc}`, i);
          out += ESCAPES[esc];
          i++;
          continue;
        }
        out += ch;
        i++;
      }
      if (i >= n) throw new ExprError("Unterminated string literal", start);
      i++;
      tokens.push({ type: "STRING", value: out, pos: start });
      continue;
    }

    if (c === "`") {
      const end = expr.indexOf("`", i + 1);
      if (end < 0) throw new ExprError("Unterminated backtick identifier", i);
      if (end === i + 1) throw new ExprError("Empty backtick identifier", i);
      tokens.push({ type: "IDENT", value: expr.slice(i + 1, end), pos: i });
      i = end + 1;
      continue;
    }

    if (c === "." && tokens.length && DOT_AFTER.has(tokens[tokens.length - 1].type)) {
      tokens.push({ type: "DOT", value: ".", pos: i });
      i++;
      continue;
    }

    if (isDigit(c) || (c === "." && isDigit(expr[i + 1] || ""))) {
      const start = i;
      while (i < n && (isDigit(expr[i]) || expr[i] === "_")) i++;
      if (expr[i] === "." && isDigit(expr[i + 1] || "")) {
        i++;
        while (i < n && (isDigit(expr[i]) || expr[i] === "_")) i++;
      }
      if (expr[i] === "e" || expr[i] === "E") {
        let j = i + 1;
        if (expr[j] === "+" || expr[j] === "-") j++;
        if (isDigit(expr[j] || "")) {
          i = j;
          while (i < n && isDigit(expr[i])) i++;
        }
      }
      const text = expr.slice(start, i);
      if (text.startsWith("_") || text.endsWith("_") || text.includes("__")) throw new ExprError(`Bad number literal: ${text}`, start);
      if (i < n && isAlpha(expr[i])) throw new ExprError(`Bad number literal: ${expr.slice(start, i + 1)}`, start);
      tokens.push({ type: "NUMBER", value: text.replace(/_/g, ""), pos: start });
      continue;
    }

    const two = expr.slice(i, i + 2);
    if (OPS2.includes(two)) {
      tokens.push({ type: two === "|>" ? "PIPE" : two === "->" ? "ARROW" : "OP", value: two, pos: i });
      i += 2;
      continue;
    }
    if (OPS1.includes(c)) { tokens.push({ type: "OP", value: c, pos: i }); i++; continue; }
    if (c in SIMPLE) { tokens.push({ type: SIMPLE[c], value: c, pos: i }); i++; continue; }

    if (isAlpha(c)) {
      const start = i;
      while (i < n && isAlnum(expr[i])) i++;
      const word = expr.slice(start, i);
      tokens.push({ type: KEYWORDS.has(word.toUpperCase()) ? "KEYWORD" : "IDENT", value: word, pos: start });
      continue;
    }
    throw new ExprError(`Unexpected character: '${c}'`, i);
  }
  tokens.push({ type: "EOF", value: "", pos: n });
  return tokens;
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

const COMPARE = new Set(["==", "!=", "<", "<=", ">", ">="]);
const TOKEN_NAMES = { RPAREN: "')'", RBRACKET: "']'", RBRACE: "'}'", COLON: "':'", COMMA: "','", IDENT: "a name" };
const node = (type, props) => ({ type, ...props });

class Parser {
  constructor(tokens) { this.tokens = tokens; this.pos = 0; }
  peek(k = 0) { const i = this.pos + k; return i < this.tokens.length ? this.tokens[i] : this.tokens[this.tokens.length - 1]; }
  advance() { return this.tokens[this.pos++]; }
  at(type, value) { const t = this.peek(); return t.type === type && (value === undefined || t.value === value); }
  expect(type, value) {
    const t = this.peek();
    if (t.type !== type || (value !== undefined && t.value !== value)) {
      const want = value !== undefined ? value : (TOKEN_NAMES[type] || type.toLowerCase());
      const got = t.type === "EOF" ? "end of expression" : `'${t.value}'`;
      throw new ExprError(`Expected ${want}, got ${got}`, t.pos);
    }
    return this.advance();
  }

  parse() {
    const n = this.parseExpr();
    if (!this.at("EOF")) { const t = this.peek(); throw new ExprError(`Unexpected token: '${t.value}'`, t.pos); }
    return n;
  }

  parseExpr() {
    const lam = this.tryLambda();
    if (lam) return lam;
    let left = this.parseOr();
    while (this.at("PIPE")) {
      const pipe = this.advance();
      const msg = "Right side of |> must be a parenthesized lambda: (x -> ...)";
      if (!this.at("LPAREN")) throw new ExprError(msg, this.peek().pos);
      const start = this.peek().pos;
      const right = this.parsePostfix(this.parsePrimary());
      if (right.type !== "Lambda") throw new ExprError(msg, start);
      left = node("Apply", { fn: right, arg: left, pos: pipe.pos });
    }
    return left;
  }

  tryLambda() {
    const tok = this.peek();
    if (tok.type === "IDENT" && this.peek(1).type === "ARROW") {
      this.advance(); this.advance();
      return node("Lambda", { params: [tok.value], body: this.parseExpr(), pos: tok.pos });
    }
    if (tok.type === "LPAREN") {
      let k = 1;
      const params = [];
      for (;;) {
        const t = this.peek(k);
        if (t.type !== "IDENT") return null;
        params.push(t.value);
        k++;
        const u = this.peek(k);
        if (u.type === "COMMA") { k++; continue; }
        if (u.type === "RPAREN") { k++; break; }
        return null;
      }
      if (this.peek(k).type !== "ARROW") return null;
      this.pos += k + 1;
      return node("Lambda", { params, body: this.parseExpr(), pos: tok.pos });
    }
    return null;
  }

  parseOr() {
    let left = this.parseAnd();
    while (this.at("OP", "||")) { const p = this.advance().pos; left = node("Binary", { op: "||", left, right: this.parseAnd(), pos: p }); }
    return left;
  }
  parseAnd() {
    let left = this.parseComparison();
    while (this.at("OP", "&&")) { const p = this.advance().pos; left = node("Binary", { op: "&&", left, right: this.parseComparison(), pos: p }); }
    return left;
  }
  parseComparison() {
    const left = this.parseCoalesce();
    const t = this.peek();
    if (t.type === "OP" && COMPARE.has(t.value)) {
      this.advance();
      const right = this.parseCoalesce();
      const nx = this.peek();
      if (nx.type === "OP" && COMPARE.has(nx.value)) throw new ExprError("Comparisons don't chain — use && to combine them", nx.pos);
      return node("Binary", { op: t.value, left, right, pos: t.pos });
    }
    return left;
  }
  parseCoalesce() {
    let left = this.parseAdditive();
    while (this.at("OP", "??")) { const p = this.advance().pos; left = node("Binary", { op: "??", left, right: this.parseAdditive(), pos: p }); }
    return left;
  }
  parseAdditive() {
    let left = this.parseMultiplicative();
    while (this.peek().type === "OP" && (this.peek().value === "+" || this.peek().value === "-")) {
      const t = this.advance();
      left = node("Binary", { op: t.value, left, right: this.parseMultiplicative(), pos: t.pos });
    }
    return left;
  }
  parseMultiplicative() {
    let left = this.parseUnary();
    while (this.peek().type === "OP" && ["*", "/", "//", "%"].includes(this.peek().value)) {
      const t = this.advance();
      left = node("Binary", { op: t.value, left, right: this.parseUnary(), pos: t.pos });
    }
    return left;
  }
  parseUnary() {
    const t = this.peek();
    if (t.type === "OP" && (t.value === "-" || t.value === "!")) {
      this.advance();
      return node("Unary", { op: t.value, operand: this.parseUnary(), pos: t.pos });
    }
    return this.parsePower();
  }
  parsePower() {
    const base = this.parsePostfix(this.parsePrimary());
    if (this.at("OP", "**")) { const p = this.advance().pos; return node("Binary", { op: "**", left: base, right: this.parseUnary(), pos: p }); }
    return base;
  }
  parsePostfix(n) {
    for (;;) {
      const t = this.peek();
      if (t.type === "LBRACKET") {
        this.advance();
        const key = this.parseExpr();
        this.expect("RBRACKET");
        n = node("Index", { target: n, key, pos: t.pos });
      } else if (t.type === "DOT") {
        this.advance();
        const u = this.peek();
        if (u.type === "IDENT" || u.type === "KEYWORD") { this.advance(); n = node("Index", { target: n, key: node("Literal", { value: u.value, pos: u.pos }), pos: t.pos }); }
        else if (u.type === "NUMBER") { this.advance(); n = node("Index", { target: n, key: node("Literal", { value: Number(u.value), pos: u.pos }), pos: t.pos }); }
        else throw new ExprError("Expected a name after '.'", u.pos);
      } else return n;
    }
  }
  parsePrimary() {
    const t = this.peek();
    if (t.type === "LPAREN") {
      const lam = this.tryLambda();
      if (lam) return lam;
      this.advance();
      const n = this.parseExpr();
      this.expect("RPAREN");
      return n;
    }
    if (t.type === "LBRACKET") {
      this.advance();
      const elements = [];
      while (!this.at("RBRACKET")) {
        elements.push(this.parseExpr());
        if (!this.at("COMMA")) break;
        this.advance();
      }
      this.expect("RBRACKET");
      return node("Array", { elements, pos: t.pos });
    }
    if (t.type === "LBRACE") {
      this.advance();
      const keys = [], values = [];
      while (!this.at("RBRACE")) {
        const kt = this.peek();
        if (kt.type === "IDENT" || kt.type === "STRING" || kt.type === "KEYWORD") { this.advance(); keys.push(kt.value); }
        else throw new ExprError("Map key must be a name or string", kt.pos);
        this.expect("COLON");
        values.push(this.parseExpr());
        if (!this.at("COMMA")) break;
        this.advance();
      }
      this.expect("RBRACE");
      return node("Map", { keys, values, pos: t.pos });
    }
    if (t.type === "STRING") { this.advance(); return node("Literal", { value: t.value, pos: t.pos }); }
    if (t.type === "NUMBER") { this.advance(); return node("Literal", { value: Number(t.value), pos: t.pos }); }
    if (t.type === "KEYWORD") { this.advance(); return node("Literal", { value: { TRUE: true, FALSE: false, NULL: null }[t.value.toUpperCase()], pos: t.pos }); }
    if (t.type === "IDENT") {
      this.advance();
      if (this.at("LPAREN")) {
        this.advance();
        const args = [], kwargs = [];
        while (!this.at("RPAREN")) {
          if (this.peek().type === "IDENT" && this.peek(1).type === "COLON") {
            const kt = this.advance(); this.advance();
            kwargs.push([kt.value, this.parseExpr()]);
          } else {
            if (kwargs.length) throw new ExprError("Positional argument after named argument", this.peek().pos);
            args.push(this.parseExpr());
          }
          if (!this.at("COMMA")) break;
          this.advance();
        }
        this.expect("RPAREN");
        return node("Call", { name: t.value.toUpperCase(), args, kwargs, pos: t.pos });
      }
      return node("Name", { name: t.value, pos: t.pos });
    }
    if (t.type === "ARROW") throw new ExprError("Lambda parameters must be names: x -> ... or (a, b) -> ...", t.pos);
    if (t.type === "EOF") throw new ExprError("Unexpected end of expression", t.pos);
    throw new ExprError(`Unexpected token: '${t.value}'`, t.pos);
  }
}

export function parse(expr) {
  if (typeof expr !== "string") throw new ExprError(`Expression must be a string, got ${typeof expr}`);
  return new Parser(tokenize(expr)).parse();
}

// ---------------------------------------------------------------------------
// Registries & environments
// ---------------------------------------------------------------------------

export const LIBRARIES = new Map();   // name -> Map(NAME -> FunctionDef)
export const TYPES = new Map();       // name -> TypeDef

export function registerFunction(name, fn, meta = {}) {
  const upper = name.toUpperCase();
  const library = meta.library || "user";
  if (KEYWORDS.has(upper)) throw new ExprError(`Cannot register function with reserved name: ${name}`);
  if (!/^[A-Z_][A-Z0-9_]*$/.test(upper)) throw new ExprError(`Invalid function name: '${name}'`);
  for (const [lib, fns] of LIBRARIES) {
    if (lib !== library && fns.has(upper)) throw new ExprError(`Function ${upper} already registered in library '${lib}'`);
  }
  const fdef = {
    name: upper, fn, library,
    lazy: !!meta.lazy, numeric: !!meta.numeric,
    params: meta.params ? [...meta.params] : [],
    required: meta.required ?? null,   // minimum positional count (null = no check)
    variadic: !!meta.variadic,
    doc: meta.doc || "",
  };
  if (!LIBRARIES.has(library)) LIBRARIES.set(library, new Map());
  LIBRARIES.get(library).set(upper, fdef);
  return fdef;
}

export function registerLibrary(name, functions) {
  for (const [fname, spec] of Object.entries(functions)) {
    if (Array.isArray(spec)) registerFunction(fname, spec[0], { ...spec[1], library: name });
    else registerFunction(fname, spec, { library: name });
  }
}

export function unregisterLibrary(name) { LIBRARIES.delete(name); }

export function registerType(name, isInstance, hooks = {}) {
  const tdef = { name, isInstance, add: hooks.add || null, toString: hooks.toString || null, truthy: hooks.truthy || null, compare: hooks.compare || null, concat: hooks.concat || null };
  TYPES.set(name, tdef);
  return tdef;
}

export function findType(value) {
  for (const t of TYPES.values()) if (t.isInstance(value)) return t;
  return null;
}

export class Env {
  constructor(opts = {}) {
    this.libraries = opts.libraries ? [...opts.libraries] : null;
    this.strict = opts.strict !== undefined ? !!opts.strict : true;
  }
  function(name) {
    const upper = name.toUpperCase();
    const libs = this.libraries || [...LIBRARIES.keys()];
    for (const lib of libs) {
      const fns = LIBRARIES.get(lib);
      if (fns && fns.has(upper)) return fns.get(upper);
    }
    return null;
  }
  functions() {
    const out = new Map();
    for (const lib of this.libraries || [...LIBRARIES.keys()]) for (const [k, v] of LIBRARIES.get(lib) || []) out.set(k, v);
    return out;
  }
}

export const defaultEnv = new Env();

// ---------------------------------------------------------------------------
// Value semantics
// ---------------------------------------------------------------------------

export class Closure {
  constructor(params, body, scope) { this.params = params; this.body = body; this.scope = scope; }
  call(...args) {
    if (args.length !== this.params.length) throw new ExprError(`Lambda expects ${this.params.length} argument(s), got ${args.length}`);
    const vars = {};
    this.params.forEach((p, i) => { vars[p] = args[i]; });
    return this.body(this.scope.child(vars));
  }
}

const isPlainObject = (v) => v !== null && typeof v === "object" && !Array.isArray(v) && (Object.getPrototypeOf(v) === Object.prototype || Object.getPrototypeOf(v) === null);
export const isNumber = (v) => typeof v === "number";
const isCallable = (v) => v instanceof Closure || typeof v === "function";

export function kind(v) {
  if (v === null || v === undefined) return "null";
  if (typeof v === "boolean") return "boolean";
  if (typeof v === "number") return "number";
  if (typeof v === "string") return "string";
  if (Array.isArray(v)) return "array";
  if (isPlainObject(v)) return "map";
  if (isCallable(v)) return "function";
  const t = findType(v);
  if (t) return t.name;
  return v.constructor ? v.constructor.name : typeof v;
}

export function truthy(v) {
  if (v === null || v === undefined || v === false) return false;
  if (v === true) return true;
  if (typeof v === "number") return v !== 0;
  if (typeof v === "string" || Array.isArray(v)) return v.length > 0;
  if (isPlainObject(v)) return Object.keys(v).length > 0;
  const t = findType(v);
  if (t && t.truthy) return !!t.truthy(v);
  return true;
}

export function numberToString(v) {
  if (Object.is(v, -0)) return "0";
  return String(v);
}

export function toString(v) {
  if (v === null || v === undefined) return "";
  if (v === true) return "true";
  if (v === false) return "false";
  if (typeof v === "number") return numberToString(v);
  if (typeof v === "string") return v;
  if (Array.isArray(v)) return "[" + v.map(repr).join(", ") + "]";
  if (isPlainObject(v)) return "{" + Object.entries(v).map(([k, x]) => `${k}: ${repr(x)}`).join(", ") + "}";
  const t = findType(v);
  if (t && t.toString) return t.toString(v);
  if (isCallable(v)) return "<function>";
  return String(v);
}

function repr(v) {
  if (typeof v === "string") return "'" + v.replace(/\\/g, "\\\\").replace(/'/g, "\\'") + "'";
  if (v === null || v === undefined) return "null";
  return toString(v);
}

export function equals(a, b) {
  const ka = kind(a), kb = kind(b);
  if (ka !== kb) return false;
  if (ka === "array") return a.length === b.length && a.every((x, i) => equals(x, b[i]));
  if (ka === "map") {
    const ak = Object.keys(a), bk = Object.keys(b);
    return ak.length === bk.length && ak.every((k) => Object.hasOwn(b, k) && equals(a[k], b[k]));
  }
  if (ka === "null") return true;
  const t = findType(a);
  if (t && t.compare) return t.compare(a, b) === 0;
  return a === b;
}

export function compare(a, b, op, pos = null) {
  if (isNumber(a) && isNumber(b)) return a > b ? 1 : a < b ? -1 : 0;
  if (typeof a === "string" && typeof b === "string") return a > b ? 1 : a < b ? -1 : 0;
  const t = findType(a);
  if (t && t.compare && t.isInstance(b)) return t.compare(a, b);
  throw new ExprError(`Cannot compare ${kind(a)} ${op} ${kind(b)}`, pos);
}

export function add(a, b, pos = null) {
  if (isNumber(a) && isNumber(b)) return a + b;
  if (typeof a === "string" && typeof b === "string") return a + b;
  const ta = findType(a);
  if (ta && ta.add) return ta.add(a, b);
  const tb = findType(b);
  if (tb && tb.add) return tb.add(a, b);
  throw new ExprError(`Cannot add ${kind(a)} + ${kind(b)}`, pos);
}

function requireNumber(v, what, pos = null) {
  if (!isNumber(v)) throw new ExprError(`${what} requires a number, got ${kind(v)}`, pos);
  return v;
}

const isBlockedKey = (k) => typeof k === "string" && (k.startsWith("__") || k === "constructor" || k === "prototype");
const chars = (s) => Array.from(s);

// ---------------------------------------------------------------------------
// Evaluator
// ---------------------------------------------------------------------------

export class Scope {
  constructor(vars, parent = null, strict = true) { this.vars = vars; this.parent = parent; this.strict = strict; }
  lookup(name, pos) {
    for (let s = this; s; s = s.parent) if (Object.hasOwn(s.vars, name)) { const v = s.vars[name]; return v === undefined ? null : v; }
    if (this.strict) {
      const names = [...this.allNames()].sort();
      throw new ExprError(`Unknown field: '${name}'. Available fields: ${names.length ? names.join(", ") : "(none)"}`, pos);
    }
    return null;
  }
  allNames() { const out = new Set(); for (let s = this; s; s = s.parent) for (const k of Object.keys(s.vars)) out.add(k); return out; }
  child(vars) { return new Scope(vars, this, this.strict); }
}

export class Arg {
  constructor(node, fn, scope) { this.node = node; this.fn = fn; this.scope = scope; }
  value() { return this.fn(this.scope); }
  eval(scope) { return this.fn(scope); }
  get name() { return this.node.type === "Name" ? this.node.name : null; }
}

export function compileNode(n, env) {
  switch (n.type) {
    case "Literal": { const v = n.value; return () => v; }
    case "Name": { const { name, pos } = n; return (scope) => scope.lookup(name, pos); }
    case "Unary": {
      const operand = compileNode(n.operand, env);
      const pos = n.pos;
      if (n.op === "!") return (scope) => !truthy(operand(scope));
      return (scope) => { const v = -requireNumber(operand(scope), "Unary minus", pos); return Object.is(v, -0) ? 0 : v; };
    }
    case "Binary": return compileBinary(n, env);
    case "Call": return compileCall(n, env);
    case "Lambda": { const body = compileNode(n.body, env); const params = n.params; return (scope) => new Closure(params, body, scope); }
    case "Apply": {
      const fn = compileNode(n.fn, env), arg = compileNode(n.arg, env), pos = n.pos;
      return (scope) => { const f = fn(scope); if (!(f instanceof Closure)) throw new ExprError("Right side of |> must be a lambda", pos); return f.call(arg(scope)); };
    }
    case "Array": { const els = n.elements.map((e) => compileNode(e, env)); return (scope) => els.map((e) => e(scope)); }
    case "Map": {
      const keys = n.keys, vals = n.values.map((v) => compileNode(v, env));
      return (scope) => { const out = {}; keys.forEach((k, i) => { out[k] = vals[i](scope); }); return out; };
    }
    case "Index": return compileIndex(n, env);
    default: throw new ExprError(`Unknown node type: ${n.type}`);
  }
}

function compileBinary(n, env) {
  const { op, pos } = n;
  const left = compileNode(n.left, env), right = compileNode(n.right, env);
  switch (op) {
    case "||": return (s) => truthy(left(s)) ? true : truthy(right(s));
    case "&&": return (s) => truthy(left(s)) ? truthy(right(s)) : false;
    case "??": return (s) => { const l = left(s); return l === null || l === undefined ? right(s) : l; };
    case "==": return (s) => equals(left(s), right(s));
    case "!=": return (s) => !equals(left(s), right(s));
    case "<": return (s) => compare(left(s), right(s), op, pos) < 0;
    case "<=": return (s) => compare(left(s), right(s), op, pos) <= 0;
    case ">": return (s) => compare(left(s), right(s), op, pos) > 0;
    case ">=": return (s) => compare(left(s), right(s), op, pos) >= 0;
    case "+": return (s) => add(left(s), right(s), pos);
  }
  const numeric = (fn) => (s) => {
    const a = requireNumber(left(s), `Operator ${op}`, pos);
    const b = requireNumber(right(s), `Operator ${op}`, pos);
    const r = fn(a, b);
    return Object.is(r, -0) ? 0 : r;
  };
  switch (op) {
    case "-": return numeric((a, b) => a - b);
    case "*": return numeric((a, b) => a * b);
    case "/": return numeric((a, b) => { if (b === 0) throw new ExprError("Division by zero", pos); return a / b; });
    case "//": return numeric((a, b) => { if (b === 0) throw new ExprError("Division by zero", pos); return Math.floor(a / b); });
    case "%": return numeric((a, b) => { if (b === 0) throw new ExprError("Division by zero", pos); let r = a % b; if (r !== 0 && (r < 0) !== (b < 0)) r += b; return r; });
    case "**": return numeric((a, b) => {
      const r = a ** b;
      if (Number.isNaN(r)) throw new ExprError("Power of a negative base with fractional exponent", pos);
      if (!Number.isFinite(r)) throw new ExprError("Power error: result out of range", pos);
      return r;
    });
  }
  throw new ExprError(`Unknown operator: ${op}`, pos);
}

function compileCall(n, env) {
  const fdef = env.function(n.name);
  if (!fdef) throw new ExprError(`Unknown function: ${n.name}`, n.pos);
  const pos = n.pos;
  const argFns = n.args.map((a) => compileNode(a, env));
  const kwFns = n.kwargs.map(([k, v]) => [k, compileNode(v, env)]);
  for (const [k] of n.kwargs) {
    if (fdef.params.length && !fdef.params.includes(k)) throw new ExprError(`${fdef.name}() has no parameter '${k}'. Parameters: ${fdef.params.join(", ")}`, pos);
  }
  const { fn, name } = fdef;
  if (fdef.lazy) {
    return (scope) => {
      const ctx = { scope, env, pos };
      const args = n.args.map((a, i) => new Arg(a, argFns[i], scope));
      const kwargs = {};
      n.kwargs.forEach(([k, v], i) => { kwargs[k] = new Arg(v, kwFns[i][1], scope); });
      return fn(ctx, args, kwargs);
    };
  }
  const maxPos = fdef.variadic || !fdef.params.length ? Infinity : fdef.params.length;
  return (scope) => {
    let args = argFns.map((f) => f(scope));
    if (kwFns.length) {
      for (const [k, f] of kwFns) {
        const idx = fdef.params.indexOf(k);
        while (args.length < idx) args.push(undefined);
        args[idx] = f(scope);
      }
    }
    if (args.length > maxPos) throw new ExprError(`${name}(): takes at most ${maxPos} positional argument(s) but ${args.length} were given`, pos);
    if (fdef.required !== null && argFns.length < fdef.required) throw new ExprError(`${name}(): missing required argument(s)`, pos);
    try {
      return fn(...args);
    } catch (e) {
      if (e instanceof ExprError) { if (e.pos === null) { e.pos = pos; e.message = `${e.detail} (at position ${pos})`; } throw e; }
      throw new ExprError(`${name}(): ${e.message}`, pos);
    }
  };
}

function compileIndex(n, env) {
  const target = compileNode(n.target, env), key = compileNode(n.key, env), pos = n.pos;
  return (scope) => {
    const t = target(scope);
    let k = key(scope);
    if (isBlockedKey(k)) throw new ExprError(`Access to '${k}' is not allowed`, pos);
    if (t === null || t === undefined) return null;
    if (Array.isArray(t) || typeof t === "string") {
      if (!isNumber(k) || !Number.isInteger(k)) throw new ExprError(`Index must be an integer, got ${kind(k)}`, pos);
      const arr = typeof t === "string" ? chars(t) : t;
      const i = k < 0 ? arr.length + k : k;
      return i >= 0 && i < arr.length ? arr[i] : null;
    }
    if (isPlainObject(t)) {
      if (isNumber(k)) k = numberToString(k);
      if (typeof k !== "string") return null;
      return Object.hasOwn(t, k) ? (t[k] === undefined ? null : t[k]) : null;
    }
    throw new ExprError(`Cannot index into ${kind(t)}`, pos);
  };
}

// ---------------------------------------------------------------------------
// Compile API
// ---------------------------------------------------------------------------

export class Compiled {
  constructor(source, ast, env, fn) { this.source = source; this.ast = ast; this.env = env; this._fn = fn; }
  call(scope) { return this._fn(new Scope(scope || {}, null, this.env.strict)); }
  evaluate(scope) { return this._fn(scope); }
  get fieldRefs() { return fieldRefs(this.ast); }
}

export function compile(expr, env = defaultEnv) {
  const ast = parse(expr);
  return new Compiled(expr, ast, env, compileNode(ast, env));
}

export function compileFilter(expr, env = defaultEnv) {
  const c = compile(expr, env);
  const pred = (scope) => truthy(c.call(scope));
  pred.compiled = c;
  return pred;
}

export function compileFormatter(fields, env = defaultEnv) {
  const compiled = Object.entries(fields).map(([k, src]) => [k, compile(src, env)]);
  const fmt = (scope) => {
    const s = new Scope(scope || {}, null, env.strict);
    const out = {};
    for (const [k, c] of compiled) out[k] = c.evaluate(s);
    return out;
  };
  fmt.compiled = Object.fromEntries(compiled);
  return fmt;
}

export function evaluate(expr, scope = {}, env = defaultEnv) {
  return compile(expr, env).call(scope);
}

// ---------------------------------------------------------------------------
// Templates
// ---------------------------------------------------------------------------

export function splitTemplate(template) {
  const parts = [];
  let buf = "";
  const n = template.length;
  let i = 0;
  while (i < n) {
    if (template.startsWith("$${", i)) { buf += "${"; i += 3; continue; }
    if (template.startsWith("${", i)) {
      let j = i + 2, depth = 1, quote = null;
      while (j < n) {
        const c = template[j];
        if (quote) {
          if (c === "\\") { j += 2; continue; }
          if (c === quote) quote = null;
        } else if (c === "'" || c === '"') quote = c;
        else if (c === "{") depth++;
        else if (c === "}") { depth--; if (depth === 0) break; }
        j++;
      }
      if (j >= n) throw new ExprError("Unterminated ${...} in template", i);
      if (buf) { parts.push(["text", buf]); buf = ""; }
      parts.push(["expr", template.slice(i + 2, j)]);
      i = j + 1;
      continue;
    }
    buf += template[i];
    i++;
  }
  if (buf) parts.push(["text", buf]);
  return parts;
}

export const hasExpressions = (template) => template.includes("${");

export class CompiledTemplate {
  constructor(source, parts, env) { this.source = source; this.parts = parts; this.env = env; }
  get isPure() { return this.parts.length === 1 && this.parts[0][0] === "expr"; }
  call(scope) { return this.evaluate(new Scope(scope || {}, null, this.env.strict)); }
  /** Evaluate against an existing Scope (hosts that build scope chains). */
  evaluate(s) {
    if (this.isPure) return this.parts[0][1].evaluate(s);
    // Mixed templates concatenate as strings — unless a part is a host type
    // with a `concat` hook (rich text, say), in which case the result is
    // built through it so the host value survives. `concat(a, b)` receives
    // strings or instances on either side.
    let out = "";
    let joiner = null;
    for (const [k, p] of this.parts) {
      const v = k === "text" ? p : p.evaluate(s);
      const t = typeof v === "string" ? null : findType(v);
      if (t && t.concat) { joiner = t.concat; out = out === "" ? v : joiner(out, v); }
      else if (joiner) out = joiner(out, toString(v));
      else out += toString(v);
    }
    return out;
  }
  get fieldRefs() { const refs = new Set(); for (const [k, p] of this.parts) if (k === "expr") for (const r of p.fieldRefs) refs.add(r); return refs; }
}

export function compileTemplate(template, env = defaultEnv) {
  const parts = splitTemplate(template).map(([k, src]) => [k, k === "expr" ? compile(src, env) : src]);
  if (!parts.length) parts.push(["text", ""]);
  return new CompiledTemplate(template, parts, env);
}

// ---------------------------------------------------------------------------
// Analysis
// ---------------------------------------------------------------------------

const NUMERIC_OPS = new Set(["-", "*", "/", "//", "%", "**"]);

function walk(n, fn) {
  fn(n);
  switch (n.type) {
    case "Binary": walk(n.left, fn); walk(n.right, fn); break;
    case "Unary": walk(n.operand, fn); break;
    case "Call": n.args.forEach((a) => walk(a, fn)); n.kwargs.forEach(([, v]) => walk(v, fn)); break;
    case "Lambda": walk(n.body, fn); break;
    case "Apply": walk(n.fn, fn); walk(n.arg, fn); break;
    case "Array": n.elements.forEach((e) => walk(e, fn)); break;
    case "Map": n.values.forEach((v) => walk(v, fn)); break;
    case "Index": walk(n.target, fn); walk(n.key, fn); break;
  }
}

export function fieldRefs(n) {
  const refs = new Set();
  const rec = (m, bound) => {
    switch (m.type) {
      case "Name": if (!bound.has(m.name)) refs.add(m.name); break;
      case "Literal": break;
      case "Binary": rec(m.left, bound); rec(m.right, bound); break;
      case "Unary": rec(m.operand, bound); break;
      case "Call":
        if (m.name === "LET") {
          let inner = bound;
          const a = m.args;
          for (let i = 0; i + 1 < a.length; i += 2) { rec(a[i + 1], inner); if (a[i].type === "Name") inner = new Set([...inner, a[i].name]); }
          if (a.length) rec(a[a.length - 1], inner);
        } else m.args.forEach((x) => rec(x, bound));
        m.kwargs.forEach(([, v]) => rec(v, bound));
        break;
      case "Lambda": rec(m.body, new Set([...bound, ...m.params])); break;
      case "Apply": rec(m.fn, bound); rec(m.arg, bound); break;
      case "Array": m.elements.forEach((e) => rec(e, bound)); break;
      case "Map": m.values.forEach((v) => rec(v, bound)); break;
      case "Index": rec(m.target, bound); rec(m.key, bound); break;
    }
  };
  rec(n, new Set());
  return refs;
}

export function functionRefs(n) {
  const out = new Set();
  walk(n, (m) => { if (m.type === "Call") out.add(m.name); });
  return out;
}

export function numericFields(n, env = defaultEnv) {
  const refs = new Set();
  const rec = (m, ctx, bound) => {
    switch (m.type) {
      case "Name": if (ctx && !bound.has(m.name)) refs.add(m.name); break;
      case "Binary": { const c = ctx || NUMERIC_OPS.has(m.op); rec(m.left, c, bound); rec(m.right, c, bound); break; }
      case "Unary": rec(m.operand, ctx || m.op === "-", bound); break;
      case "Call": {
        const fdef = env.function(m.name);
        const c = ctx || !!(fdef && fdef.numeric);
        if (m.name === "LET") {
          let inner = bound;
          const a = m.args;
          for (let i = 0; i + 1 < a.length; i += 2) { rec(a[i + 1], ctx, inner); if (a[i].type === "Name") inner = new Set([...inner, a[i].name]); }
          if (a.length) rec(a[a.length - 1], ctx, inner);
        } else m.args.forEach((x) => rec(x, c, bound));
        m.kwargs.forEach(([, v]) => rec(v, false, bound));
        break;
      }
      case "Lambda": rec(m.body, ctx, new Set([...bound, ...m.params])); break;
      case "Apply": rec(m.fn, ctx, bound); rec(m.arg, ctx, bound); break;
      case "Array": m.elements.forEach((e) => rec(e, ctx, bound)); break;
      case "Map": m.values.forEach((v) => rec(v, ctx, bound)); break;
      case "Index": rec(m.target, ctx, bound); break;
    }
  };
  rec(n, false, new Set());
  return refs;
}

// ---------------------------------------------------------------------------
// Decimal helpers (rounding half away from zero on the decimal representation)
// ---------------------------------------------------------------------------

/** Expand a JS number to a plain decimal string (no exponent). */
function toDecimalString(x) {
  let s = numberToString(x);
  if (!s.includes("e")) return s;
  let [mant, exp] = s.split("e");
  let e = parseInt(exp, 10);
  const neg = mant.startsWith("-");
  if (neg) mant = mant.slice(1);
  let [ip, fp = ""] = mant.split(".");
  let digits = ip + fp;
  let point = ip.length + e;
  if (point <= 0) digits = "0".repeat(-point + 1) + digits, point = 1;
  else if (point > digits.length) digits = digits + "0".repeat(point - digits.length);
  let out = digits.slice(0, point) + (point < digits.length ? "." + digits.slice(point) : "");
  return (neg ? "-" : "") + out;
}

function incrementDigits(d) {
  const arr = d.split("");
  let i = arr.length - 1;
  while (i >= 0) {
    if (arr[i] === "9") { arr[i] = "0"; i--; }
    else { arr[i] = String(Number(arr[i]) + 1); return arr.join(""); }
  }
  return "1" + arr.join("");
}

/** Round a decimal string to `digits` places (may be negative), half away from zero. */
function roundDecimalString(s, digits) {
  const neg = s.startsWith("-");
  if (neg) s = s.slice(1);
  let [ip, fp = ""] = s.split(".");
  let all, keepLen;
  if (digits >= 0) {
    if (fp.length <= digits) return (neg ? "-" : "") + ip + (digits ? "." + fp.padEnd(digits, "0") : "");
    all = ip + fp.slice(0, digits);
    keepLen = ip.length;
    if (fp[digits] >= "5") all = incrementDigits(all);
    const intLen = all.length - digits;
    const res = digits ? all.slice(0, intLen) + "." + all.slice(intLen) : all;
    return (neg ? "-" : "") + res;
  }
  const n = -digits;
  if (ip.length <= n) {
    const first = ip.length === n ? ip[0] : "0";
    const up = ip.length === n && first >= "5";
    return (neg && up ? "-" : "") + (up ? "1" + "0".repeat(n) : "0");
  }
  let head = ip.slice(0, ip.length - n);
  if (ip[ip.length - n] >= "5") head = incrementDigits(head);
  return (neg ? "-" : "") + head + "0".repeat(n);
}

function fixed(x, digits) {
  if (!Number.isFinite(x)) return numberToString(x);
  let s = roundDecimalString(toDecimalString(x), digits);
  if (s.startsWith("-") && /^-[0.]+$/.test(s)) s = s.slice(1);
  return s;
}

export function roundDecimal(x, digits = 0) {
  requireNumber(x, "ROUND");
  digits = Math.trunc(requireNumber(digits, "ROUND digits"));
  if (!Number.isFinite(x)) return x;
  const r = Number(roundDecimalString(toDecimalString(x), digits));
  return Object.is(r, -0) ? 0 : r;
}

// ---------------------------------------------------------------------------
// core library
// ---------------------------------------------------------------------------

const argc = (ctx, args, kwargs, ok, msg) => { if (Object.keys(kwargs).length || !ok(args.length)) throw new ExprError(msg, ctx.pos); };

function numOf(x) {
  if (isNumber(x)) return x;
  if (x === true) return 1;
  if (x === false) return 0;
  if (typeof x === "string") {
    const s = x.trim().replace(/_/g, "");
    if (!s) return null;
    if (!/^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/.test(s)) return null;
    const v = Number(s);
    return Number.isFinite(v) ? v : null;
  }
  return null;
}

function get(coll, key, dflt = null) {
  if (isPlainObject(coll)) { const k = typeof key === "string" ? key : toString(key); return Object.hasOwn(coll, k) ? coll[k] : dflt; }
  if (Array.isArray(coll) && isNumber(key)) { const i = key < 0 ? coll.length + key : key; return i >= 0 && i < coll.length ? coll[i] : dflt; }
  return dflt;
}

function has(coll, key) {
  if (isPlainObject(coll)) return Object.hasOwn(coll, typeof key === "string" ? key : toString(key));
  if (Array.isArray(coll) && isNumber(key)) { const i = Math.trunc(key); return -coll.length <= i && i < coll.length; }
  return false;
}

registerLibrary("core", {
  IF: [(ctx, args, kw) => { argc(ctx, args, kw, (n) => n === 3, "IF(cond, then, else) requires exactly 3 arguments"); return truthy(args[0].value()) ? args[1].value() : args[2].value(); }, { lazy: true, params: ["cond", "then", "else"], doc: "Return `then` if `cond` is truthy, else `else`. Only the taken branch is evaluated." }],
  CASE: [(ctx, args, kw) => {
    argc(ctx, args, kw, (n) => n >= 2, "CASE(cond1, value1, ..., default?) requires at least 2 arguments");
    const odd = args.length % 2 === 1;
    const pairs = odd ? args.slice(0, -1) : args;
    for (let i = 0; i < pairs.length; i += 2) if (truthy(pairs[i].value())) return pairs[i + 1].value();
    return odd ? args[args.length - 1].value() : null;
  }, { lazy: true, doc: "CASE(c1, v1, c2, v2, ..., default?) — first truthy condition wins; NULL if none and no default." }],
  TRY: [(ctx, args, kw) => {
    argc(ctx, args, kw, (n) => n === 1 || n === 2, "TRY(expr, fallback?) requires 1 or 2 arguments");
    try { return args[0].value(); } catch (e) { if (e instanceof ExprError) return args.length === 2 ? args[1].value() : null; throw e; }
  }, { lazy: true, params: ["expr", "fallback"], doc: "Evaluate `expr`; on error return `fallback` (or NULL)." }],
  LET: [(ctx, args, kw) => {
    argc(ctx, args, kw, (n) => n >= 3 && n % 2 === 1, "LET(name, value, ..., body) requires an odd number of arguments (at least 3)");
    const scope = ctx.scope.child({});
    for (let i = 0; i + 1 < args.length; i += 2) {
      const name = args[i].name;
      if (name === null) throw new ExprError("LET binding names must be plain names", ctx.pos);
      scope.vars[name] = args[i + 1].eval(scope);
    }
    return args[args.length - 1].eval(scope);
  }, { lazy: true, doc: "LET(name, value, ..., body) — bind names in order, then evaluate `body`." }],
  COALESCE: [(...a) => { for (const x of a) if (x !== null && x !== undefined) return x; return null; }, { variadic: true, doc: "First non-NULL argument." }],
  TYPE: [kind, { params: ["x"], doc: "Kind of a value: null, boolean, number, string, array, map, function, or a host type name." }],
  STR: [toString, { params: ["x"], doc: "String form (NULL → '', TRUE → 'true', 2.0 → '2')." }],
  NUM_OF: [numOf, { params: ["x"], doc: "Parse a number from a string or boolean; NULL if not numeric." }],
  INT: [(x) => { const n = numOf(x); return n === null ? null : Math.trunc(n) || 0; }, { params: ["x"], doc: "Integer part, truncated toward zero; NULL if not numeric." }],
  BOOL: [truthy, { params: ["x"], doc: "Truthiness: NULL, FALSE, 0, '', [], {} are false." }],
  IS_NUM: [isNumber, { params: ["x"], doc: "TRUE for numbers." }],
  IS_STR: [(x) => typeof x === "string", { params: ["x"], doc: "TRUE for strings." }],
  GET: [get, { params: ["coll", "key", "default"], required: 2, doc: "Lookup in a map or array, `default` (NULL) when absent — never an error." }],
  HAS: [has, { params: ["coll", "key"], required: 2, doc: "TRUE if a map has the key or an array has the index." }],
});

// ---------------------------------------------------------------------------
// math library
// ---------------------------------------------------------------------------

function minmax(pick, name, args) {
  let vals = args.length === 1 && Array.isArray(args[0]) ? args[0] : args;
  vals = vals.filter((v) => v !== null && v !== undefined);
  if (!vals.length) return null;
  let best = vals[0];
  for (const v of vals.slice(1)) if (pick(compare(v, best, name))) best = v;
  return best;
}
const arr = (xs, name) => { if (xs === null || xs === undefined) return []; if (!Array.isArray(xs)) throw new ExprError(`${name} requires an array, got ${kind(xs)}`); return xs; };

registerLibrary("math", {
  ROUND: [roundDecimal, { numeric: true, params: ["x", "digits"], required: 1, doc: "Round half away from zero to `digits` places (default 0)." }],
  FLOOR: [(x) => Math.floor(requireNumber(x, "FLOOR")), { numeric: true, params: ["x"], required: 1, doc: "Largest integer ≤ x." }],
  CEIL: [(x) => Math.ceil(requireNumber(x, "CEIL")) || 0, { numeric: true, params: ["x"], required: 1, doc: "Smallest integer ≥ x." }],
  ABS: [(x) => Math.abs(requireNumber(x, "ABS")), { numeric: true, params: ["x"], required: 1, doc: "Absolute value." }],
  SIGN: [(x) => Math.sign(requireNumber(x, "SIGN")) || 0, { numeric: true, params: ["x"], required: 1, doc: "-1, 0, or 1." }],
  MIN: [(...a) => minmax((c) => c < 0, "MIN", a), { variadic: true, doc: "Smallest of the arguments, or of a single array; NULLs ignored." }],
  MAX: [(...a) => minmax((c) => c > 0, "MAX", a), { variadic: true, doc: "Largest of the arguments, or of a single array; NULLs ignored." }],
  SUM: [(xs) => { if (!Array.isArray(xs)) throw new ExprError(`SUM requires an array, got ${kind(xs)}`); let t = 0; for (const v of xs) if (v !== null && v !== undefined) t += requireNumber(v, "SUM element"); return t; }, { numeric: true, params: ["xs"], required: 1, doc: "Sum of an array of numbers; NULLs ignored." }],
  AVG: [(xs) => { if (!Array.isArray(xs)) throw new ExprError(`AVG requires an array, got ${kind(xs)}`); const v = xs.filter((x) => x !== null && x !== undefined).map((x) => requireNumber(x, "AVG element")); return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null; }, { numeric: true, params: ["xs"], required: 1, doc: "Mean of an array of numbers; NULL when empty." }],
  CLAMP: [(x, lo, hi) => { requireNumber(x, "CLAMP"); requireNumber(lo, "CLAMP"); requireNumber(hi, "CLAMP"); return Math.min(Math.max(x, lo), hi); }, { numeric: true, params: ["x", "lo", "hi"], required: 3, doc: "x limited to [lo, hi]." }],
  POW: [(a, b) => { requireNumber(a, "POW"); requireNumber(b, "POW"); const r = a ** b; if (Number.isNaN(r)) throw new ExprError("POW of a negative base with fractional exponent"); return r; }, { numeric: true, params: ["x", "y"], required: 2, doc: "x to the power y." }],
  SQRT: [(x) => { requireNumber(x, "SQRT"); if (x < 0) throw new ExprError("SQRT of a negative number"); return Math.sqrt(x); }, { numeric: true, params: ["x"], required: 1, doc: "Square root." }],
});

// ---------------------------------------------------------------------------
// string library
// ---------------------------------------------------------------------------

const str = (x, name) => { if (x === null || x === undefined) return null; if (typeof x !== "string") throw new ExprError(`${name} requires a string, got ${kind(x)}`); return x; };
const passthrough = (fn, name) => (x) => { const s = str(x, name); return s === null ? null : fn(s); };
const padFill = (ch, n) => { const c = chars(ch); const out = []; for (let i = 0; i < n; i++) out.push(c[i % c.length]); return out.join(""); };

registerLibrary("string", {
  UPPER: [passthrough((s) => s.toUpperCase(), "UPPER"), { params: ["s"], required: 1, doc: "Upper-case." }],
  LOWER: [passthrough((s) => s.toLowerCase(), "LOWER"), { params: ["s"], required: 1, doc: "Lower-case." }],
  TITLE: [passthrough((s) => s.split(" ").map((w) => { const c = chars(w); return c.length ? c[0].toUpperCase() + c.slice(1).join("").toLowerCase() : w; }).join(" "), "TITLE"), { params: ["s"], required: 1, doc: "Capitalise each space-separated word." }],
  TRIM: [passthrough((s) => s.trim(), "TRIM"), { params: ["s"], required: 1, doc: "Strip surrounding whitespace." }],
  PAD: [(s, width, ch = " ") => { s = toString(s); ch = str(ch, "PAD") || " "; const w = Math.trunc(width); const len = chars(s).length; return len >= w ? s : padFill(ch, w - len) + s; }, { params: ["s", "width", "ch"], required: 2, doc: "Left-pad to `width` with `ch` (default space)." }],
  PAD_END: [(s, width, ch = " ") => { s = toString(s); ch = str(ch, "PAD_END") || " "; const w = Math.trunc(width); const len = chars(s).length; return len >= w ? s : s + padFill(ch, w - len); }, { params: ["s", "width", "ch"], required: 2, doc: "Right-pad to `width` with `ch` (default space)." }],
  TRUNCATE: [(s, n, suffix = "") => { s = str(s, "TRUNCATE"); if (s === null) return null; const c = chars(s); n = Math.trunc(n); if (c.length <= n) return s; const keep = Math.max(n - chars(suffix).length, 0); return c.slice(0, keep).join("") + suffix; }, { params: ["s", "n", "suffix"], required: 2, doc: "Cut to at most `n` characters, ending with `suffix` if cut." }],
  REPLACE: [(s, o, nw) => { s = str(s, "REPLACE"); return s === null ? null : s.split(toString(o)).join(toString(nw)); }, { params: ["s", "old", "new"], required: 3, doc: "Replace every literal occurrence of `old` with `new`." }],
  SUBSTR: [(s, start, length = null) => { s = str(s, "SUBSTR"); if (s === null) return null; const c = chars(s); start = Math.trunc(start); if (start < 0) start = Math.max(c.length + start, 0); return (length === null ? c.slice(start) : c.slice(start, start + Math.max(Math.trunc(length), 0))).join(""); }, { params: ["s", "start", "length"], required: 2, doc: "Substring from `start` (negative counts from the end), optionally `length` long." }],
  SPLIT: [(s, sep) => { s = str(s, "SPLIT"); if (s === null) return null; sep = toString(sep); return sep === "" ? chars(s) : s.split(sep); }, { params: ["s", "sep"], required: 2, doc: "Split on a literal separator ('' splits into characters)." }],
  JOIN: [(xs, sep = "") => { if (xs === null || xs === undefined) return null; if (!Array.isArray(xs)) throw new ExprError(`JOIN requires an array, got ${kind(xs)}`); return xs.map(toString).join(toString(sep)); }, { params: ["xs", "sep"], required: 1, doc: "Join an array's string forms with `sep` (default '')." }],
  CONCAT: [(...a) => a.map(toString).join(""), { variadic: true, doc: "Concatenate the string forms of all arguments." }],
  LEN: [(x) => { if (x === null || x === undefined) return 0; if (typeof x === "string") return chars(x).length; if (Array.isArray(x)) return x.length; if (isPlainObject(x)) return Object.keys(x).length; throw new ExprError(`LEN requires a string, array, or map, got ${kind(x)}`); }, { params: ["x"], required: 1, doc: "Length of a string, array, or map (NULL → 0)." }],
  CONTAINS: [(hay, x) => {
    if (hay === null || hay === undefined) return false;
    if (typeof hay === "string") return hay.includes(toString(x));
    if (Array.isArray(hay)) return hay.some((v) => equals(v, x));
    if (isPlainObject(hay)) return Object.hasOwn(hay, typeof x === "string" ? x : toString(x));
    throw new ExprError(`CONTAINS requires a string, array, or map, got ${kind(hay)}`);
  }, { params: ["hay", "x"], required: 2, doc: "Substring of a string, member of an array, or key of a map." }],
  STARTS_WITH: [(s, p) => { s = str(s, "STARTS_WITH"); return s === null ? false : s.startsWith(toString(p)); }, { params: ["s", "prefix"], required: 2, doc: "TRUE if `s` starts with `prefix`." }],
  ENDS_WITH: [(s, p) => { s = str(s, "ENDS_WITH"); return s === null ? false : s.endsWith(toString(p)); }, { params: ["s", "suffix"], required: 2, doc: "TRUE if `s` ends with `suffix`." }],
  MATCHES: [(s, pattern) => { s = str(s, "MATCHES"); if (s === null) return false; let re; try { re = new RegExp(toString(pattern), "u"); } catch (e) { throw new ExprError(`MATCHES: bad pattern: ${e.message}`); } return re.test(s); }, { params: ["s", "pattern"], required: 2, doc: "TRUE if the regular expression matches anywhere in `s`." }],
});

// ---------------------------------------------------------------------------
// format library
// ---------------------------------------------------------------------------

function group(ip) {
  const neg = ip.startsWith("-");
  let d = neg ? ip.slice(1) : ip;
  const out = [];
  while (d.length > 3) { out.unshift(d.slice(-3)); d = d.slice(0, -3); }
  out.unshift(d);
  return (neg ? "-" : "") + out.join(",");
}

export function num(x, digits = null, grp = false) {
  if (x === null || x === undefined) return "";
  requireNumber(x, "NUM");
  let s = digits === null || digits === undefined ? numberToString(x) : fixed(x, Math.trunc(requireNumber(digits, "NUM digits")));
  if (grp && !s.includes("e")) { const [ip, fp] = s.split("."); s = group(ip) + (fp ? "." + fp : ""); }
  return s;
}

const BYTE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];

registerLibrary("format", {
  NUM: [num, { params: ["x", "digits", "group"], required: 1, doc: "Number to string with fixed `digits` (shortest form when omitted) and optional thousands `group`." }],
  PCT: [(x, digits = 0) => { if (x === null || x === undefined) return ""; requireNumber(x, "PCT"); return fixed(x * 100, Math.trunc(digits)) + "%"; }, { params: ["x", "digits"], required: 1, doc: "Fraction to percentage string: 0.125 → '12.5%' with digits: 1." }],
  SCI: [(x, digits = 2) => {
    if (x === null || x === undefined) return "";
    requireNumber(x, "SCI");
    digits = Math.trunc(digits);
    if (x === 0) return fixed(0, digits) + "e+0";
    if (!Number.isFinite(x)) return numberToString(x);
    let exp = Math.floor(Math.log10(Math.abs(x)));
    let mant = x / 10 ** exp;
    let m = fixed(mant, digits);
    if (Math.abs(Number(m)) >= 10) { exp += 1; m = fixed(mant / 10, digits); }
    return `${m}e${exp >= 0 ? "+" : "-"}${Math.abs(exp)}`;
  }, { params: ["x", "digits"], required: 1, doc: "Scientific notation: 123456 → '1.23e+5'." }],
  BYTES: [(n, digits = 1) => {
    if (n === null || n === undefined) return "";
    requireNumber(n, "BYTES");
    let v = Math.abs(n), i = 0;
    while (v >= 1024 && i < BYTE_UNITS.length - 1) { v /= 1024; i++; }
    return (n < 0 ? "-" : "") + fixed(v, i === 0 ? 0 : Math.trunc(digits)) + " " + BYTE_UNITS[i];
  }, { params: ["n", "digits"], required: 1, doc: "Byte count to '1.5 KB' (1024-based)." }],
  DURATION: [(seconds, digits = 0) => {
    if (seconds === null || seconds === undefined) return "";
    requireNumber(seconds, "DURATION");
    const neg = seconds < 0;
    let s = Math.abs(seconds);
    const d = Math.floor(s / 86400); s -= d * 86400;
    const h = Math.floor(s / 3600); s -= h * 3600;
    const m = Math.floor(s / 60); s -= m * 60;
    const parts = [];
    if (d) parts.push(`${d}d`);
    if (h || parts.length) parts.push(`${h}h`);
    if (m || parts.length) parts.push(`${m}m`);
    parts.push(fixed(s, Math.trunc(digits)) + "s");
    return (neg ? "-" : "") + parts.join(" ");
  }, { params: ["seconds", "digits"], required: 1, doc: "Seconds to '1d 2h 3m 4s'." }],
  FORMAT: [(pattern, ...args) => {
    pattern = toString(pattern);
    let out = "", i = 0, auto = 0;
    const n = pattern.length;
    while (i < n) {
      const c = pattern[i];
      if (c === "{") {
        if (pattern.startsWith("{{", i)) { out += "{"; i += 2; continue; }
        const end = pattern.indexOf("}", i);
        if (end < 0) throw new ExprError("FORMAT: unmatched '{'");
        const spec = pattern.slice(i + 1, end).trim();
        let idx;
        if (spec === "") idx = auto++;
        else if (/^\d+$/.test(spec)) idx = parseInt(spec, 10);
        else throw new ExprError(`FORMAT: bad placeholder {${spec}}`);
        if (idx >= args.length) throw new ExprError(`FORMAT: placeholder {${spec}} has no argument`);
        out += toString(args[idx]);
        i = end + 1;
        continue;
      }
      if (c === "}") { if (pattern.startsWith("}}", i)) { out += "}"; i += 2; continue; } throw new ExprError("FORMAT: unmatched '}'"); }
      out += c;
      i++;
    }
    return out;
  }, { variadic: true, doc: "Fill '{}' / '{0}' placeholders in a pattern with the remaining arguments." }],
});

// ---------------------------------------------------------------------------
// time library (seconds since the epoch)
// ---------------------------------------------------------------------------

const REF_RE = /^(\d{4})(\d{2})(\d{2}) (\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,12}))?$/;
const ISO_RE = /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?)?(Z|[+-]\d{2}:?\d{2})?$/;
const OFFSET_RE = /^([+-])(\d{2}):?(\d{2})$/;

/** Offset in minutes for a tz spec; null means local time. */
function tzOffset(spec) {
  if (spec === null || spec === undefined || spec === "UTC" || spec === "utc") return 0;
  if (spec === "local") return null;
  const m = OFFSET_RE.exec(String(spec));
  if (!m) throw new ExprError(`Unknown time zone '${spec}': use 'UTC', 'local', or '+HH:MM'`);
  return (m[1] === "+" ? 1 : -1) * (parseInt(m[2], 10) * 60 + parseInt(m[3], 10));
}

const fracSeconds = (frac) => parseInt(frac.slice(0, 9).padEnd(9, "0"), 10) / 1e9;

export function epoch(x) {
  if (x === null || x === undefined) return null;
  if (isNumber(x)) return x;
  if (typeof x !== "string") throw new ExprError(`EPOCH requires a number or string, got ${kind(x)}`);
  const s = x.trim();
  let m = REF_RE.exec(s);
  if (m) {
    const base = Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]) / 1000;
    return m[7] ? base + fracSeconds(m[7]) : base;
  }
  m = ISO_RE.exec(s);
  if (m) {
    const off = m[8] === undefined || m[8] === "Z" ? 0 : tzOffset(m[8]);
    const base = Date.UTC(+m[1], +m[2] - 1, +m[3], +(m[4] || 0), +(m[5] || 0), +(m[6] || 0)) / 1000 - off * 60;
    return m[7] ? base + fracSeconds(m[7]) : base;
  }
  throw new ExprError(`EPOCH: cannot parse '${x}' as a ref or ISO-8601 time`);
}

/** Broken-down time fields for `secs` in the given zone. */
function fields(secs, tz) {
  const off = tzOffset(tz);
  const whole = Math.floor(secs);
  let micro = Math.round((secs - whole) * 1e6);
  let base = whole;
  if (micro >= 1e6) { micro -= 1e6; base += 1; }
  if (off === null) {
    const d = new Date(base * 1000);
    return { Y: d.getFullYear(), m: d.getMonth() + 1, d: d.getDate(), H: d.getHours(), M: d.getMinutes(), S: d.getSeconds(), f: micro, z: -d.getTimezoneOffset() };
  }
  const d = new Date((base + off * 60) * 1000);
  return { Y: d.getUTCFullYear(), m: d.getUTCMonth() + 1, d: d.getUTCDate(), H: d.getUTCHours(), M: d.getUTCMinutes(), S: d.getUTCSeconds(), f: micro, z: off };
}

const p2 = (n) => String(n).padStart(2, "0");

function strftime(f, fmt) {
  let out = "", i = 0;
  while (i < fmt.length) {
    const c = fmt[i];
    if (c !== "%") { out += c; i++; continue; }
    if (i + 1 >= fmt.length) throw new ExprError("Bad time format: trailing '%'");
    const t = fmt[i + 1];
    i += 2;
    switch (t) {
      case "Y": out += String(f.Y).padStart(4, "0"); break;
      case "m": out += p2(f.m); break;
      case "d": out += p2(f.d); break;
      case "H": out += p2(f.H); break;
      case "M": out += p2(f.M); break;
      case "S": out += p2(f.S); break;
      case "f": out += String(f.f).padStart(6, "0"); break;
      case "z": { const z = f.z; const sign = z >= 0 ? "+" : "-"; const a = Math.abs(z); out += `${sign}${p2(Math.floor(a / 60))}${p2(a % 60)}`; break; }
      case "%": out += "%"; break;
      default: throw new ExprError(`Bad time format token: %${t}`);
    }
  }
  return out;
}

function formatTime(ts, fmt, tz) {
  const secs = epoch(ts);
  if (secs === null) return "";
  requireNumber(secs, "time");
  return strftime(fields(secs, tz), String(fmt));
}

registerLibrary("time", {
  NOW: [() => Date.now() / 1000, { doc: "Current time in seconds since the epoch." }],
  EPOCH: [epoch, { params: ["x"], required: 1, doc: "Seconds since the epoch from a number, mkio ref string, or ISO-8601 string." }],
  DATE: [(ts, fmt = "%Y-%m-%d", tz = "UTC") => formatTime(ts, fmt, tz), { params: ["ts", "fmt", "tz"], required: 1, doc: "Format a time as a date (default '%Y-%m-%d', UTC)." }],
  TIME: [(ts, fmt = "%H:%M:%S", tz = "UTC") => formatTime(ts, fmt, tz), { params: ["ts", "fmt", "tz"], required: 1, doc: "Format a time as a clock time (default '%H:%M:%S', UTC)." }],
  REF_TIME: [(ref, fmt = "%Y-%m-%d %H:%M:%S", tz = "UTC") => formatTime(ref, fmt, tz), { params: ["ref", "fmt", "tz"], required: 1, doc: "Format an mkio ref (default '%Y-%m-%d %H:%M:%S', UTC)." }],
});

// ---------------------------------------------------------------------------
// collection library
// ---------------------------------------------------------------------------

const fnArg = (f, name) => { if (!isCallable(f)) throw new ExprError(`${name} requires a lambda, got ${kind(f)}`); return f instanceof Closure ? (...a) => f.call(...a) : f; };
const mapArg = (m, name) => { if (m === null || m === undefined) return {}; if (!isPlainObject(m)) throw new ExprError(`${name} requires a map, got ${kind(m)}`); return m; };

registerLibrary("collection", {
  MAP: [(xs, f) => { f = fnArg(f, "MAP"); return arr(xs, "MAP").map((x) => f(x)); }, { params: ["xs", "fn"], required: 2, doc: "Apply `fn` to each element." }],
  FILTER: [(xs, f) => { f = fnArg(f, "FILTER"); return arr(xs, "FILTER").filter((x) => truthy(f(x))); }, { params: ["xs", "fn"], required: 2, doc: "Elements for which `fn` is truthy." }],
  ANY: [(xs, f = null) => { xs = arr(xs, "ANY"); if (f === null) return xs.some(truthy); f = fnArg(f, "ANY"); return xs.some((x) => truthy(f(x))); }, { params: ["xs", "fn"], required: 1, doc: "TRUE if `fn` (or the element) is truthy for any element." }],
  ALL: [(xs, f = null) => { xs = arr(xs, "ALL"); if (f === null) return xs.every(truthy); f = fnArg(f, "ALL"); return xs.every((x) => truthy(f(x))); }, { params: ["xs", "fn"], required: 1, doc: "TRUE if `fn` (or the element) is truthy for every element (TRUE for empty)." }],
  FIND: [(xs, f) => { f = fnArg(f, "FIND"); for (const x of arr(xs, "FIND")) if (truthy(f(x))) return x; return null; }, { params: ["xs", "fn"], required: 2, doc: "First element for which `fn` is truthy, else NULL." }],
  FIRST: [(xs) => { xs = arr(xs, "FIRST"); return xs.length ? xs[0] : null; }, { params: ["xs"], required: 1, doc: "First element, or NULL." }],
  LAST: [(xs) => { xs = arr(xs, "LAST"); return xs.length ? xs[xs.length - 1] : null; }, { params: ["xs"], required: 1, doc: "Last element, or NULL." }],
  RANGE: [(a, b = null, step = 1) => {
    if (b === null) { b = a; a = 0; }
    for (const v of [a, b, step]) if (!isNumber(v) || !Number.isInteger(v)) throw new ExprError("RANGE requires integers");
    if (step === 0) throw new ExprError("RANGE step must not be zero");
    const out = [];
    if (step > 0) for (let i = a; i < b; i += step) out.push(i);
    else for (let i = a; i > b; i += step) out.push(i);
    return out;
  }, { params: ["a", "b", "step"], required: 1, doc: "RANGE(n) → [0..n), RANGE(a, b) → [a..b), optional step." }],
  SORT_BY: [(xs, f = null, desc = false) => {
    xs = [...arr(xs, "SORT_BY")];
    const key = f === null ? (x) => x : fnArg(f, "SORT_BY");
    const keyed = xs.map((x, i) => [key(x), x, i]);
    const d = truthy(desc);
    keyed.sort((a, b) => {
      const ka = a[0], kb = b[0];
      const na = ka === null || ka === undefined, nb = kb === null || kb === undefined;
      if (na && nb) return a[2] - b[2];
      if (na) return 1;
      if (nb) return -1;
      const c = compare(ka, kb, "SORT_BY");
      return c === 0 ? a[2] - b[2] : (d ? -c : c);
    });
    return keyed.map((p) => p[1]);
  }, { params: ["xs", "fn", "desc"], required: 1, doc: "Stable sort by `fn(x)` (or the element); NULL keys last; `desc: TRUE` reverses." }],
  REDUCE: [(xs, f, init = null) => { f = fnArg(f, "REDUCE"); let acc = init; for (const x of arr(xs, "REDUCE")) acc = f(acc, x); return acc; }, { params: ["xs", "fn", "init"], required: 2, doc: "Fold with `fn(acc, x)` starting from `init`." }],
  KEYS: [(m) => Object.keys(mapArg(m, "KEYS")), { params: ["m"], required: 1, doc: "Keys of a map." }],
  VALUES: [(m) => Object.values(mapArg(m, "VALUES")), { params: ["m"], required: 1, doc: "Values of a map." }],
  FLATTEN: [(xs) => { const out = []; for (const x of arr(xs, "FLATTEN")) Array.isArray(x) ? out.push(...x) : out.push(x); return out; }, { params: ["xs"], required: 1, doc: "Flatten one level of nesting." }],
  MERGE: [(...maps) => { const out = {}; for (const m of maps) { if (m === null || m === undefined) continue; if (!isPlainObject(m)) throw new ExprError(`MERGE requires maps, got ${kind(m)}`); Object.assign(out, m); } return out; }, { variadic: true, doc: "Merge maps left to right (later keys win)." }],
});

// ---------------------------------------------------------------------------
// Export for classic scripts
// ---------------------------------------------------------------------------

const api = {
  LANGUAGE_VERSION, ExprError, Env, defaultEnv, Compiled, CompiledTemplate,
  compile, compileFilter, compileFormatter, compileTemplate, evaluate, parse, tokenize,
  registerFunction, registerLibrary, registerType, unregisterLibrary, LIBRARIES, TYPES, findType,
  fieldRefs, functionRefs, numericFields, Scope, Closure, Arg, compileNode,
  toString, truthy, equals, kind, splitTemplate, hasExpressions, roundDecimal, num, epoch,
};
if (typeof globalThis !== "undefined") globalThis.mkioExpr = api;
export default api;
