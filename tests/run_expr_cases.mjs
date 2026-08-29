// Run tests/expr_cases.json against client/mkio-expr.mjs. Prints one JSON line:
// {"total": N, "failures": [{id, expr, message}]}. Invoked by test_expr_js.py.
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import * as X from "../src/mkio/client/mkio-expr.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const { cases, language } = JSON.parse(readFileSync(join(here, "expr_cases.json"), "utf8"));
const failures = [];
const fail = (c, message) => failures.push({ id: c.id, expr: c.expr ?? c.template, message });

if (language !== X.LANGUAGE_VERSION) failures.push({ id: "language", expr: "", message: `fixture language ${language} != ${X.LANGUAGE_VERSION}` });

for (const c of cases) {
  const env = new X.Env({ strict: c.strict !== undefined ? c.strict : true });
  let got;
  try {
    got = "template" in c ? X.compileTemplate(c.template, env).call(c.scope) : X.compile(c.expr, env).call(c.scope);
  } catch (e) {
    if (!(e instanceof X.ExprError)) { fail(c, `CRASH ${e.constructor.name}: ${e.message}\n${e.stack}`); continue; }
    if (!("error" in c)) fail(c, `unexpected error: ${e.message}`);
    else if (!e.message.includes(c.error)) fail(c, `wrong error: got '${e.message}' want '${c.error}'`);
    continue;
  }
  if ("error" in c) { fail(c, `expected error '${c.error}', got ${JSON.stringify(got)}`); continue; }
  if (!X.equals(got, c.expect) || X.kind(got) !== X.kind(c.expect)) fail(c, `got ${JSON.stringify(got)} (${X.kind(got)}) want ${JSON.stringify(c.expect)}`);
}

// -- JS API checks (mirror of the Python API tests) --------------------------
const api = [];
const check = (name, fn) => { try { const r = fn(); if (r !== true) api.push(`${name}: got ${JSON.stringify(r)}`); } catch (e) { api.push(`${name}: ${e.message}`); } };
const expectError = (fn, sub) => { try { fn(); return `no error (want ${sub})`; } catch (e) { return e.message.includes(sub) ? true : `wrong error: ${e.message}`; } };

check("registerFunction", () => { X.registerFunction("MASK_PAN", (s) => "****" + s.slice(-4), { library: "scratch" }); const r = X.evaluate("mask_pan(pan)", { pan: "1234567890" }) === "****7890"; X.unregisterLibrary("scratch"); return r; });
check("reserved name", () => expectError(() => X.registerFunction("null", () => 1), "reserved"));
check("collision", () => { const r = expectError(() => X.registerFunction("UPPER", (s) => s, { library: "scratch" }), "already registered"); X.unregisterLibrary("scratch"); return r; });
check("library metadata", () => {
  X.registerLibrary("scratch", { TWICE: [(x) => x * 2, { numeric: true, params: ["x"], doc: "Double it" }], HELLO: () => "hi" });
  const ok = X.evaluate("TWICE(a)", { a: 2 }) === 4 && X.evaluate("HELLO()") === "hi" && [...X.numericFields(X.parse("TWICE(a) + b"))].join() === "a" && X.defaultEnv.function("twice").doc === "Double it";
  X.unregisterLibrary("scratch");
  return ok;
});
check("named args validated", () => { X.registerFunction("F", (x, y = 0) => x + y, { params: ["x", "y"], library: "scratch" }); const ok = X.evaluate("F(1, y: 2)") === 3 && expectError(() => X.compile("F(1, z: 2)"), "no parameter 'z'") === true; X.unregisterLibrary("scratch"); return ok; });
check("env restricts libraries", () => expectError(() => X.compile("UPPER('a')", new X.Env({ libraries: ["core"] })), "Unknown function: UPPER"));
check("strict lists fields", () => expectError(() => X.compile("x").call({ a: 1, b: 2 }), "Unknown field: 'x'. Available fields: a, b"));
check("lenient", () => X.compile("x ?? 'd'", new X.Env({ strict: false })).call({}) === "d");
check("lazy function", () => {
  const calls = [];
  X.registerFunction("WHEN", (ctx, [cond, then]) => { calls.push(cond.name); return X.truthy(cond.value()) ? then.value() : null; }, { lazy: true, library: "scratch" });
  const ok = X.evaluate("WHEN(a, 1 / a)", { a: 2 }) === 0.5 && X.evaluate("WHEN(a, 1 / a)", { a: 0 }) === null && calls.join() === "a,a";
  X.unregisterLibrary("scratch");
  return ok;
});
check("lazy binds names", () => { X.registerFunction("WITH", (ctx, [name, value, body]) => body.eval(ctx.scope.child({ [name.name]: value.value() })), { lazy: true, library: "scratch" }); const ok = X.evaluate("WITH(x, a * 2, x + 1)", { a: 5 }) === 11; X.unregisterLibrary("scratch"); return ok; });
check("register type", () => {
  class Dec { constructor(v) { this.v = v; } }
  X.registerType("decimal", (v) => v instanceof Dec, { add: (a, b) => new Dec(a.v + b.v), toString: (v) => v.v.toFixed(2), truthy: (v) => v.v !== 0, compare: (a, b) => Math.sign(a.v - b.v) });
  const scope = { a: new Dec(1.1), b: new Dec(2.2), z: new Dec(0) };
  const ok = X.evaluate("a + b", scope).v === 1.1 + 2.2 && X.evaluate("a < b && !z", scope) === true && X.evaluate("TYPE(a)", scope) === "decimal" && X.compileTemplate("${a}!").call(scope) === "1.10!" && X.evaluate("BOOL(z)", scope) === false;
  X.TYPES.delete("decimal");
  return ok;
});
check("register type concat", () => {
  class Tag { constructor(parts) { this.parts = parts; } }
  const parts = (x) => (x instanceof Tag ? x.parts : [x]);
  X.registerType("tag", (v) => v instanceof Tag, { toString: (t) => t.parts.join(""), concat: (a, b) => new Tag([...parts(a), ...parts(b)]) });
  const out = X.compileTemplate("<${x}> and ${y}").call({ x: new Tag(["a"]), y: 2 });
  const ok = out instanceof Tag && out.parts.join("|") === "<|a|> and |2";
  X.TYPES.delete("tag");
  return ok;
});
check("closure callable", () => { const f = X.evaluate("(a, b) -> a * b + c", { c: 1 }); return f.call(2, 3) === 7 && expectError(() => f.call(1), "expects 2") === true; });
check("compileFilter", () => { const p = X.compileFilter("qty > 50 && status == 'pending'"); return p({ qty: 100, status: "pending" }) === true && p({ qty: 10, status: "pending" }) === false && X.compileFilter("tags")({ tags: [] }) === false; });
check("compileFormatter", () => { const f = X.compileFormatter({ total: "qty * price", ticker: "UPPER(symbol)", fee: "qty * price |> (s -> ROUND(s * 0.001, 2))" }); return X.equals(f({ qty: 10, price: 2.5, symbol: "aapl" }), { total: 25, ticker: "AAPL", fee: 0.03 }); });
check("fieldRefs", () => [...X.compile("qty * price + LET(x, 1, x)").fieldRefs].sort().join() === "price,qty" && [...X.fieldRefs(X.parse("MAP(items, i -> i.qty + z)"))].sort().join() === "items,z" && [...X.fieldRefs(X.parse("LET(s, s, s)"))].join() === "s" && [...X.fieldRefs(X.parse("NUM(a, digits: d)"))].sort().join() === "a,d");
check("template attrs", () => { const t = X.compileTemplate("Order ${id}: ${NUM(qty * price, digits: 2)}"); return [...t.fieldRefs].sort().join() === "id,price,qty" && !t.isPure && X.compileTemplate("${a}").isPure && X.hasExpressions("a ${b}") && !X.hasExpressions("plain"); });
check("functionRefs", () => [...X.functionRefs(X.parse("IF(a, upper(b), Round(c, 2)) |> (x -> LEN(x))"))].sort().join() === "IF,LEN,ROUND,UPPER");
check("numericFields", () => [...X.numericFields(X.parse("SUM(xs) // d ** e % f"))].sort().join() === "d,e,f,xs" && X.numericFields(X.parse("a + b")).size === 0 && [...X.numericFields(X.parse("LET(x, a * 2, x + b)"))].join() === "a" && X.numericFields(X.parse("MAP(xs, i -> i * 2)")).size === 0);
check("unknown fn at compile", () => expectError(() => X.compile("IF(TRUE, 1, NOPE())"), "Unknown function: NOPE"));
check("error positions", () => { try { X.compile("1 + 'a'").call({}); return "no error"; } catch (e) { return e.pos === 2 && e.message.includes("(at position 2)"); } });
check("arity errors", () => expectError(() => X.evaluate("UPPER('a', 'b')"), "UPPER()") === true && expectError(() => X.evaluate("NUM(1, 2, 3, 4)"), "NUM()") === true && expectError(() => X.evaluate("ROUND()"), "ROUND()") === true);
check("NOW", () => Math.abs(X.evaluate("NOW()") - Date.now() / 1000) < 5);
check("globalThis export", () => globalThis.mkioExpr.compile === X.compile);

for (const m of api) failures.push({ id: "api", expr: "", message: m });
console.log(JSON.stringify({ total: cases.length, failures }));
process.exit(failures.length ? 1 : 0);
