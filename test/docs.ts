/**
 * Helpers of the doc tests: take a `ts` block from README.md or CHANGELOG.md as it stands and
 * type-check it with the repository's compiler options.
 */
import { readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import ts from "typescript";
import { expect } from "vitest";

const root = resolve(__dirname, "..");

/** The first ```ts block after `heading` in `file`, without the indentation of a list item. */
export function snippet(file: string, heading: string): string {
  const text = readFileSync(join(root, file), "utf8");
  const from = text.indexOf(heading);
  expect(from, `${heading} in ${file}`).toBeGreaterThanOrEqual(0);
  const match = /^([ \t]*)```ts\n([\s\S]*?)^\1```/m.exec(text.slice(from));
  expect(match, `a ts block after ${heading} in ${file}`).not.toBeNull();
  const indent = match![1]!;
  return match![2]!
    .split("\n")
    .map((line) => (line.startsWith(indent) ? line.slice(indent.length) : line))
    .join("\n");
}

/** Type-check virtual files (placed under test/, so the tsconfig paths resolve) and return their diagnostics. */
export function typecheck(files: Record<string, string>): string[] {
  const config = ts.readConfigFile(join(root, "tsconfig.json"), ts.sys.readFile);
  const parsed = ts.parseJsonConfigFileContent(config.config, ts.sys, root);
  const virtual = new Map(Object.entries(files).map(([name, text]) => [join(root, "test", name), text]));
  const host = ts.createCompilerHost(parsed.options);
  const getSourceFile = host.getSourceFile.bind(host);
  host.getSourceFile = (name, version, ...rest) => {
    const text = virtual.get(name);
    return text !== undefined ? ts.createSourceFile(name, text, version) : getSourceFile(name, version, ...rest);
  };
  const fileExists = host.fileExists.bind(host);
  host.fileExists = (name) => virtual.has(name) || fileExists(name);
  const program = ts.createProgram([...virtual.keys()], parsed.options, host);
  return [...virtual.keys()].flatMap((name) =>
    ts.getPreEmitDiagnostics(program, program.getSourceFile(name)).map((d) => {
      const where = d.file && d.start !== undefined ? d.file.getLineAndCharacterOfPosition(d.start) : undefined;
      return `${name.slice(root.length + 1)}${where ? `:${where.line + 1}` : ""} TS${d.code}: ${ts.flattenDiagnosticMessageText(d.messageText, "\n")}`;
    }),
  );
}
