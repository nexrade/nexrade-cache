declare module "../wasm/nexrade_wasm.js" {
  const init: () => Promise<unknown>;
  export default init;

  export class NexradeWasm {
    constructor();
    command(args: unknown[]): Promise<unknown>;
    execute(command: string): Promise<string>;
    dbsize(): number;
    flushall(): void;
  }
}
