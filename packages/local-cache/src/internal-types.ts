export interface IDBRequest<T = unknown> extends EventTarget {
  result: T;
  error: DOMException | null;
  onsuccess: ((event: Event) => void) | null;
  onerror: ((event: Event) => void) | null;
}

export interface IDBPDatabase {
  transaction(storeNames: string, mode: IDBTransactionMode): IDBTransaction;
  close(): void;
}
