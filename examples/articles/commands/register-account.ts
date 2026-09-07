import { es } from "@orgops/eventstore";
import { accounts } from "../events.js";

/**
 * Accounts live in the platform tenant. The email's uniqueness is the index's job: a duplicate
 * surfaces as `UniqueViolationError` (409) after the guard passed — `es.command()` does not
 * retry it. (Fold the context and check in `decide` if you want a friendlier rejection.)
 */
export async function registerAccount(input: { email: string; displayName: string }) {
  const registered = accounts.AccountRegistered({ email: input.email, displayName: input.displayName });
  return es.forPlatform().command({
    decide: () => ({ events: [registered], result: registered.id }),
  });
}
