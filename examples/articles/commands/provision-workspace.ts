import { es } from "@orgops/eventstore";
import { workspace } from "../events.js";

/**
 * A workspace is its own tenant: the root event is appended into the tenant named by its own id.
 * Slug uniqueness across ALL workspaces is the `unique: ["slug"]` index — no cross-tenant read needed.
 */
export async function provisionWorkspace(input: { slug: string; name: string }) {
  const provisioned = workspace.WorkspaceProvisioned({ slug: input.slug, name: input.name });
  return es.forTenant(provisioned.id).command<readonly unknown[], string>({
    context: workspace.$scope("workspaceProvisionedId", provisioned.id), // create-if-absent: version 0
    decide: () => ({ events: [provisioned], result: provisioned.id }),
  });
}
