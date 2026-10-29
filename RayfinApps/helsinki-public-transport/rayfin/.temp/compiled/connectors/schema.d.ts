import type { FabricSemanticModel } from '@microsoft/rayfin-connector-fabric-semanticmodel';
/**
 * Connector instances declared in `rayfin/rayfin.yml`.
 *
 * `transitModel` points at the transit semantic model, which is a DirectQuery
 * model over the `HSL_EH` Eventhouse. The workspace and item ids live in the
 * YAML only - the browser never sees them.
 */
export type AppConnectorsSchema = {
    transitModel: FabricSemanticModel<'executeQuery'>;
};
