/**
 * An operator note pinned to one vehicle.
 *
 * This is the part of the app that is not a live read of the Eventhouse: the GTFS-RT feed says
 * where a bus is, but only a person can say *why* it has been sitting at Kamppi for nine minutes.
 * Comments make the map somewhere a duty manager can leave that context for the next shift.
 *
 * Access is deliberately asymmetric. Every signed-in user reads every comment - a note only the
 * author can see is worthless to a control room - but a comment can only be edited or deleted by
 * the person who wrote it, enforced server-side by matching the JWT `sub` claim against
 * `user_id` rather than by hiding buttons in the UI.
 */
export declare class VehicleComment {
    id: string;
    /** GTFS-RT vehicle id the note belongs to, e.g. `90/1044`. */
    vehicle_id: string;
    /** Route at the time of writing, kept so an old note still reads sensibly. */
    route?: string;
    body: string;
    /** Display name, captured at write time so the list needs no second lookup. */
    author: string;
    createdAt: Date;
    /** Owner association, populated from the JWT `sub` claim. */
    user_id: string;
}
