var __esDecorate = (this && this.__esDecorate) || function (ctor, descriptorIn, decorators, contextIn, initializers, extraInitializers) {
    function accept(f) { if (f !== void 0 && typeof f !== "function") throw new TypeError("Function expected"); return f; }
    var kind = contextIn.kind, key = kind === "getter" ? "get" : kind === "setter" ? "set" : "value";
    var target = !descriptorIn && ctor ? contextIn["static"] ? ctor : ctor.prototype : null;
    var descriptor = descriptorIn || (target ? Object.getOwnPropertyDescriptor(target, contextIn.name) : {});
    var _, done = false;
    for (var i = decorators.length - 1; i >= 0; i--) {
        var context = {};
        for (var p in contextIn) context[p] = p === "access" ? {} : contextIn[p];
        for (var p in contextIn.access) context.access[p] = contextIn.access[p];
        context.addInitializer = function (f) { if (done) throw new TypeError("Cannot add initializers after decoration has completed"); extraInitializers.push(accept(f || null)); };
        var result = (0, decorators[i])(kind === "accessor" ? { get: descriptor.get, set: descriptor.set } : descriptor[key], context);
        if (kind === "accessor") {
            if (result === void 0) continue;
            if (result === null || typeof result !== "object") throw new TypeError("Object expected");
            if (_ = accept(result.get)) descriptor.get = _;
            if (_ = accept(result.set)) descriptor.set = _;
            if (_ = accept(result.init)) initializers.unshift(_);
        }
        else if (_ = accept(result)) {
            if (kind === "field") initializers.unshift(_);
            else descriptor[key] = _;
        }
    }
    if (target) Object.defineProperty(target, contextIn.name, descriptor);
    done = true;
};
var __runInitializers = (this && this.__runInitializers) || function (thisArg, initializers, value) {
    var useValue = arguments.length > 2;
    for (var i = 0; i < initializers.length; i++) {
        value = useValue ? initializers[i].call(thisArg, value) : initializers[i].call(thisArg);
    }
    return useValue ? value : void 0;
};
import { date, entity, role, text, uuid } from '@microsoft/rayfin-core';
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
let VehicleComment = (() => {
    let _classDecorators = [entity(), role('authenticated', ['read', 'create']), role('authenticated', ['update', 'delete'], {
            policy: (claims, item) => claims.sub.eq(item.user_id),
        })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _id_decorators;
    let _id_initializers = [];
    let _id_extraInitializers = [];
    let _vehicle_id_decorators;
    let _vehicle_id_initializers = [];
    let _vehicle_id_extraInitializers = [];
    let _route_decorators;
    let _route_initializers = [];
    let _route_extraInitializers = [];
    let _body_decorators;
    let _body_initializers = [];
    let _body_extraInitializers = [];
    let _author_decorators;
    let _author_initializers = [];
    let _author_extraInitializers = [];
    let _createdAt_decorators;
    let _createdAt_initializers = [];
    let _createdAt_extraInitializers = [];
    let _user_id_decorators;
    let _user_id_initializers = [];
    let _user_id_extraInitializers = [];
    var VehicleComment = class {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(null) : void 0;
            _id_decorators = [uuid()];
            _vehicle_id_decorators = [text()];
            _route_decorators = [text({ optional: true })];
            _body_decorators = [text()];
            _author_decorators = [text()];
            _createdAt_decorators = [date()];
            _user_id_decorators = [text()];
            __esDecorate(null, null, _id_decorators, { kind: "field", name: "id", static: false, private: false, access: { has: obj => "id" in obj, get: obj => obj.id, set: (obj, value) => { obj.id = value; } }, metadata: _metadata }, _id_initializers, _id_extraInitializers);
            __esDecorate(null, null, _vehicle_id_decorators, { kind: "field", name: "vehicle_id", static: false, private: false, access: { has: obj => "vehicle_id" in obj, get: obj => obj.vehicle_id, set: (obj, value) => { obj.vehicle_id = value; } }, metadata: _metadata }, _vehicle_id_initializers, _vehicle_id_extraInitializers);
            __esDecorate(null, null, _route_decorators, { kind: "field", name: "route", static: false, private: false, access: { has: obj => "route" in obj, get: obj => obj.route, set: (obj, value) => { obj.route = value; } }, metadata: _metadata }, _route_initializers, _route_extraInitializers);
            __esDecorate(null, null, _body_decorators, { kind: "field", name: "body", static: false, private: false, access: { has: obj => "body" in obj, get: obj => obj.body, set: (obj, value) => { obj.body = value; } }, metadata: _metadata }, _body_initializers, _body_extraInitializers);
            __esDecorate(null, null, _author_decorators, { kind: "field", name: "author", static: false, private: false, access: { has: obj => "author" in obj, get: obj => obj.author, set: (obj, value) => { obj.author = value; } }, metadata: _metadata }, _author_initializers, _author_extraInitializers);
            __esDecorate(null, null, _createdAt_decorators, { kind: "field", name: "createdAt", static: false, private: false, access: { has: obj => "createdAt" in obj, get: obj => obj.createdAt, set: (obj, value) => { obj.createdAt = value; } }, metadata: _metadata }, _createdAt_initializers, _createdAt_extraInitializers);
            __esDecorate(null, null, _user_id_decorators, { kind: "field", name: "user_id", static: false, private: false, access: { has: obj => "user_id" in obj, get: obj => obj.user_id, set: (obj, value) => { obj.user_id = value; } }, metadata: _metadata }, _user_id_initializers, _user_id_extraInitializers);
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            VehicleComment = _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
            __runInitializers(_classThis, _classExtraInitializers);
        }
        id = __runInitializers(this, _id_initializers, void 0);
        /** GTFS-RT vehicle id the note belongs to, e.g. `90/1044`. */
        vehicle_id = (__runInitializers(this, _id_extraInitializers), __runInitializers(this, _vehicle_id_initializers, void 0));
        /** Route at the time of writing, kept so an old note still reads sensibly. */
        route = (__runInitializers(this, _vehicle_id_extraInitializers), __runInitializers(this, _route_initializers, void 0));
        body = (__runInitializers(this, _route_extraInitializers), __runInitializers(this, _body_initializers, void 0));
        /** Display name, captured at write time so the list needs no second lookup. */
        author = (__runInitializers(this, _body_extraInitializers), __runInitializers(this, _author_initializers, void 0));
        createdAt = (__runInitializers(this, _author_extraInitializers), __runInitializers(this, _createdAt_initializers, void 0));
        /** Owner association, populated from the JWT `sub` claim. */
        user_id = (__runInitializers(this, _createdAt_extraInitializers), __runInitializers(this, _user_id_initializers, void 0));
        constructor() {
            __runInitializers(this, _user_id_extraInitializers);
        }
    };
    return VehicleComment = _classThis;
})();
export { VehicleComment };
