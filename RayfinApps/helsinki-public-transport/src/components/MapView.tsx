import L from 'leaflet';
import { useEffect, useMemo, useRef } from 'react';

import type { PathPoint, Vehicle } from '@/data/model';
import { DIMMED_COLOR, speedColor, trackColor } from '@/theme';

const HELSINKI: L.LatLngTuple = [60.1699, 24.9384];
const DEFAULT_ZOOM = 12;

const TILE_URL = 'https://tile.openstreetmap.org/{z}/{x}/{y}.png';

const ATTRIBUTION =
  '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors' +
  ' &middot; data &copy; ' +
  '<a href="https://www.hsl.fi/">HSL</a>';

interface MapViewProps {
  vehicles: Vehicle[];
  /** Compared vehicles, in tab order - the order also picks each track's colour. */
  selectedIds: string[];
  activeVehicleId: string | null;
  paths: Map<string, PathPoint[]>;
  onSelect: (vehicleId: string | null, additive: boolean) => void;
}

/**
 * Leaflet map with one marker per vehicle.
 *
 * Markers are kept in a ref-held map keyed by vehicle id and *moved* on each poll rather than
 * torn down and rebuilt - at ~900 vehicles every 2 s, recreating layers drops frames badly.
 */
export function MapView({
  vehicles,
  selectedIds,
  activeVehicleId,
  paths,
  onSelect,
}: MapViewProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<L.Map | null>(null);
  const markersRef = useRef(new Map<string, L.CircleMarker>());
  const pathsRef = useRef(new Map<string, L.Polyline>());
  const onSelectRef = useRef(onSelect);
  onSelectRef.current = onSelect;

  const selectedIndex = useMemo(
    () => new Map(selectedIds.map((id, index) => [id, index])),
    [selectedIds],
  );

  // Create the map once.
  useEffect(() => {
    if (mapRef.current || !containerRef.current) return;

    const map = L.map(containerRef.current, {
      center: HELSINKI,
      zoom: DEFAULT_ZOOM,
      zoomControl: true,
      preferCanvas: true,
    });
    mapRef.current = map;

    map.on('click', () => onSelectRef.current(null, false));

    L.tileLayer(TILE_URL, {
      attribution: ATTRIBUTION,
      maxZoom: 19,
      referrerPolicy: 'strict-origin-when-cross-origin',
    }).addTo(map);

    return () => {
      map.remove();
      mapRef.current = null;
      markersRef.current.clear();
    };
  }, []);

  const vehicleIndex = useMemo(() => new Map(vehicles.map((v) => [v.vehicleId, v])), [vehicles]);

  // Add / move / remove markers.
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;

    const markers = markersRef.current;

    for (const vehicle of vehicles) {
      const trackIndex = selectedIndex.get(vehicle.vehicleId);
      const selected = trackIndex !== undefined;
      const active = vehicle.vehicleId === activeVehicleId;
      // While a comparison is open everything else greys out, so the tracked vehicles are the
      // only coloured things on the map and stay findable in a fleet of ~1,300.
      const dimmed = selectedIds.length > 0 && !selected;
      const style: L.CircleMarkerOptions = {
        radius: active ? 8 : selected ? 7 : 4,
        // A selected vehicle is ringed in its track colour, so marker and trail read as one thing.
        color: selected ? trackColor(trackIndex) : dimmed ? DIMMED_COLOR : speedColor(vehicle.speedKmh),
        weight: selected ? 3 : 1,
        fillColor: dimmed ? DIMMED_COLOR : speedColor(vehicle.speedKmh),
        fillOpacity: dimmed ? 0.45 : 0.9,
      };

      let marker = markers.get(vehicle.vehicleId);
      if (marker) {
        marker.setLatLng([vehicle.lat, vehicle.lon]);
        marker.setStyle(style);
        marker.setRadius(style.radius as number);
      } else {
        marker = L.circleMarker([vehicle.lat, vehicle.lon], style).addTo(map);
        marker.on('click', (event) => {
          L.DomEvent.stopPropagation(event);
          const source = event.originalEvent;
          onSelectRef.current(
            vehicle.vehicleId,
            source.ctrlKey || source.metaKey || source.shiftKey,
          );
        });
        markers.set(vehicle.vehicleId, marker);
      }

      marker.bindTooltip(
        `<b>${vehicle.route || 'unknown route'}</b><br/>${vehicle.vehicleId}<br/>` +
          `${vehicle.speedKmh.toFixed(0)} km/h`,
        { className: 'hsl-tooltip', direction: 'top' },
      );
    }

    for (const [id, marker] of markers) {
      if (!vehicleIndex.has(id)) {
        marker.remove();
        markers.delete(id);
      }
    }
  }, [vehicles, vehicleIndex, selectedIndex, activeVehicleId]);

  // Draw one track per compared vehicle.
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;

    const lines = pathsRef.current;

    for (const [id, line] of lines) {
      if (!selectedIndex.has(id)) {
        line.remove();
        lines.delete(id);
      }
    }

    for (const [id, index] of selectedIndex) {
      const points = paths.get(id) ?? [];
      const existing = lines.get(id);

      if (points.length < 2) {
        existing?.remove();
        lines.delete(id);
        continue;
      }

      const latlngs = points.map((p) => [p.lat, p.lon] as L.LatLngTuple);
      const style: L.PolylineOptions = {
        color: trackColor(index),
        weight: id === activeVehicleId ? 4 : 2.5,
        opacity: id === activeVehicleId ? 0.95 : 0.7,
      };

      if (existing) {
        existing.setLatLngs(latlngs);
        existing.setStyle(style);
      } else {
        lines.set(id, L.polyline(latlngs, style).addTo(map));
      }
    }
  }, [paths, selectedIndex, activeVehicleId]);

  // Pan to whichever vehicle the panel is showing.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !activeVehicleId) return;
    const vehicle = vehicleIndex.get(activeVehicleId);
    if (vehicle) map.panTo([vehicle.lat, vehicle.lon], { animate: true });
    // Only when the selection itself changes - not on every position poll.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeVehicleId]);

  return <div ref={containerRef} className="h-full w-full" data-testid="map" />;
}
