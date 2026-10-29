import L from 'leaflet';
import { StrictMode, act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { MapView } from './MapView';

describe('key-free 2D basemap', () => {
  let container: HTMLDivElement;
  let root: Root;

  const renderMap = () => (
    <MapView
      vehicles={[]}
      selectedIds={[]}
      activeVehicleId={null}
      paths={new Map()}
      onSelect={() => undefined}
    />
  );

  beforeEach(() => {
    vi.stubGlobal('IS_REACT_ACT_ENVIRONMENT', true);
    container = document.createElement('div');
    document.body.append(container);
    root = createRoot(container);
  });

  afterEach(async () => {
    await act(() => root.unmount());
    container.remove();
    document.documentElement.classList.remove('dark');
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it.each(['dark', 'light'])('uses attributed, key-free tiles in the %s theme', async (theme) => {
    document.documentElement.classList.toggle('dark', theme === 'dark');
    const tileLayer = vi.spyOn(L, 'tileLayer');

    await act(() => root.render(renderMap()));

    expect(tileLayer).toHaveBeenCalledWith(
      'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
      expect.objectContaining({
        maxZoom: 19,
        referrerPolicy: 'strict-origin-when-cross-origin',
      }),
    );
    const attribution = container.querySelector('.leaflet-control-attribution');
    expect(attribution?.textContent).toContain('OpenStreetMap contributors');
    expect(attribution?.textContent).toContain('HSL');
    expect(attribution?.textContent).not.toContain('CARTO');
    expect(attribution?.querySelector('a[href="https://www.openstreetmap.org/copyright"]'))
      .not.toBeNull();
  });

  it('retains the tile layer when the app theme changes', async () => {
    const tileLayer = vi.spyOn(L, 'tileLayer');
    await act(() => root.render(renderMap()));
    document.documentElement.classList.add('dark');
    await act(() => root.render(renderMap()));
    expect(tileLayer).toHaveBeenCalledTimes(1);
  });

  it('recreates the basemap after StrictMode effect cleanup', async () => {
    const tileLayer = vi.spyOn(L, 'tileLayer');
    await act(() => root.render(<StrictMode>{renderMap()}</StrictMode>));
    expect(tileLayer).toHaveBeenCalledTimes(2);
    expect(container.querySelectorAll('.leaflet-control-attribution')).toHaveLength(1);
  });
});
