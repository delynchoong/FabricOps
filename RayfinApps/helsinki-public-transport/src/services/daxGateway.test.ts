import { beforeEach, describe, expect, it, vi } from 'vitest';

const { executeQuery } = vi.hoisted(() => ({ executeQuery: vi.fn() }));

vi.mock('@/services/rayfinClient', () => ({
  getRayfinClient: () => ({ connectors: { transitModel: { executeQuery } } }),
}));
vi.mock('@/services/fabricHostBridge', () => ({
  hasFabricHost: () => false,
  executeDaxViaHost: vi.fn(),
}));
vi.mock('@/services/powerBiDirect', () => ({
  acquirePowerBiToken: vi.fn(),
  executeQueriesDirect: vi.fn(),
}));

describe('connector DAX results', () => {
  beforeEach(() => {
    vi.resetModules();
    executeQuery.mockReset();
  });

  it('maps column-aligned values into named DAX rows', async () => {
    executeQuery.mockResolvedValue({
      status: 'success',
      table: {
        columns: [{ name: '[vehicle]' }, { name: '[speed]' }],
        rows: [['bus-1', 0], ['tram-2', null]],
      },
      requestId: 'test',
    });
    const { runDax } = await import('./daxGateway');
    await expect(runDax('EVALUATE Vehicles')).resolves.toEqual([
      { '[vehicle]': 'bus-1', '[speed]': 0 },
      { '[vehicle]': 'tram-2', '[speed]': null },
    ]);
  });

  it('preserves an empty successful result', async () => {
    executeQuery.mockResolvedValue({
      status: 'success',
      table: { columns: [], rows: [] },
      requestId: 'test',
    });
    const { runDax } = await import('./daxGateway');
    await expect(runDax('EVALUATE Vehicles')).resolves.toEqual([]);
  });

  it('surfaces query failures instead of returning empty rows', async () => {
    executeQuery.mockResolvedValue({
      status: 'error',
      error: { category: 'query', message: 'Invalid DAX expression' },
      requestId: 'test',
    });
    const { runDax } = await import('./daxGateway');
    await expect(runDax('invalid')).rejects.toThrow('Invalid DAX expression');
  });
});
