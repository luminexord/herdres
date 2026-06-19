import assert from 'node:assert/strict';
import test from 'node:test';

import { cleanHerdrPtyEnv } from './herdr-env.js';

test('cleanHerdrPtyEnv removes inherited pane session state before spawning herdr', () => {
  const cleaned = cleanHerdrPtyEnv({
    PATH: '/usr/bin',
    HOME: '/Users/example',
    HERDR_ENV: '1',
    HERDR_SOCKET_PATH: '/tmp/herdr.sock',
    HERDR_CLIENT_SOCKET_PATH: '/tmp/herdr-client.sock',
    HERDR_WORKSPACE_ID: 'w2',
    HERDR_TAB_ID: 'w2:t1',
    HERDR_PANE_ID: 'w2:p1',
    HERDR_PLUGIN_ID: 'plugin',
  });

  assert.equal(cleaned.PATH, '/usr/bin');
  assert.equal(cleaned.HOME, '/Users/example');
  assert.equal(cleaned.TERM, 'xterm-256color');
  assert.equal('HERDR_ENV' in cleaned, false);
  assert.equal('HERDR_SOCKET_PATH' in cleaned, false);
  assert.equal('HERDR_CLIENT_SOCKET_PATH' in cleaned, false);
  assert.equal('HERDR_WORKSPACE_ID' in cleaned, false);
  assert.equal('HERDR_TAB_ID' in cleaned, false);
  assert.equal('HERDR_PANE_ID' in cleaned, false);
  assert.equal('HERDR_PLUGIN_ID' in cleaned, false);
});
