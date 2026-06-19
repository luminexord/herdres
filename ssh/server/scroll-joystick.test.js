import assert from 'node:assert/strict';
import test from 'node:test';

import { scrollJoystickMessage } from '../web/scroll-joystick.js';

test('scrollJoystickMessage requests active pane wheel scrolling instead of arrow keys', () => {
  assert.deepEqual(scrollJoystickMessage('down'), { t: 'scroll', direction: 'down', units: 3 });
  assert.deepEqual(scrollJoystickMessage('up', 2), { t: 'scroll', direction: 'up', units: 2 });
  assert.equal(scrollJoystickMessage('sideways'), null);
});
