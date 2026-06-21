import assert from 'node:assert/strict';
import test from 'node:test';

import { renderSpacePaneNavigation } from '../web/navigation.js';

class FakeElement {
  constructor(tag) {
    this.tag = tag;
    this.children = [];
    this.parentNode = null;
    this.dataset = {};
    this.attributes = {};
    this._className = '';
    this._textContent = '';
    this.onclick = null;
  }

  get className() {
    return this._className;
  }

  set className(value) {
    this._className = String(value || '');
  }

  get textContent() {
    return this._textContent;
  }

  set textContent(value) {
    this._textContent = String(value || '');
  }

  set innerHTML(value) {
    this.children = [];
    for (const match of String(value || '').matchAll(/<span class="([^"]+)"[^>]*>([^<]*)<\/span>/g)) {
      const child = new FakeElement('span');
      child.className = match[1];
      child.textContent = match[2] || '';
      this.appendChild(child);
    }
  }

  get innerHTML() {
    return '';
  }

  setAttribute(name, value) {
    this.attributes[name] = String(value);
  }

  appendChild(child) {
    child.parentNode = this;
    this.children.push(child);
    return child;
  }

  insertBefore(child, before) {
    child.parentNode = this;
    const index = this.children.indexOf(before);
    if (index === -1) this.children.push(child);
    else this.children.splice(index, 0, child);
    return child;
  }

  remove() {
    if (!this.parentNode) return;
    const index = this.parentNode.children.indexOf(this);
    if (index !== -1) this.parentNode.children.splice(index, 1);
    this.parentNode = null;
  }

  querySelector(selector) {
    return this.querySelectorAll(selector)[0] || null;
  }

  querySelectorAll(selector) {
    if (selector === ':scope > .head') return this.children.filter((child) => child.hasClass('head'));
    if (!selector.startsWith('.')) return [];
    const cls = selector.slice(1);
    const out = [];
    const visit = (node) => {
      for (const child of node.children) {
        if (child.hasClass(cls)) out.push(child);
        visit(child);
      }
    };
    visit(this);
    return out;
  }

  hasClass(cls) {
    return this.className.split(/\s+/).includes(cls);
  }

  click() {
    if (this.onclick) this.onclick({ target: this });
  }
}

function withFakeDocument(fn) {
  const previous = globalThis.document;
  globalThis.document = { createElement: (tag) => new FakeElement(tag) };
  try {
    fn();
  } finally {
    if (previous === undefined) delete globalThis.document;
    else globalThis.document = previous;
  }
}

test('renderSpacePaneNavigation refreshes reused pane button click handlers', () => withFakeDocument(() => {
  const spacesEl = document.createElement('div');
  const panesEl = document.createElement('div');
  const clicked = [];

  renderSpacePaneNavigation({
    spacesEl,
    panesEl,
    status: {
      spaces: [{ id: 'w1', label: 'One' }],
      panes: [{ id: 'w1:p1', label: 'Claude', agent: 'claude', spaceId: 'w1' }],
    },
    selected: 'w1',
    onSpace: () => {},
    onPane: (id) => clicked.push(id),
  });
  panesEl.children[0].click();

  renderSpacePaneNavigation({
    spacesEl,
    panesEl,
    status: {
      spaces: [{ id: 'w2', label: 'Two' }],
      panes: [{ id: 'w2:p7', label: 'Claude', agent: 'claude', spaceId: 'w2' }],
    },
    selected: 'w2',
    onSpace: () => {},
    onPane: (id) => clicked.push(id),
  });
  assert.equal(panesEl.children[0].dataset.id, 'w2:p7');
  panesEl.children[0].click();

  assert.deepEqual(clicked, ['w1:p1', 'w2:p7']);
}));
