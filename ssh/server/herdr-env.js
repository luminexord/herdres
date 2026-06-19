const HERDR_PTY_ENV_KEYS = [
  'HERDR_ENV',
  'HERDR_SOCKET_PATH',
  'HERDR_CLIENT_SOCKET_PATH',
  'HERDR_WORKSPACE_ID',
  'HERDR_TAB_ID',
  'HERDR_PANE_ID',
  'HERDR_PLUGIN_ID',
];

export function cleanHerdrPtyEnv(sourceEnv = process.env) {
  const env = { ...sourceEnv, TERM: 'xterm-256color' };
  for (const key of HERDR_PTY_ENV_KEYS) delete env[key];
  return env;
}
