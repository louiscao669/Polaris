import { readJson } from './api';

export async function readPolicyOptions() {
  return readJson('/metadata/policy-options');
}

export function formatRoleOption(role) {
  if (!role) return '';
  const id = role.role_id || role.role || '';
  const description = (role.description || '').trim();
  return description ? `${id} - ${description}` : id;
}

export function formatConstraintOption(constraint) {
  if (!constraint) return '';
  const id = constraint.constraint_id;
  const name = constraint.name || `Constraint ${id}`;
  const description = (constraint.description || '').trim();
  return description ? `${name} (#${id}) - ${description}` : `${name} (#${id})`;
}

export function getMarketAccessLabel(code) {
  if (code === 'better') return 'Better';
  if (code === 'analytic') return 'Analyzer';
  if (code === 'viewer') return 'Viewer';
  return code || '';
}

export function getMarketAccessView(code) {
  if (code === 'better') return 'bettor';
  if (code === 'analytic') return 'analyzer';
  return 'viewer';
}

export function formatMarketAccessOption(option) {
  if (!option) return '';
  const code = option.as_code || '';
  const label = getMarketAccessLabel(code);
  const description = (option.description || '').trim();
  return description ? `${label} - ${description}` : label;
}
