import { WEBUI_API_BASE_URL } from '$lib/constants';

async function request(path: string, method = 'GET', body?: unknown) {
	const response = await fetch(`${WEBUI_API_BASE_URL}/machines/pending-chats${path}`, {
		method,
		headers: { Authorization: `Bearer ${localStorage.token}`, 'Content-Type': 'application/json' },
		...(body === undefined ? {} : { body: JSON.stringify(body) })
	});
	const result = await response.json();
	if (!response.ok) throw new Error(result.detail ?? 'Unable to prepare conversation');
	return result;
}

export const pendingPolicy = (machineId: string) =>
	request(`/settings?machine_id=${encodeURIComponent(machineId)}`);
export const savePendingPolicy = (machineId: string, enabled: boolean, expiryMinutes: number) =>
	request(`/settings?machine_id=${encodeURIComponent(machineId)}`, 'PUT', {
		enabled, expiry_minutes: expiryMinutes
	});
export const ensurePendingChat = (machineId: string, folderId: string | null, params: unknown) =>
	request('/ensure', 'POST', { machine_id: machineId, folder_id: folderId, params });
